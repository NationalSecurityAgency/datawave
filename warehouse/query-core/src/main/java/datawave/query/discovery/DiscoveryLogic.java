package datawave.query.discovery;

import static com.google.common.collect.Iterators.concat;
import static com.google.common.collect.Iterators.transform;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.LongRange;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.javatuples.Pair;

import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.configuration.QueryData;
import datawave.data.type.Type;
import datawave.query.Constants;
import datawave.query.QueryParameters;
import datawave.query.discovery.FindLiteralsAndPatternsVisitor.QueryValues;
import datawave.query.exceptions.IllegalRangeArgumentException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.lookups.ShardIndexQueryTableStaticMethods;
import datawave.query.jexl.visitors.CaseSensitivityVisitor;
import datawave.query.jexl.visitors.QueryModelVisitor;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.language.tree.QueryNode;
import datawave.query.parser.JavaRegexAnalyzer.JavaRegexParseException;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.ShardIndexQueryTable;
import datawave.query.util.MetadataHelper;
import datawave.webservice.query.Query;
import datawave.webservice.query.exception.QueryException;

public class DiscoveryLogic extends ShardIndexQueryTable {

    private static final Logger log = Logger.getLogger(DiscoveryLogic.class);

    public static final String SEPARATE_COUNTS_BY_COLVIS = "separate.counts.by.colvis";
    public static final String SHOW_REFERENCE_COUNT = "show.reference.count";
    public static final String REVERSE_INDEX = "reverse.index";
    private DiscoveryQueryConfiguration config;
    private MetadataHelper metadataHelper;

    public DiscoveryLogic() {
        super();
    }

    public DiscoveryLogic(ShardIndexQueryTable other) {
        super(other);
    }

    @Override
    public DiscoveryQueryConfiguration getConfig() {
        if (this.config == null) {
            this.config = DiscoveryQueryConfiguration.create();
        }

        return this.config;
    }

    @Override
    public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set<Authorizations> auths) throws Exception {
        this.config = new DiscoveryQueryConfiguration(this, settings);
        this.scannerFactory = new ScannerFactory(client);

        this.metadataHelper = initializeMetadataHelper(client, config.getMetadataTableName(), auths);

        if (StringUtils.isEmpty(settings.getQuery())) {
            throw new IllegalArgumentException("Query cannot be null");
        }

        if (log.isDebugEnabled()) {
            log.debug("Query parameters set to " + settings.getParameters());
        }

        // Check if the default modelName and modelTableNames have been overriden by custom parameters.
        if (null != settings.findParameter(QueryParameters.PARAMETER_MODEL_NAME)
                        && !settings.findParameter(QueryParameters.PARAMETER_MODEL_NAME).getParameterValue().trim().isEmpty()) {
            setModelName(settings.findParameter(QueryParameters.PARAMETER_MODEL_NAME).getParameterValue().trim());
        }
        if (null != settings.findParameter(QueryParameters.PARAMETER_MODEL_TABLE_NAME)
                        && !settings.findParameter(QueryParameters.PARAMETER_MODEL_TABLE_NAME).getParameterValue().trim().isEmpty()) {
            setModelTableName(settings.findParameter(QueryParameters.PARAMETER_MODEL_TABLE_NAME).getParameterValue().trim());
        }

        // Check if user would like counts separated by column visibility
        if (null != settings.findParameter(SEPARATE_COUNTS_BY_COLVIS)
                        && !settings.findParameter(SEPARATE_COUNTS_BY_COLVIS).getParameterValue().trim().isEmpty()) {
            boolean separateCountsByColVis = Boolean.valueOf(settings.findParameter(SEPARATE_COUNTS_BY_COLVIS).getParameterValue().trim());
            getConfig().setSeparateCountsByColVis(separateCountsByColVis);
        }

        // Check if user would like to show reference counts instead of term counts
        if (null != settings.findParameter(SHOW_REFERENCE_COUNT) && !settings.findParameter(SHOW_REFERENCE_COUNT).getParameterValue().trim().isEmpty()) {
            boolean showReferenceCount = Boolean.valueOf(settings.findParameter(SHOW_REFERENCE_COUNT).getParameterValue().trim());
            getConfig().setShowReferenceCount(showReferenceCount);
        }
        setQueryModel(metadataHelper.getQueryModel(getModelTableName(), getModelName(), null));
        // get the data type filter set if any
        if (null != settings.findParameter(QueryParameters.DATATYPE_FILTER_SET)
                        && !settings.findParameter(QueryParameters.DATATYPE_FILTER_SET).getParameterValue().trim().isEmpty()) {
            Set<String> dataTypeFilter = new HashSet<>(Arrays.asList(StringUtils
                            .split(settings.findParameter(QueryParameters.DATATYPE_FILTER_SET).getParameterValue().trim(), Constants.PARAM_VALUE_SEP)));
            getConfig().setDatatypeFilter(dataTypeFilter);
            if (log.isDebugEnabled()) {
                log.debug("Data type filter set to " + dataTypeFilter);
            }
        }

        // Set the connector
        getConfig().setClient(client);
        // Set the auths
        getConfig().setAuthorizations(auths);

        // Get the ranges
        getConfig().setBeginDate(settings.getBeginDate());
        getConfig().setEndDate(settings.getEndDate());

        if (null == getConfig().getBeginDate() || null == getConfig().getEndDate()) {
            getConfig().setBeginDate(new Date(0));
            getConfig().setEndDate(new Date(Long.MAX_VALUE));
            log.warn("Dates not specified, using entire date range");
        }

        // start with a trimmed version of the query, converted to JEXL
        LuceneToJexlQueryParser parser = new LuceneToJexlQueryParser();
        parser.setAllowLeadingWildCard(isAllowLeadingWildcard());
        QueryNode node = parser.parse(settings.getQuery().trim());
        // TODO: Validate that this is a simple list of terms type of query
        getConfig().setQueryString(node.getOriginalQuery());
        if (log.isDebugEnabled()) {
            log.debug("Original Query = " + settings.getQuery().trim());
            log.debug("JEXL Query = " + node.getOriginalQuery());
        }

        // Parse & flatten the query
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(getConfig().getQueryString());

        script = CaseSensitivityVisitor.upperCaseIdentifiers(getConfig(), metadataHelper, script);

        Set<String> dataTypes = getConfig().getDatatypeFilter();
        Set<String> allFields;
        allFields = metadataHelper.getAllFields(dataTypes);
        script = QueryModelVisitor.applyModel(script, getQueryModel(), allFields);

        QueryValues literalsAndPatterns = FindLiteralsAndPatternsVisitor.find(script);
        Stopwatch timer = Stopwatch.createStarted();
        // no caching for getAllNormalizers, so try some magic with getFields...
        Multimap<String,Type<?>> dataTypeMap = ArrayListMultimap.create(metadataHelper.getFieldsToDatatypes(getConfig().getDatatypeFilter()));
        /*
         * we have a mapping of FIELD->DataType, but not a mapping of ANYFIELD->DataType which should be all dataTypes
         */
        dataTypeMap.putAll(Constants.ANY_FIELD, uniqueByType(dataTypeMap.values()));
        timer.stop();
        log.debug("Took " + timer.elapsed(TimeUnit.MILLISECONDS) + "ms to get all the dataTypes.");
        getConfig().setLiterals(normalize(new LiteralNormalization(), literalsAndPatterns.getLiterals(), dataTypeMap));
        getConfig().setPatterns(normalize(new PatternNormalization(), literalsAndPatterns.getPatterns(), dataTypeMap));
        getConfig().setRanges(normalizeRanges(new LiteralNormalization(), literalsAndPatterns.getRanges(), dataTypeMap));
        if (log.isDebugEnabled()) {
            log.debug("Normalized Literals = " + getConfig().getLiterals());
            log.debug("Normalized Patterns = " + getConfig().getPatterns());
        }

        getConfig().setQueries(createQueries(getConfig()));

        return getConfig();
    }

    public List<QueryData> createQueries(DiscoveryQueryConfiguration config) throws QueryException, TableNotFoundException, IOException, ExecutionException {
        final List<QueryData> queries = Lists.newLinkedList();

        Set<String> familiesToSeek = Sets.newHashSet();
        Pair<Set<Range>,Set<Range>> seekRanges = makeRanges(getConfig(), familiesToSeek, metadataHelper);
        Collection<Range> forward = seekRanges.getValue0();

        if (!forward.isEmpty()) {
            List<IteratorSetting> settings = getIteratorSettingsForDiscovery(getConfig(), getConfig().getLiterals(), getConfig().getPatterns(),
                            getConfig().getRanges(), false);
            if (isCheckpointable()) {
                // if checkpointable, then only one range per query data so that the whole checkpointing thing works correctly
                for (Range range : forward) {
                    queries.add(new QueryData(config.getIndexTableName(), null, Collections.singleton(range), settings, familiesToSeek));
                }
            } else {
                queries.add(new QueryData(config.getIndexTableName(), null, forward, settings, familiesToSeek));
            }
        }

        Collection<Range> reverse = seekRanges.getValue1();
        if (!reverse.isEmpty()) {
            List<IteratorSetting> settings = getIteratorSettingsForDiscovery(getConfig(), getConfig().getLiterals(), getConfig().getPatterns(),
                            getConfig().getRanges(), true);
            if (isCheckpointable()) {
                // if checkpointable, then only one range per query data so that the whole checkpointing thing works correctly
                for (Range range : reverse) {
                    queries.add(new QueryData(config.getReverseIndexTableName(), null, Collections.singleton(range), settings, familiesToSeek));
                }
            } else {
                queries.add(new QueryData(config.getReverseIndexTableName(), null, reverse, settings, familiesToSeek));
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("Created ranges: " + queries);
        }

        return queries;
    }

    @Override
    public void setupQuery(GenericQueryConfiguration genericConfig) throws QueryException, TableNotFoundException, IOException, ExecutionException {
        if (!genericConfig.getClass().getName().equals(DiscoveryQueryConfiguration.class.getName())) {
            throw new QueryException("Did not receive a DiscoveryQueryConfiguration instance!!");
        }
        this.config = (DiscoveryQueryConfiguration) genericConfig;
        final List<Iterator<DiscoveredThing>> iterators = Lists.newArrayList();

        for (QueryData qd : config.getQueries()) {
            if (log.isDebugEnabled()) {
                log.debug("Creating scanner for " + qd);
            }
            // scan the table
            BatchScanner bs = scannerFactory.newScanner(qd.getTableName(), config.getAuthorizations(), config.getNumQueryThreads(), config.getQuery());

            bs.setRanges(qd.getRanges());
            for (IteratorSetting setting : qd.getSettings()) {
                bs.addScanIterator(setting);
            }
            for (String cf : qd.getColumnFamilies()) {
                bs.fetchColumnFamily(new Text(cf));
            }

            iterators.add(transformScanner(bs, qd));
        }
        this.iterator = concat(iterators.iterator());
    }

    public static List<IteratorSetting> getIteratorSettingsForDiscovery(DiscoveryQueryConfiguration config, Multimap<String,String> literals,
                    Multimap<String,String> patterns, Multimap<String,LiteralRange<String>> ranges, boolean reverseIndex) {

        List<IteratorSetting> settings = Lists.newLinkedList();
        // The begin date from the query may be down to the second, for doing lookups in the index we want to use the day because
        // the times in the index table have been truncated to the day.
        Date begin = DateUtils.truncate(config.getBeginDate(), Calendar.DAY_OF_MONTH);
        // we don't need to bump up the end date any more because it's not apart of the range set on the scanner
        Date end = config.getEndDate();

        LongRange dateRange = new LongRange(begin.getTime(), end.getTime());

        settings.add(ShardIndexQueryTableStaticMethods.configureGlobalIndexDateRangeFilter(config, dateRange));
        settings.add(ShardIndexQueryTableStaticMethods.configureGlobalIndexDataTypeFilter(config, config.getDatatypeFilter()));

        IteratorSetting matchingIterator = configureIndexMatchingIterator(config, literals, patterns, ranges, reverseIndex);
        if (matchingIterator != null) {
            settings.add(matchingIterator);
        }

        IteratorSetting discoveryIteratorSetting = new IteratorSetting(config.getBaseIteratorPriority() + 50, DiscoveryIterator.class);
        discoveryIteratorSetting.addOption(REVERSE_INDEX, Boolean.toString(reverseIndex));
        discoveryIteratorSetting.addOption(SEPARATE_COUNTS_BY_COLVIS, config.getSeparateCountsByColVis().toString());
        if (config.getShowReferenceCount()) {
            discoveryIteratorSetting.addOption(SHOW_REFERENCE_COUNT, config.getShowReferenceCount().toString());
        }
        settings.add(discoveryIteratorSetting);

        return settings;
    }

    public static final IteratorSetting configureIndexMatchingIterator(DiscoveryQueryConfiguration config, Multimap<String,String> literals,
                    Multimap<String,String> patterns, Multimap<String,LiteralRange<String>> ranges, boolean reverseIndex) {
        if ((literals == null || literals.isEmpty()) && (patterns == null || patterns.isEmpty()) && (ranges == null || ranges.isEmpty())) {
            return null;
        }
        log.debug("Configuring IndexMatchingIterator with " + literals + " and " + patterns);

        IteratorSetting cfg = new IteratorSetting(config.getBaseIteratorPriority() + 23, "termMatcher", IndexMatchingIterator.class);

        IndexMatchingIterator.Configuration conf = new IndexMatchingIterator.Configuration();
        if (literals != null) {
            for (Entry<String,String> literal : literals.entries()) {
                if (Constants.ANY_FIELD.equals(literal.getValue())) {
                    conf.addLiteral(literal.getKey());
                } else {
                    conf.addLiteral(literal.getKey(), literal.getValue());
                }
            }
        }
        if (patterns != null) {
            for (Entry<String,String> pattern : patterns.entries()) {
                if (Constants.ANY_FIELD.equals(pattern.getValue())) {
                    conf.addPattern(pattern.getKey());
                } else {
                    conf.addPattern(pattern.getKey(), pattern.getValue());
                }
            }
        }
        if (ranges != null) {
            for (Entry<String,LiteralRange<String>> range : ranges.entries()) {
                if (Constants.ANY_FIELD.equals(range.getKey())) {
                    conf.addRange(range.getValue());
                } else {
                    conf.addRange(range.getValue(), range.getKey());
                }
            }
        }

        cfg.addOption(IndexMatchingIterator.CONF, IndexMatchingIterator.gson().toJson(conf));

        cfg.addOption(IndexMatchingIterator.REVERSE_INDEX, Boolean.toString(reverseIndex));

        return cfg;
    }

    @Override
    public ShardIndexQueryTable clone() {
        return new DiscoveryLogic(this);
    }

    /**
     * Takes in a batch scanner and returns an iterator over the DiscoveredThing objects contained in the value.
     *
     * @param scanner
     *            a batch scanner
     * @return iterator for discoveredthings
     */
    public static Iterator<DiscoveredThing> transformScanner(final BatchScanner scanner, final QueryData queryData) {
        return concat(transform(scanner.iterator(), new Function<Entry<Key,Value>,Iterator<DiscoveredThing>>() {
            DataInputBuffer in = new DataInputBuffer();

            @Override
            public Iterator<DiscoveredThing> apply(Entry<Key,Value> from) {
                queryData.setLastResult(from.getKey());
                Value value = from.getValue();
                in.reset(value.get(), value.getSize());
                ArrayWritable aw = new ArrayWritable(DiscoveredThing.class);
                try {
                    aw.readFields(in);
                } catch (IOException e) {
                    log.error(e);
                    return null;
                }
                ArrayList<DiscoveredThing> thangs = Lists.newArrayListWithCapacity(aw.get().length);
                for (Writable w : aw.get()) {
                    thangs.add((DiscoveredThing) w);
                }
                return thangs.iterator();
            }
        }));
    }

    /**
     * Makes two collections of ranges: one for the forward index (value0) and one for the reverse index (value1).
     *
     * If a literal has a field name, then the Range for that term will include the column family. If there are multiple fields, then multiple ranges are
     * created.
     *
     * @param config
     *            the discovery config
     * @param familiesToSeek
     *            the families to seek
     * @param metadataHelper
     *            a metadata helper
     * @return a pair of ranges
     * @throws TableNotFoundException
     *             if the table is not found
     * @throws ExecutionException
     *             for execution exceptions
     */
    @SuppressWarnings("unchecked")
    public static Pair<Set<Range>,Set<Range>> makeRanges(DiscoveryQueryConfiguration config, Set<String> familiesToSeek, MetadataHelper metadataHelper)
                    throws TableNotFoundException, ExecutionException {
        Set<Range> forwardRanges = new HashSet<>();
        for (Entry<String,String> literalAndField : config.getLiterals().entries()) {
            String literal = literalAndField.getKey(), field = literalAndField.getValue();
            // if we're _ANYFIELD_, then use null when making the literal range
            field = Constants.ANY_FIELD.equals(field) ? null : field;
            if (field != null) {
                familiesToSeek.add(field);
            }
            forwardRanges.add(ShardIndexQueryTableStaticMethods.getLiteralRange(field, literal));
        }
        for (Entry<String,LiteralRange<String>> rangeEntry : config.getRanges().entries()) {
            LiteralRange<String> range = rangeEntry.getValue();
            String field = rangeEntry.getKey();
            // if we're _ANYFIELD_, then use null when making the literal range
            field = Constants.ANY_FIELD.equals(field) ? null : field;
            if (field != null) {
                familiesToSeek.add(field);
            }
            try {
                forwardRanges.add(ShardIndexQueryTableStaticMethods.getBoundedRangeRange(range));
            } catch (IllegalRangeArgumentException e) {
                log.error("Error using range [" + range + "]", e);
                continue;
            }
        }
        Set<Range> reverseRanges = new HashSet<>();
        for (Entry<String,String> patternAndField : config.getPatterns().entries()) {
            String pattern = patternAndField.getKey(), field = patternAndField.getValue();
            // if we're _ANYFIELD_, then use null when making the literal range
            field = Constants.ANY_FIELD.equals(field) ? null : field;
            ShardIndexQueryTableStaticMethods.RefactoredRangeDescription description;
            try {
                if (field != null) {
                    familiesToSeek.add(field);
                }
                description = ShardIndexQueryTableStaticMethods.getRegexRange(field, pattern, false, metadataHelper, config);
            } catch (JavaRegexParseException e) {
                log.error("Error parsing pattern [" + pattern + "]", e);
                continue;
            }
            if (description.isForReverseIndex) {
                reverseRanges.add(description.range);
            } else {
                forwardRanges.add(description.range);
            }
        }
        return Pair.with(forwardRanges, reverseRanges);
    }

    /**
     * This attempts to normalize all of the {@code <value, field>} tuples with the corresponding {@code <field, normalizer>} tuple. The Normalization object
     * will determine whether or not a regex or literal is being normalized.
     *
     * See the {@link PatternNormalization} and {@link LiteralNormalization} implementations.
     *
     * @param normalization
     *            the normalizer object
     * @param valuesToFields
     *            mapping of values to fields
     * @param dataTypeMap
     *            the data type map
     * @return a mapping of the noramlized tuples
     */
    public static Multimap<String,String> normalize(Normalization normalization, Multimap<String,String> valuesToFields, Multimap<String,Type<?>> dataTypeMap) {
        Multimap<String,String> normalizedValuesToFields = HashMultimap.create();
        for (Entry<String,String> valueAndField : valuesToFields.entries()) {
            String value = valueAndField.getKey(), field = valueAndField.getValue();
            for (Type<?> dataType : dataTypeMap.get(field)) {
                try {
                    log.debug("Attempting to normalize [" + value + "] with [" + dataType.getClass() + "]");
                    String normalized = normalization.normalize(dataType, field, value);
                    normalizedValuesToFields.put(normalized, field);
                    log.debug("Normalization succeeded!");
                } catch (Exception exception) {
                    log.debug("Normalization failed.");
                }
            }
        }
        return normalizedValuesToFields;
    }

    /**
     * This attempts to normalize all of the {@code <value, field>} tuples with the corresponding {@code <field, normalizer>} tuple. The Normalization object
     * will determine whether or not a regex or literal is being normalized.
     *
     * See the {@link PatternNormalization} and {@link LiteralNormalization} implementations.
     *
     * @param normalization
     *            the normalizer object
     * @param valuesToFields
     *            mapping of values to fields
     * @param dataTypeMap
     *            the data type map
     * @return a mapping of the normalized ranges
     */
    public static Multimap<String,LiteralRange<String>> normalizeRanges(Normalization normalization, Multimap<String,LiteralRange<?>> valuesToFields,
                    Multimap<String,Type<?>> dataTypeMap) {
        Multimap<String,LiteralRange<String>> normalizedValuesToFields = HashMultimap.create();
        for (Entry<String,LiteralRange<?>> valueAndField : valuesToFields.entries()) {
            String field = valueAndField.getKey();
            LiteralRange<?> value = valueAndField.getValue();
            for (Type<?> dataType : dataTypeMap.get(field)) {
                try {
                    log.debug("Attempting to normalize [" + value + "] with [" + dataType.getClass() + "]");
                    String normalizedLower = normalization.normalize(dataType, field, value.getLower().toString());
                    String normalizedUpper = normalization.normalize(dataType, field, value.getUpper().toString());
                    normalizedValuesToFields.put(field, new LiteralRange<>(normalizedLower, value.isLowerInclusive(), normalizedUpper, value.isUpperInclusive(),
                                    value.getFieldName(), value.getNodeOperand()));
                    log.debug("Normalization succeeded!");
                } catch (Exception exception) {
                    log.debug("Normalization failed.");
                }
            }
        }
        return normalizedValuesToFields;
    }

    /**
     * Given a sequence of objects of type T, this method will return a single object for every unique type passed in. This is used to dedupe normalizer
     * instances by their type, so that we only get 1 instance per type of normalizer.
     *
     * @param things
     *            iterable list of objects
     * @param <T>
     *            type of the objects
     * @return an object for each type passed in
     */
    public static <T> Collection<T> uniqueByType(Iterable<T> things) {
        Map<Class<?>,T> map = Maps.newHashMap();
        for (T t : things) {
            map.put(t.getClass(), t);
        }
        return map.values();
    }

    @Override
    public Set<String> getOptionalQueryParameters() {
        Set<String> params = super.getOptionalQueryParameters();
        params.add(SEPARATE_COUNTS_BY_COLVIS);
        return params;
    }

    public Boolean getSeparateCountsByColVis() {
        return getConfig().getSeparateCountsByColVis();
    }

    public void setSeparateCountsByColVis(Boolean separateCountsByColVis) {
        getConfig().setSeparateCountsByColVis(separateCountsByColVis);
    }

    public Boolean getShowReferenceCount() {
        return getConfig().getShowReferenceCount();
    }

    public void setShowReferenceCount(Boolean showReferenceCount) {
        getConfig().setShowReferenceCount(showReferenceCount);
    }

}
