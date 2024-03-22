package datawave.query.discovery;

import static com.google.common.collect.Iterators.concat;
import static com.google.common.collect.Iterators.transform;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
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
import org.apache.accumulo.core.client.ScannerBase;
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
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.exception.QueryException;

public class DiscoveryLogic extends ShardIndexQueryTable {

    private static final Logger log = Logger.getLogger(DiscoveryLogic.class);

    /**
     * Used to specify if counts should be separated by column visibility.
     */
    public static final String SEPARATE_COUNTS_BY_COLVIS = "separate.counts.by.colvis";
    /**
     * Used to specify if reference counts should be shown instead of term counts.
     */
    public static final String SHOW_REFERENCE_COUNT = "show.reference.count";
    /**
     * Used to specify whether to sum up the counts instead of returning counts per date.
     */
    public static final String SUM_COUNTS = "sum.counts";

    public static final String REVERSE_INDEX = "reverse.index";

    private boolean separateCountsByColVis = false;
    private boolean showReferenceCount = false;
    private boolean sumCounts = false;
    private MetadataHelper metadataHelper;

    public DiscoveryLogic() {
        super();
    }

    public DiscoveryLogic(DiscoveryLogic other) {
        super(other);
        this.separateCountsByColVis = other.separateCountsByColVis;
        this.showReferenceCount = other.showReferenceCount;
        this.sumCounts = other.sumCounts;
        this.metadataHelper = other.metadataHelper;
    }

    @Override
    public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set<Authorizations> auths) throws Exception {
        if (StringUtils.isEmpty(settings.getQuery())) {
            throw new IllegalArgumentException("Query must not be null or blank");
        }

        log.debug("Query parameters: " + settings.getParameters());

        DiscoveryQueryConfiguration config = new DiscoveryQueryConfiguration(this, settings);
        this.scannerFactory = new ScannerFactory(client);
        this.metadataHelper = initializeMetadataHelper(client, config.getMetadataTableName(), auths);

        // Check if the default model name and model table name have been overridden.
        this.modelName = getOrDefault(settings, QueryParameters.PARAMETER_MODEL_NAME, this.modelName);
        this.modelTableName = getOrDefault(settings, QueryParameters.PARAMETER_MODEL_TABLE_NAME, this.modelTableName);
        // Update the query model.
        this.queryModel = metadataHelper.getQueryModel(modelTableName, modelName, null);

        // Check if counts should be separated by column visibility.
        this.separateCountsByColVis = getOrDefaultBoolean(settings, SEPARATE_COUNTS_BY_COLVIS, this.separateCountsByColVis);
        config.setSeparateCountsByColVis(this.separateCountsByColVis);

        // Check if reference counts should be shown.
        this.showReferenceCount = getOrDefaultBoolean(settings, SHOW_REFERENCE_COUNT, this.showReferenceCount);
        config.setShowReferenceCount(this.showReferenceCount);

        // Check if counts should be summed.
        this.sumCounts = getOrDefaultBoolean(settings, SUM_COUNTS, this.sumCounts);
        config.setSumCounts(this.sumCounts);

        // Check if any datatype filters were specified.
        config.setDatatypeFilter(getOrDefaultSet(settings, QueryParameters.DATATYPE_FILTER_SET, config.getDatatypeFilter()));
        log.debug("Datatype filters set to " + config.getDatatypeFilterAsString());

        // Set the connector.
        config.setClient(client);

        // Set the auths.
        config.setAuthorizations(auths);

        // Set the table names.
        if (getIndexTableName() != null) {
            config.setIndexTableName(getIndexTableName());
        }
        if (getReverseIndexTableName() != null) {
            config.setReverseIndexTableName(getReverseIndexTableName());
        }

        // Get the begin date.
        config.setBeginDate(settings.getBeginDate());
        if (config.getBeginDate() == null) {
            config.setBeginDate(new Date(0L));
            log.warn("Begin date not specified, using earliest begin date.");
        }

        // Get the end date.
        config.setEndDate(settings.getEndDate());
        if (config.getEndDate() == null) {
            config.setEndDate(new Date(Long.MAX_VALUE));
            log.warn("End date not specified, using latest end date.");
        }

        // Start with a trimmed version of the query, converted to JEXL.
        LuceneToJexlQueryParser parser = new LuceneToJexlQueryParser();
        parser.setAllowLeadingWildCard(this.isAllowLeadingWildcard());
        QueryNode node = parser.parse(settings.getQuery().trim());
        // TODO: Validate that this is a simple list of terms type of query
        config.setQueryString(node.getOriginalQuery());
        log.debug("Original Query = " + settings.getQuery().trim());
        log.debug("JEXL Query = " + node.getOriginalQuery());

        // Parse & flatten the query.
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(config.getQueryString());
        CaseSensitivityVisitor.upperCaseIdentifiers(config, metadataHelper, script);

        // Apply the query model.
        Set<String> dataTypes = config.getDatatypeFilter();
        Set<String> allFields;
        allFields = metadataHelper.getAllFields(dataTypes);
        script = QueryModelVisitor.applyModel(script, getQueryModel(), allFields);

        QueryValues literalsAndPatterns = FindLiteralsAndPatternsVisitor.find(script);
        Stopwatch timer = Stopwatch.createStarted();
        // No caching for getAllNormalizers, so try some magic with getFields.
        Multimap<String,Type<?>> dataTypeMap = ArrayListMultimap.create(metadataHelper.getFieldsToDatatypes(config.getDatatypeFilter()));
        // We have a mapping of FIELD->DataType, but not a mapping of ANYFIELD->DataType which should be all dataTypes.
        dataTypeMap.putAll(Constants.ANY_FIELD, getUniqueTypes(dataTypeMap.values()));
        timer.stop();
        log.debug("Took " + timer.elapsed(TimeUnit.MILLISECONDS) + "ms to get all the dataTypes.");

        config.setLiterals(normalize(new LiteralNormalization(), literalsAndPatterns.getLiterals(), dataTypeMap));
        config.setPatterns(normalize(new PatternNormalization(), literalsAndPatterns.getPatterns(), dataTypeMap));
        config.setRanges(normalizeRanges(new LiteralNormalization(), literalsAndPatterns.getRanges(), dataTypeMap));
        log.debug("Normalized Literals = " + config.getLiterals());
        log.debug("Normalized Patterns = " + config.getPatterns());

        return config;
    }

    /**
     * If present, return the value of the given parameter from the given settings, or return the default value otherwise.
     */
    private String getOrDefault(Query settings, String parameterName, String defaultValue) {
        String value = getTrimmedParameter(settings, parameterName);
        return StringUtils.isBlank(value) ? defaultValue : value;
    }

    /**
     * If present, return the value of the given parameter from the given settings as a boolean, or return the default value otherwise.
     */
    private boolean getOrDefaultBoolean(Query settings, String parameterName, boolean defaultValue) {
        String value = getTrimmedParameter(settings, parameterName);
        log.debug("Trimmed value for " + parameterName + ": " + value);
        return StringUtils.isBlank(value) ? defaultValue : Boolean.parseBoolean(value);
    }

    /**
     * If present, return the value of the given parameter from the given settings as a set, or return the default value otherwise.
     */
    private Set<String> getOrDefaultSet(Query settings, String parameterName, Set<String> defaultValue) {
        String value = getTrimmedParameter(settings, parameterName);
        return StringUtils.isBlank(value) ? defaultValue : new HashSet<>(Arrays.asList(StringUtils.split(value, Constants.PARAM_VALUE_SEP)));
    }

    /**
     * Return the trimmed value of the given parameter from the given settings, or null if a value is not present.
     */
    private String getTrimmedParameter(Query settings, String parameterName) {
        QueryImpl.Parameter parameter = settings.findParameter(parameterName);
        return parameter != null ? parameter.getParameterValue().trim() : null;
    }

    /**
     * Given a sequence of objects of type T, this method will return a single object for every unique type passed in. This is used to dedupe normalizer
     * instances by their type, so that we only get 1 instance per type of normalizer.
     */
    private Collection<Type<?>> getUniqueTypes(Iterable<Type<?>> things) {
        Map<Class<?>,Type<?>> map = Maps.newHashMap();
        for (Type<?> t : things) {
            map.put(t.getClass(), t);
        }
        return map.values();
    }

    /**
     * This attempts to normalize all of the {@code <value, field>} tuples with the corresponding {@code <field, normalizer>} tuple. The Normalization object
     * will determine whether a regex or literal is being normalized.
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
    private Multimap<String,String> normalize(Normalization normalization, Multimap<String,String> valuesToFields, Multimap<String,Type<?>> dataTypeMap) {
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
     * will determine whether a regex or literal is being normalized.
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
    private Multimap<String,LiteralRange<String>> normalizeRanges(Normalization normalization, Multimap<String,LiteralRange<?>> valuesToFields,
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

    @Override
    public void setupQuery(GenericQueryConfiguration genericConfig) throws QueryException, TableNotFoundException, IOException, ExecutionException {
        DiscoveryQueryConfiguration config = (DiscoveryQueryConfiguration) genericConfig;
        List<Iterator<DiscoveredThing>> iterators = Lists.newArrayList();
        Set<Text> familiesToSeek = Sets.newHashSet();
        Pair<Set<Range>,Set<Range>> seekRanges = makeRanges(config, familiesToSeek, metadataHelper);
        Collection<Range> forward = seekRanges.getValue0();
        if (!forward.isEmpty()) {
            BatchScanner bs = configureBatchScannerForDiscovery(config, scannerFactory, config.getIndexTableName(), forward, familiesToSeek, false);
            if (bs != null) {
                iterators.add(transformScanner(bs));
            }
        }
        Collection<Range> reverse = seekRanges.getValue1();
        if (!reverse.isEmpty()) {
            BatchScanner bs = configureBatchScannerForDiscovery(config, scannerFactory, config.getReverseIndexTableName(), reverse, familiesToSeek, true);
            if (bs != null) {
                iterators.add(transformScanner(bs));
            }
        }

        config.setSeparateCountsByColVis(separateCountsByColVis);
        config.setShowReferenceCount(showReferenceCount);
        config.setSumCounts(sumCounts);
        this.iterator = concat(iterators.iterator());
    }

    private BatchScanner configureBatchScannerForDiscovery(DiscoveryQueryConfiguration config, ScannerFactory scannerFactory, String tableName,
                    Collection<Range> seekRanges, Set<Text> columnFamilies, boolean reverseIndex) throws TableNotFoundException {
        log.debug("BS: Config: " + config);
        // if we have no ranges, then nothing to scan
        if (seekRanges.isEmpty()) {
            return null;
        }

        BatchScanner bs = scannerFactory.newScanner(tableName, config.getAuthorizations(), config.getNumQueryThreads(), config.getQuery());
        bs.setRanges(seekRanges);
        if (!columnFamilies.isEmpty()) {
            for (Text family : columnFamilies) {
                bs.fetchColumnFamily(family);
            }
        }

        // The begin date from the query may be down to the second, for doing lookups in the index we want to use the day because
        // the times in the index table have been truncated to the day.
        Date begin = DateUtils.truncate(config.getBeginDate(), Calendar.DAY_OF_MONTH);
        // we don't need to bump up the end date any more because it's not a part of the range set on the scanner
        Date end = config.getEndDate();

        LongRange dateRange = new LongRange(begin.getTime(), end.getTime());

        ShardIndexQueryTableStaticMethods.configureGlobalIndexDateRangeFilter(config, bs, dateRange);
        ShardIndexQueryTableStaticMethods.configureGlobalIndexDataTypeFilter(config, bs, config.getDatatypeFilter());

        configureIndexMatchingIterator(config, bs, config.getLiterals(), config.getPatterns(), config.getRanges(), reverseIndex);

        IteratorSetting discoveryIteratorSetting = new IteratorSetting(config.getBaseIteratorPriority() + 50, DiscoveryIterator.class);
        discoveryIteratorSetting.addOption(REVERSE_INDEX, Boolean.toString(reverseIndex));
        discoveryIteratorSetting.addOption(SEPARATE_COUNTS_BY_COLVIS, Boolean.toString(config.getSeparateCountsByColVis()));
        discoveryIteratorSetting.addOption(SHOW_REFERENCE_COUNT, Boolean.toString(config.getShowReferenceCount()));
        discoveryIteratorSetting.addOption(SUM_COUNTS, Boolean.toString(config.getSumCounts()));
        bs.addScanIterator(discoveryIteratorSetting);

        return bs;
    }

    private void configureIndexMatchingIterator(DiscoveryQueryConfiguration config, ScannerBase bs, Multimap<String,String> literals,
                    Multimap<String,String> patterns, Multimap<String,LiteralRange<String>> ranges, boolean reverseIndex) {
        if ((literals == null || literals.isEmpty()) && (patterns == null || patterns.isEmpty()) && (ranges == null || ranges.isEmpty())) {
            return;
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

        bs.addScanIterator(cfg);
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
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
    private Iterator<DiscoveredThing> transformScanner(final BatchScanner scanner) {
        return concat(transform(scanner.iterator(), new Function<Entry<Key,Value>,Iterator<DiscoveredThing>>() {
            final DataInputBuffer in = new DataInputBuffer();

            @Override
            public Iterator<DiscoveredThing> apply(Entry<Key,Value> from) {
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
    private Pair<Set<Range>,Set<Range>> makeRanges(DiscoveryQueryConfiguration config, Set<Text> familiesToSeek, MetadataHelper metadataHelper)
                    throws TableNotFoundException, ExecutionException {
        Set<Range> forwardRanges = new HashSet<>();
        for (Entry<String,String> literalAndField : config.getLiterals().entries()) {
            String literal = literalAndField.getKey(), field = literalAndField.getValue();
            // if we're _ANYFIELD_, then use null when making the literal range
            field = Constants.ANY_FIELD.equals(field) ? null : field;
            if (field != null) {
                familiesToSeek.add(new Text(field));
            }
            forwardRanges.add(ShardIndexQueryTableStaticMethods.getLiteralRange(field, literal));
        }
        for (Entry<String,LiteralRange<String>> rangeEntry : config.getRanges().entries()) {
            LiteralRange<String> range = rangeEntry.getValue();
            String field = rangeEntry.getKey();
            // if we're _ANYFIELD_, then use null when making the literal range
            field = Constants.ANY_FIELD.equals(field) ? null : field;
            if (field != null) {
                familiesToSeek.add(new Text(field));
            }
            try {
                forwardRanges.add(ShardIndexQueryTableStaticMethods.getBoundedRangeRange(range));
            } catch (IllegalRangeArgumentException e) {
                log.error("Error using range [" + range + "]", e);
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
                    familiesToSeek.add(new Text(field));
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

    @Override
    public Set<String> getOptionalQueryParameters() {
        Set<String> params = super.getOptionalQueryParameters();
        params.add(SEPARATE_COUNTS_BY_COLVIS);
        params.add(SUM_COUNTS);
        return params;
    }

    public boolean getSeparateCountsByColVis() {
        return separateCountsByColVis;
    }

    public void setSeparateCountsByColVis(boolean separateCountsByColVis) {
        this.separateCountsByColVis = separateCountsByColVis;
    }

    public boolean getShowReferenceCount() {
        return showReferenceCount;
    }

    public void setShowReferenceCount(boolean showReferenceCount) {
        this.showReferenceCount = showReferenceCount;
    }

    public boolean getSumCounts() {
        return sumCounts;
    }

    public void setSumCounts(boolean sumCounts) {
        this.sumCounts = sumCounts;
    }
}
