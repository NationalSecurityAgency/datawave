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
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
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

    /**
     * Used to specify whether to search against the reversed index.
     */
    public static final String REVERSE_INDEX = "reverse.index";

    private DiscoveryQueryConfiguration config;
    private MetadataHelper metadataHelper;

    /**
     * Basic constructor.
     */
    public DiscoveryLogic() {
        super();
    }

    /**
     * Copy constructor.
     *
     * @param other
     *            the other logic to copy
     */
    public DiscoveryLogic(DiscoveryLogic other) {
        super(other);
        this.config = new DiscoveryQueryConfiguration(other.config);
        this.metadataHelper = other.metadataHelper;
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

        // Check if the default model name and model table name have been overridden.
        setModelName(getOrDefault(settings, QueryParameters.PARAMETER_MODEL_NAME, getConfig().getModelName()));
        setModelTableName(getOrDefault(settings, QueryParameters.PARAMETER_MODEL_TABLE_NAME, getConfig().getModelTableName()));

        // Check if counts should be separated by column visibility.
        setSeparateCountsByColVis(getOrDefaultBoolean(settings, SEPARATE_COUNTS_BY_COLVIS, getSeparateCountsByColVis()));

        // Check if reference counts should be shown.
        setShowReferenceCount(getOrDefaultBoolean(settings, SHOW_REFERENCE_COUNT, getShowReferenceCount()));

        // Check if counts should be summed.
        setSumCounts(getOrDefaultBoolean(settings, SUM_COUNTS, getSumCounts()));

        // Check if any datatype filters were specified.
        getConfig().setDatatypeFilter(getOrDefaultSet(settings, QueryParameters.DATATYPE_FILTER_SET, getConfig().getDatatypeFilter()));

        // Update the query model.
        setQueryModel(metadataHelper.getQueryModel(getModelTableName(), getModelName(), null));

        // Set the currently indexed fields
        getConfig().setIndexedFields(metadataHelper.getIndexedFields(Collections.emptySet()));

        // Set the connector.
        getConfig().setClient(client);

        // Set the auths.
        getConfig().setAuthorizations(auths);

        // Get the ranges.
        getConfig().setBeginDate(settings.getBeginDate());
        getConfig().setEndDate(settings.getEndDate());

        // If a begin date was not specified, default to the earliest date.
        if (getConfig().getBeginDate() == null) {
            getConfig().setBeginDate(new Date(0L));
            log.warn("Begin date not specified, using earliest begin date.");
        }

        // If an end date was not specified, default to the latest date.
        if (getConfig().getEndDate() == null) {
            getConfig().setEndDate(new Date(Long.MAX_VALUE));
            log.warn("End date not specified, using latest end date.");
        }

        // Start with a trimmed version of the query, converted to JEXL
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
        CaseSensitivityVisitor.upperCaseIdentifiers(getConfig(), metadataHelper, script);

        // Apply the query model.
        Set<String> dataTypes = getConfig().getDatatypeFilter();
        Set<String> allFields;
        allFields = metadataHelper.getAllFields(dataTypes);
        script = QueryModelVisitor.applyModel(script, getQueryModel(), allFields);

        QueryValues literalsAndPatterns = FindLiteralsAndPatternsVisitor.find(script);
        Stopwatch timer = Stopwatch.createStarted();
        // No caching for getAllNormalizers, so try some magic with getFields...
        Multimap<String,Type<?>> dataTypeMap = ArrayListMultimap.create(metadataHelper.getFieldsToDatatypes(getConfig().getDatatypeFilter()));
        // We have a mapping of FIELD->DataType, but not a mapping of ANYFIELD->DataType which should be all datatypes.
        dataTypeMap.putAll(Constants.ANY_FIELD, getUniqueTypes(dataTypeMap.values()));
        timer.stop();
        log.debug("Took " + timer.elapsed(TimeUnit.MILLISECONDS) + "ms to get all the dataTypes.");

        getConfig().setLiterals(normalize(new LiteralNormalization(), literalsAndPatterns.getLiterals(), dataTypeMap));
        getConfig().setPatterns(normalize(new PatternNormalization(), literalsAndPatterns.getPatterns(), dataTypeMap));
        getConfig().setRanges(normalizeRanges(new LiteralNormalization(), literalsAndPatterns.getRanges(), dataTypeMap));
        if (log.isDebugEnabled()) {
            log.debug("Normalized Literals = " + getConfig().getLiterals());
            log.debug("Normalized Patterns = " + getConfig().getPatterns());
        }

        // Set the planned queries to execute.
        getConfig().setQueries(createQueries(getConfig()));

        return getConfig();
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
     * @return a mapping of the normalized tuples
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

    /**
     * Create and return a list of planned queries.
     *
     * @param config
     *            the config
     * @return the list of query data
     */
    private List<QueryData> createQueries(DiscoveryQueryConfiguration config) throws TableNotFoundException, ExecutionException {
        final List<QueryData> queries = Lists.newLinkedList();

        Set<String> familiesToSeek = Sets.newHashSet(); // This will be populated by createRanges().
        Pair<Set<Range>,Set<Range>> seekRanges = createRanges(config, familiesToSeek, metadataHelper);

        // Create the forward queries.
        queries.addAll(createQueriesFromRanges(config, seekRanges.getValue0(), familiesToSeek, false));

        // Create the reverse queries.
        queries.addAll(createQueriesFromRanges(config, seekRanges.getValue1(), familiesToSeek, true));

        if (log.isDebugEnabled()) {
            log.debug("Created ranges: " + queries);
        }

        return queries;
    }

    /**
     * Create planned queries for the given ranges.
     *
     * @param config
     *            the config
     * @param ranges
     *            the ranges
     * @param familiesToSeek
     *            the families to seek
     * @param reversed
     *            whether the ranges are for the reversed index
     * @return the queries
     */
    private List<QueryData> createQueriesFromRanges(DiscoveryQueryConfiguration config, Set<Range> ranges, Set<String> familiesToSeek, boolean reversed) {
        List<QueryData> queries = new ArrayList<>();
        if (!ranges.isEmpty()) {
            List<IteratorSetting> settings = getIteratorSettings(config, reversed);
            String tableName = reversed ? config.getReverseIndexTableName() : config.getIndexTableName();
            if (isCheckpointable()) {
                for (Range range : ranges) {
                    queries.add(new QueryData(tableName, null, Collections.singleton(range), familiesToSeek, settings));
                }
            } else {
                queries.add(new QueryData(tableName, null, ranges, familiesToSeek, settings));
            }
        }
        return queries;
    }

    /**
     * Creates two collections of ranges: one for the forward index (value0) and one for the reverse index (value1). If a literal has a field name, then the
     * Range for that term will include the column family. If there are multiple fields, then multiple ranges are created.
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
    private Pair<Set<Range>,Set<Range>> createRanges(DiscoveryQueryConfiguration config, Set<String> familiesToSeek, MetadataHelper metadataHelper)
                    throws TableNotFoundException, ExecutionException {
        Set<Range> forwardRanges = new HashSet<>();
        Set<Range> reverseRanges = new HashSet<>();

        // Evaluate the literals.
        for (Entry<String,String> literalAndField : config.getLiterals().entries()) {
            String literal = literalAndField.getKey(), field = literalAndField.getValue();
            // If the field is _ANYFIELD_, use null when making the range.
            field = Constants.ANY_FIELD.equals(field) ? null : field;
            // Mark the field as a family to seek if not null.
            if (field != null) {
                familiesToSeek.add(field);
            }
            forwardRanges.add(ShardIndexQueryTableStaticMethods.getLiteralRange(field, literal));
        }

        // Evaluate the ranges.
        for (Entry<String,LiteralRange<String>> rangeEntry : config.getRanges().entries()) {
            LiteralRange<String> range = rangeEntry.getValue();
            String field = rangeEntry.getKey();
            // If the field is _ANYFIELD_, use null when making the range.
            field = Constants.ANY_FIELD.equals(field) ? null : field;
            // Mark the field as a family to seek if not null.
            if (field != null) {
                familiesToSeek.add(field);
            }
            try {
                forwardRanges.add(ShardIndexQueryTableStaticMethods.getBoundedRangeRange(range));
            } catch (IllegalRangeArgumentException e) {
                log.error("Error using range [" + range + "]", e);
            }
        }

        // Evaluate the patterns.
        for (Entry<String,String> patternAndField : config.getPatterns().entries()) {
            String pattern = patternAndField.getKey(), field = patternAndField.getValue();
            // If the field is _ANYFIELD_, use null when making the range.
            field = Constants.ANY_FIELD.equals(field) ? null : field;
            // Mark the field as a family to seek if not null.
            if (field != null) {
                familiesToSeek.add(field);
            }
            ShardIndexQueryTableStaticMethods.RefactoredRangeDescription description;
            try {
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
     * Return the set of iterator settings that should be applied to queries for the given configuration.
     *
     * @param config
     *            the config
     * @param reverseIndex
     *            whether the iterator settings should be configured for a reversed index
     * @return the iterator settings
     */
    private List<IteratorSetting> getIteratorSettings(DiscoveryQueryConfiguration config, boolean reverseIndex) {
        List<IteratorSetting> settings = Lists.newLinkedList();

        // Add a date range filter.
        // The begin date from the query may be down to the second, for doing look-ups in the index we want to use the day because the times in the index table
        // have been truncated to the day.
        Date begin = DateUtils.truncate(config.getBeginDate(), Calendar.DAY_OF_MONTH);
        // we don't need to bump up the end date any more because it's not a part of the range set on the scanner.
        Date end = config.getEndDate();
        LongRange dateRange = new LongRange(begin.getTime(), end.getTime());
        settings.add(ShardIndexQueryTableStaticMethods.configureGlobalIndexDateRangeFilter(config, dateRange));

        // Add a datatype filter.
        settings.add(ShardIndexQueryTableStaticMethods.configureGlobalIndexDataTypeFilter(config, config.getDatatypeFilter()));

        // Add an iterator to match literals, patterns, and ranges against the index.
        IteratorSetting matchingIterator = configureIndexMatchingIterator(config, reverseIndex);
        if (matchingIterator != null) {
            settings.add(matchingIterator);
        }

        // Add an iterator to create the actual DiscoveryThings.
        settings.add(configureDiscoveryIterator(config, reverseIndex));

        return settings;
    }

    /**
     * Return a {@link IteratorSetting} for an {@link IndexMatchingIterator}.
     *
     * @param config
     *            the config
     * @param reverseIndex
     *            whether searching against the reversed index.
     * @return the iterator setting
     */
    private IteratorSetting configureIndexMatchingIterator(DiscoveryQueryConfiguration config, boolean reverseIndex) {
        Multimap<String,String> literals = config.getLiterals();
        Multimap<String,String> patterns = config.getPatterns();
        Multimap<String,LiteralRange<String>> ranges = config.getRanges();

        if ((literals == null || literals.isEmpty()) && (patterns == null || patterns.isEmpty()) && (ranges == null || ranges.isEmpty())) {
            return null;
        }
        log.debug("Configuring IndexMatchingIterator with " + literals + " and " + patterns);

        IteratorSetting cfg = new IteratorSetting(config.getBaseIteratorPriority() + 23, "termMatcher", IndexMatchingIterator.class);

        IndexMatchingIterator.Configuration conf = new IndexMatchingIterator.Configuration();
        // Add literals.
        if (literals != null) {
            for (Entry<String,String> literal : literals.entries()) {
                if (Constants.ANY_FIELD.equals(literal.getValue())) {
                    conf.addLiteral(literal.getKey());
                } else {
                    conf.addLiteral(literal.getKey(), literal.getValue());
                }
            }
        }
        // Add patterns.
        if (patterns != null) {
            for (Entry<String,String> pattern : patterns.entries()) {
                if (Constants.ANY_FIELD.equals(pattern.getValue())) {
                    conf.addPattern(pattern.getKey());
                } else {
                    conf.addPattern(pattern.getKey(), pattern.getValue());
                }
            }
        }
        // Add ranges.
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

    /**
     * Return an {@link IteratorSetting} for an {@link DiscoveryIterator}.
     *
     * @param config
     *            the config
     * @param reverseIndex
     *            whether searching against the reversed index.
     * @return the iterator setting
     */
    private IteratorSetting configureDiscoveryIterator(DiscoveryQueryConfiguration config, boolean reverseIndex) {
        IteratorSetting setting = new IteratorSetting(config.getBaseIteratorPriority() + 50, DiscoveryIterator.class);
        setting.addOption(REVERSE_INDEX, Boolean.toString(reverseIndex));
        setting.addOption(SEPARATE_COUNTS_BY_COLVIS, Boolean.toString(config.getSeparateCountsByColVis()));
        setting.addOption(SHOW_REFERENCE_COUNT, Boolean.toString(config.getShowReferenceCount()));
        setting.addOption(SUM_COUNTS, Boolean.toString(config.getSumCounts()));
        return setting;
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

            iterators.add(transformScanner(bs, qd, config.getIndexedFields()));
        }
        this.iterator = concat(iterators.iterator());
    }

    @Override
    public ShardIndexQueryTable clone() {
        return new DiscoveryLogic(this);
    }

    /**
     * Takes in a batch scanner, removes all DiscoveredThings that do not have an indexed field, and returns an iterator over the DiscoveredThing objects
     * contained in the value.
     *
     * @param scanner
     *            a batch scanner
     * @param indexedFields
     *            set of currently indexed fields
     * @return iterator for discoveredthings
     */
    private Iterator<DiscoveredThing> transformScanner(final BatchScanner scanner, final QueryData queryData, Set<String> indexedFields) {
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
                    // Check to see if the field is currently indexed, if it's not, we should NOT be adding it to 'thangs'
                    if (indexedFields.contains(((DiscoveredThing) w).getField())) {
                        thangs.add((DiscoveredThing) w);
                    } else {
                        log.debug(((DiscoveredThing) w).getField() + " was NOT found in IndexedFields");
                    }
                }
                return thangs.iterator();
            }
        }));
    }

    @Override
    public Set<String> getOptionalQueryParameters() {
        Set<String> params = super.getOptionalQueryParameters();
        params.add(SEPARATE_COUNTS_BY_COLVIS);
        params.add(SUM_COUNTS);
        return params;
    }

    public boolean getSeparateCountsByColVis() {
        return getConfig().getSeparateCountsByColVis();
    }

    public void setSeparateCountsByColVis(boolean separateCountsByColVis) {
        getConfig().setSeparateCountsByColVis(separateCountsByColVis);
    }

    public boolean getShowReferenceCount() {
        return getConfig().getShowReferenceCount();
    }

    public void setShowReferenceCount(boolean showReferenceCount) {
        getConfig().setShowReferenceCount(showReferenceCount);
    }

    public boolean getSumCounts() {
        return getConfig().getSumCounts();
    }

    public void setSumCounts(boolean sumCounts) {
        getConfig().setSumCounts(sumCounts);
    }
}
