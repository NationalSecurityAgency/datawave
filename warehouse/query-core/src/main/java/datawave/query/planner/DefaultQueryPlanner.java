package datawave.query.planner;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import datawave.core.iterators.querylock.QueryLock;
import datawave.data.type.GeometryType;
import datawave.data.type.Type;
import datawave.ingest.mapreduce.handler.dateindex.DateIndexUtil;
import datawave.query.CloseableIterable;
import datawave.query.Constants;
import datawave.query.QueryParameters;
import datawave.query.composite.CompositeMetadata;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.CannotExpandUnfieldedTermFatalException;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.DatawaveQueryException;
import datawave.query.exceptions.DoNotPerformOptimizedQueryException;
import datawave.query.exceptions.FullTableScansDisallowedException;
import datawave.query.exceptions.InvalidQueryException;
import datawave.query.exceptions.NoResultsException;
import datawave.query.index.lookup.IndexStream.StreamContext;
import datawave.query.index.lookup.RangeStream;
import datawave.query.iterator.CloseableListIterable;
import datawave.query.iterator.QueryIterator;
import datawave.query.iterator.QueryOptions;
import datawave.query.iterator.logic.IndexIterator;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.functions.EvaluationPhaseFilterFunctions;
import datawave.query.jexl.functions.QueryFunctions;
import datawave.query.jexl.visitors.BoundedRangeDetectionVisitor;
import datawave.query.jexl.visitors.CaseSensitivityVisitor;
import datawave.query.jexl.visitors.DepthVisitor;
import datawave.query.jexl.visitors.ExecutableDeterminationVisitor;
import datawave.query.jexl.visitors.ExecutableDeterminationVisitor.STATE;
import datawave.query.jexl.visitors.ExpandCompositeTerms;
import datawave.query.jexl.visitors.ExpandMultiNormalizedTerms;
import datawave.query.jexl.visitors.FetchDataTypesVisitor;
import datawave.query.jexl.visitors.FieldMissingFromSchemaVisitor;
import datawave.query.jexl.visitors.FixNegativeNumbersVisitor;
import datawave.query.jexl.visitors.FixUnfieldedTermsVisitor;
import datawave.query.jexl.visitors.FixUnindexedNumericTerms;
import datawave.query.jexl.visitors.FunctionIndexQueryExpansionVisitor;
import datawave.query.jexl.visitors.IsNotNullIntentVisitor;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.Negations;
import datawave.query.jexl.visitors.ParallelIndexExpansion;
import datawave.query.jexl.visitors.PrintingVisitor;
import datawave.query.jexl.visitors.PullupUnexecutableNodesVisitor;
import datawave.query.jexl.visitors.PushFunctionsIntoExceededValueRanges;
import datawave.query.jexl.visitors.PushdownLowSelectivityNodesVisitor;
import datawave.query.jexl.visitors.PushdownMissingIndexRangeNodesVisitor;
import datawave.query.jexl.visitors.PushdownUnexecutableNodesVisitor;
import datawave.query.jexl.visitors.QueryModelVisitor;
import datawave.query.jexl.visitors.QueryOptionsFromQueryVisitor;
import datawave.query.jexl.visitors.RangeCoalescingVisitor;
import datawave.query.jexl.visitors.RangeConjunctionRebuildingVisitor;
import datawave.query.jexl.visitors.RegexFunctionVisitor;
import datawave.query.jexl.visitors.SetMembershipVisitor;
import datawave.query.jexl.visitors.SortedUIDsRequiredVisitor;
import datawave.query.jexl.visitors.TermCountingVisitor;
import datawave.query.jexl.visitors.TreeFlatteningRebuildingVisitor;
import datawave.query.jexl.visitors.ValidPatternVisitor;
import datawave.query.model.QueryModel;
import datawave.query.planner.comparator.DefaultQueryPlanComparator;
import datawave.query.planner.comparator.GeoWaveQueryPlanComparator;
import datawave.query.planner.pushdown.PushDownVisitor;
import datawave.query.planner.pushdown.rules.PushDownRule;
import datawave.query.postprocessing.tf.Function;
import datawave.query.postprocessing.tf.TermOffsetPopulator;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MetadataHelper;
import datawave.query.util.QueryStopwatch;
import datawave.query.util.Tuple2;
import datawave.util.time.TraceStopwatch;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl.Parameter;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.configuration.QueryData;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NotFoundQueryException;
import datawave.webservice.query.exception.PreConditionFailedQueryException;
import datawave.webservice.query.exception.QueryException;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.PatternSyntaxException;

public class DefaultQueryPlanner extends QueryPlanner {
    
    private static final Logger log = ThreadConfigurableLogger.getLogger(DefaultQueryPlanner.class);
    
    public static String EXCEED_TERM_EXPANSION_ERROR = "Query failed because it exceeded the query term expansion threshold";
    
    protected boolean limitScanners = false;
    
    /**
     * Allows developers to disable bounded lookup of ranges and regexes. This will be optimized in future releases.
     */
    protected boolean disableBoundedLookup = false;
    
    /**
     * Allows developers to disable any field lookups
     */
    protected boolean disableAnyFieldLookup = false;
    
    /**
     * Allows developers to disable expansion of composite fields
     */
    protected boolean disableCompositeFields = false;
    
    /**
     * Disables the test for non existent fields.
     */
    protected boolean disableTestNonExistentFields = false;
    
    /**
     * Disables the index expansion function
     */
    protected boolean disableExpandIndexFunction = false;
    
    /**
     * Disables the range coalescing visitor
     */
    protected boolean disableRangeCoalescing = false;
    
    /**
     * Allows developers to cache data types
     */
    protected boolean cacheDataTypes = false;
    
    /**
     * Overrides behavior with doc specific ranges
     */
    protected boolean docSpecificOverride = false;
    
    /**
     * Number of documents to combine for concurrent evaluation
     *
     */
    protected int docsToCombineForEvaluation = -1;
    
    /**
     * Boolean it identify if we wish to condense
     */
    protected boolean condenseUidsInRangeStream = true;
    
    /**
     * Boolean it identify if we wish to condense
     */
    protected boolean compressUidsInRangeStream = true;
    
    private final long maxRangesPerQueryPiece;
    
    private static Cache<String,Set<String>> allFieldTypeMap = CacheBuilder.newBuilder().maximumSize(100).concurrencyLevel(100)
                    .expireAfterAccess(24, TimeUnit.HOURS).build();
    
    private static Cache<String,Multimap<String,Type<?>>> dataTypeMap = CacheBuilder.newBuilder().maximumSize(100).concurrencyLevel(100)
                    .expireAfterAccess(24, TimeUnit.HOURS).build();
    
    private static Multimap<String,Type<?>> queryFieldsAsDataTypeMap;
    
    private static Multimap<String,Type<?>> normalizedFieldAsDataTypeMap;
    
    private static Set<String> cachedIndexedFields = null;
    private static Set<String> cachedNormalizedFields = null;
    
    protected List<PushDownRule> rules = Lists.newArrayList();
    
    protected Class<? extends SortedKeyValueIterator<Key,Value>> queryIteratorClazz = QueryIterator.class;
    
    protected String plannedScript = null;
    
    protected MetadataHelper metadataHelper = null;
    
    protected DateIndexHelper dateIndexHelper = null;
    
    protected boolean compressMappings;
    
    protected boolean buildQueryModel = true;
    
    protected boolean preloadOptions = false;
    
    protected String rangeStreamClass = RangeStream.class.getCanonicalName();
    
    protected ExecutorService builderThread = null;
    
    protected Future<IteratorSetting> settingFuture = null;
    
    protected long maxRangeWaitMillis = 125;
    
    /**
     * threshold for pushing down ranges to be all shard specific if we exceed a certain number of terms.
     */
    protected long pushdownThreshold = 500;
    
    /**
     * Maximum number of sources to open per query.
     */
    protected long sourceLimit = -1;
    
    protected QueryModelProvider.Factory queryModelProviderFactory = new MetadataHelperQueryModelProvider.Factory();
    
    public DefaultQueryPlanner() {
        this(Long.MAX_VALUE);
    }
    
    public DefaultQueryPlanner(long _maxRangesPerQueryPiece) {
        this(_maxRangesPerQueryPiece, true);
    }
    
    public DefaultQueryPlanner(long maxRangesPerQueryPiece, boolean limitScanners) {
        this.maxRangesPerQueryPiece = maxRangesPerQueryPiece;
        setLimitScanners(limitScanners);
    }
    
    protected DefaultQueryPlanner(DefaultQueryPlanner other) {
        this(other.maxRangesPerQueryPiece, other.limitScanners);
        setRangeStreamClass(other.getRangeStreamClass());
        setCacheDataTypes(other.getCacheDataTypes());
        setDisableAnyFieldLookup(other.disableAnyFieldLookup);
        setDisableBoundedLookup(other.disableBoundedLookup);
        setDisableCompositeFields(other.disableCompositeFields);
        setDisableTestNonExistentFields(other.disableTestNonExistentFields);
        setDisableExpandIndexFunction(other.disableExpandIndexFunction);
        setDisableRangeCoalescing(other.disableRangeCoalescing);
        if (null != other.cachedIndexedFields)
            cachedIndexedFields = Sets.newHashSet(other.cachedIndexedFields);
        if (null != other.cachedNormalizedFields)
            cachedNormalizedFields = Sets.newHashSet(other.cachedNormalizedFields);
        rules.addAll(other.rules);
        queryIteratorClazz = other.queryIteratorClazz;
        setMetadataHelper(other.getMetadataHelper());
        setDateIndexHelper(other.getDateIndexHelper());
        setCompressOptionMappings(other.compressMappings);
        buildQueryModel = other.buildQueryModel;
        preloadOptions = other.preloadOptions;
        rangeStreamClass = other.rangeStreamClass;
        setSourceLimit(other.sourceLimit);
        setDocsToCombineForEvaluation(other.getDocsToCombineForEvaluation());
        setCondenseUidsInRangeStream(other.getCondenseUidsInRangeStream());
        setPushdownThreshold(other.getPushdownThreshold());
    }
    
    public void setMetadataHelper(final MetadataHelper metadataHelper) {
        this.metadataHelper = metadataHelper;
    }
    
    public void setRangeStreamClass(String clazz) {
        this.rangeStreamClass = clazz;
    }
    
    public String getRangeStreamClass() {
        return rangeStreamClass;
    }
    
    public MetadataHelper getMetadataHelper() {
        return (metadataHelper);
    }
    
    private MetadataHelper getMetadataHelper(final ShardQueryConfiguration config) {
        if (null == metadataHelper) {
            throw new RuntimeException("metadataHelper was not set. fix this");
        }
        
        return metadataHelper;
    }
    
    public void setDateIndexHelper(final DateIndexHelper dateIndexHelper) {
        this.dateIndexHelper = dateIndexHelper;
    }
    
    private DateIndexHelper getDateIndexHelper() {
        return dateIndexHelper;
    }
    
    private DateIndexHelper getDateIndexHelper(final ShardQueryConfiguration config) {
        if (null == dateIndexHelper && config.getDateIndexTableName() != null && !(config.getDateIndexTableName().isEmpty())) {
            throw new RuntimeException("dateIndexHelper was not set. fix this");
        }
        
        return dateIndexHelper;
    }
    
    @Override
    public CloseableIterable<QueryData> process(GenericQueryConfiguration genericConfig, String query, Query settings, ScannerFactory scannerFactory)
                    throws DatawaveQueryException {
        if (!(genericConfig instanceof ShardQueryConfiguration)) {
            throw new ClassCastException("Config object must be an instance of ShardQueryConfiguration");
        }
        
        builderThread = Executors.newSingleThreadExecutor();
        
        ShardQueryConfiguration config = (ShardQueryConfiguration) genericConfig;
        
        // lets mark the query as started (used by ivarators at a minimum)
        try {
            markQueryStarted(config, settings);
        } catch (Exception e) {
            throw new DatawaveQueryException("Failed to mark query as started" + settings.getId(), e);
        }
        
        return process(scannerFactory, getMetadataHelper(config), getDateIndexHelper(config), config, query, settings);
    }
    
    protected CloseableIterable<QueryData> process(ScannerFactory scannerFactory, MetadataHelper metadataHelper, DateIndexHelper dateIndexHelper,
                    ShardQueryConfiguration config, String query, Query settings) throws DatawaveQueryException {
        final QueryData queryData = new QueryData();
        
        settingFuture = null;
        
        IteratorSetting cfg = null;
        
        if (preloadOptions) {
            cfg = getQueryIterator(metadataHelper, config, settings, "", false);
        }
        
        ASTJexlScript queryTree = null;
        try {
            queryTree = updateQueryTree(scannerFactory, metadataHelper, dateIndexHelper, config, query, queryData, settings);
        } catch (StackOverflowError e) {
            if (log.isTraceEnabled()) {
                log.trace("Stack trace for overflow " + e);
            }
            PreConditionFailedQueryException qe = new PreConditionFailedQueryException(DatawaveErrorCode.QUERY_DEPTH_OR_TERM_THRESHOLD_EXCEEDED, e);
            log.warn(qe);
            throw new DatawaveFatalQueryException(qe);
        } catch (NoResultsException e) {
            if (log.isTraceEnabled()) {
                log.trace("Definitively determined that no results exist from the indexes");
            }
            
            return DefaultQueryPlanner.emptyCloseableIterator();
        }
        
        final QueryStopwatch timers = config.getTimers();
        
        Tuple2<CloseableIterable<QueryPlan>,Boolean> queryRanges = getQueryRanges(scannerFactory, metadataHelper, config, queryTree);
        
        // a full table scan is required if
        final boolean isFullTable = queryRanges.second();
        
        // abort if we cannot handle full table scans
        if (isFullTable && !config.getFullTableScanEnabled()) {
            PreConditionFailedQueryException qe = new PreConditionFailedQueryException(DatawaveErrorCode.FULL_TABLE_SCAN_REQUIRED_BUT_DISABLED);
            throw new FullTableScansDisallowedException(qe);
        }
        
        TraceStopwatch stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - Rebuild JEXL String from AST");
        
        // Set the final query after we're done mucking with it
        String newQueryString = JexlStringBuildingVisitor.buildQuery(queryTree);
        if (log.isTraceEnabled())
            log.trace("newQueryString is " + newQueryString);
        if (StringUtils.isBlank(newQueryString)) {
            stopwatch.stop();
            QueryException qe = new QueryException(DatawaveErrorCode.EMPTY_QUERY_STRING_AFTER_MODIFICATION);
            throw new DatawaveFatalQueryException(qe);
        }
        
        stopwatch.stop();
        stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - Construct IteratorSettings");
        
        queryData.setQuery(newQueryString);
        
        while (null == cfg) {
            cfg = getQueryIterator(metadataHelper, config, settings, "", false);
        }
        configureIterator(config, cfg, newQueryString, isFullTable);
        
        // Load the IteratorSettings into the QueryData instance
        queryData.setSettings(Lists.newArrayList(cfg));
        
        stopwatch.stop();
        
        this.plannedScript = newQueryString;
        // docsToCombineForEvaluation is only enabled when threading is used
        if (config.getMaxEvaluationPipelines() == 1)
            docsToCombineForEvaluation = -1;
        
        // add the geo query comparator to sort by geo range granularity if this is a geo query
        List<Comparator<QueryPlan>> queryPlanComparators = null;
        if (config.isSortGeoWaveQueryRanges()) {
            List<String> geoFields = new ArrayList<>();
            for (String fieldName : config.getIndexedFields()) {
                for (Type type : config.getQueryFieldsDatatypes().get(fieldName)) {
                    if (type instanceof GeometryType) {
                        geoFields.add(fieldName);
                        break;
                    }
                }
            }
            
            if (!geoFields.isEmpty()) {
                queryPlanComparators = new ArrayList<>();
                queryPlanComparators.add(new GeoWaveQueryPlanComparator(geoFields));
                queryPlanComparators.add(new DefaultQueryPlanComparator());
            }
        }
        
        // @formatter:off
        return new ThreadedRangeBundler.Builder()
                .setOriginal(queryData)
                .setQueryTree(queryTree)
                .setRanges(queryRanges.first())
                .setMaxRanges(maxRangesPerQueryPiece())
                .setDocsToCombine(docsToCombineForEvaluation)
                .setSettings(settings)
                .setDocSpecificLimitOverride(docSpecificOverride)
                .setMaxRangeWaitMillis(maxRangeWaitMillis)
                .setQueryPlanComparators(queryPlanComparators)
                .setNumRangesToBuffer(config.getNumRangesToBuffer())
                .setRangeBufferTimeoutMillis(config.getRangeBufferTimeoutMillis())
                .setRangeBufferPollMillis(config.getRangeBufferPollMillis())
                .build();
        // @formatter:on
    }
    
    private void configureIterator(ShardQueryConfiguration config, IteratorSetting cfg, String newQueryString, boolean isFullTable)
                    throws DatawaveQueryException {
        
        // Load enrichers, filters, unevaluatedExpressions, and projection
        // fields
        setCommonIteratorOptions(config, cfg);
        
        addOption(cfg, QueryOptions.LIMIT_FIELDS, config.getLimitFieldsAsString(), true);
        addOption(cfg, QueryOptions.GROUP_FIELDS, config.getGroupFieldsAsString(), true);
        addOption(cfg, QueryOptions.GROUP_FIELDS_BATCH_SIZE, config.getGroupFieldsBatchSizeAsString(), true);
        addOption(cfg, QueryOptions.UNIQUE_FIELDS, config.getUniqueFieldsAsString(), true);
        addOption(cfg, QueryOptions.HIT_LIST, Boolean.toString(config.isHitList()), false);
        addOption(cfg, QueryOptions.TYPE_METADATA_IN_HDFS, Boolean.toString(config.isTypeMetadataInHdfs()), true);
        addOption(cfg, QueryOptions.TERM_FREQUENCY_FIELDS, Joiner.on(',').join(config.getQueryTermFrequencyFields()), false);
        addOption(cfg, QueryOptions.TERM_FREQUENCIES_REQUIRED, Boolean.toString(config.isTermFrequenciesRequired()), true);
        addOption(cfg, QueryOptions.QUERY, newQueryString, false);
        addOption(cfg, QueryOptions.QUERY_ID, config.getQuery().getId().toString(), false);
        addOption(cfg, QueryOptions.FULL_TABLE_SCAN_ONLY, Boolean.toString(isFullTable), false);
        addOption(cfg, QueryOptions.TRACK_SIZES, Boolean.toString(config.isTrackSizes()), true);
        // Set the start and end dates
        configureTypeMappings(config, cfg, metadataHelper, compressMappings);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see QueryPlanner#close(datawave. webservice .query.configuration.GenericQueryConfiguration, datawave.webservice.query.Query)
     */
    @Override
    public void close(GenericQueryConfiguration genericConfig, Query settings) {
        if (!(genericConfig instanceof ShardQueryConfiguration)) {
            if (genericConfig != null) {
                log.warn("Config object must be an instance of ShardQueryConfiguration to properly close the DefaultQueryPlanner. You gave me a "
                                + genericConfig);
            }
            if (null != builderThread) {
                builderThread.shutdown();
            }
            return;
        }
        
        ShardQueryConfiguration config = (ShardQueryConfiguration) genericConfig;
        
        // lets mark the query as closed (used by ivarators at a minimum)
        try {
            markQueryStopped(config, settings);
        } catch (Exception e) {
            log.error("Failed to close query " + settings.getId(), e);
        }
        
        if (null != builderThread)
            builderThread.shutdown();
    }
    
    private QueryLock getQueryLock(ShardQueryConfiguration config, Query settings) throws Exception {
        return new QueryLock.Builder().forQueryId(settings.getId() == null ? null : settings.getId().toString()).forZookeeper(config.getZookeeperConfig(), 0)
                        .forHdfs(config.getHdfsSiteConfigURLs()).forIvaratorDirs(config.getIvaratorCacheBaseURIs())
                        .forFstDirs(config.getIvaratorFstHdfsBaseURIs()).build();
    }
    
    private void markQueryStopped(ShardQueryConfiguration config, Query settings) throws Exception {
        QueryLock lock = getQueryLock(config, settings);
        if (lock != null) {
            try {
                lock.stopQuery();
            } finally {
                lock.cleanup();
            }
        }
    }
    
    private void markQueryStarted(ShardQueryConfiguration config, Query settings) throws Exception {
        QueryLock lock = getQueryLock(config, settings);
        if (lock != null) {
            try {
                lock.startQuery();
            } finally {
                lock.cleanup();
            }
        }
    }
    
    public static void validateQuerySize(String lastOperation, JexlNode queryTree, ShardQueryConfiguration config) {
        final QueryStopwatch timers = config.getTimers();
        TraceStopwatch stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - Validate against term and depth thresholds");
        
        // check the query depth (up to config.getMaxDepthThreshold() + 1)
        int depth = DepthVisitor.getDepth(queryTree, config.getMaxDepthThreshold());
        if (depth > config.getMaxDepthThreshold()) {
            PreConditionFailedQueryException qe = new PreConditionFailedQueryException(DatawaveErrorCode.QUERY_DEPTH_THRESHOLD_EXCEEDED, MessageFormat.format(
                            "{0} > {1}, last operation: {2}", depth, config.getMaxDepthThreshold(), lastOperation));
            throw new DatawaveFatalQueryException(qe);
        }
        
        // count the terms
        int termCount = TermCountingVisitor.countTerms(queryTree);
        if (termCount > config.getMaxTermThreshold()) {
            PreConditionFailedQueryException qe = new PreConditionFailedQueryException(DatawaveErrorCode.QUERY_TERM_THRESHOLD_EXCEEDED, MessageFormat.format(
                            "{0} > {1}, last operation: {2}", termCount, config.getMaxTermThreshold(), lastOperation));
            throw new DatawaveFatalQueryException(qe);
        }
        stopwatch.stop();
    }
    
    protected ASTJexlScript updateQueryTree(ScannerFactory scannerFactory, MetadataHelper metadataHelper, DateIndexHelper dateIndexHelper,
                    ShardQueryConfiguration config, String query, QueryData queryData, Query settings) throws DatawaveQueryException {
        final QueryStopwatch timers = config.getTimers();
        
        TraceStopwatch stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - get composites");
        if (!disableCompositeFields) {
            
            try {
                config.setCompositeToFieldMap(metadataHelper.getCompositeToFieldMap(config.getDatatypeFilter()));
                config.setCompositeTransitionDates(metadataHelper.getCompositeTransitionDateMap(config.getDatatypeFilter()));
                config.setFixedLengthFields(metadataHelper.getFixedLengthCompositeFields(config.getDatatypeFilter()));
            } catch (TableNotFoundException ex) {
                QueryException qe = new QueryException(DatawaveErrorCode.COMPOSITES_RETRIEVAL_ERROR, ex);
                log.warn(qe);
                throw new DatawaveQueryException(qe);
            }
            
        }
        stopwatch.stop();
        stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - Parse query");
        
        ASTJexlScript queryTree = parseQueryAndValidatePattern(query, stopwatch);
        
        if (log.isDebugEnabled()) {
            logQuery(queryTree, "Query after initial parse:");
        }
        
        stopwatch.stop();
        
        if (query.contains(QueryFunctions.QUERY_FUNCTION_NAMESPACE + ':')) {
            // only do the extra tree visit if the function is present
            stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - parse out queryOptions from options function");
            Map<String,String> optionsMap = new HashMap<>();
            queryTree = QueryOptionsFromQueryVisitor.collect(queryTree, optionsMap);
            if (!optionsMap.isEmpty()) {
                QueryOptionsSwitch.apply(optionsMap, config);
            }
            stopwatch.stop();
        }
        
        // groom the query so that any nodes with the literal on the left and the identifier on
        // the right will be re-ordered to simplify subsequent processing
        stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - fix not null intent");
        queryTree = JexlASTHelper.InvertNodeVisitor.invertSwappedNodes(queryTree);
        if (log.isDebugEnabled()) {
            logQuery(queryTree, "Query after inverting swapped nodes:");
        }
        stopwatch.stop();
        
        stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - fix not null intent");
        
        try {
            queryTree = IsNotNullIntentVisitor.fixNotNullIntent(queryTree);
        } catch (Exception e1) {
            throw new DatawaveQueryException("Something bad happened", e1);
        }
        if (log.isDebugEnabled()) {
            logQuery(queryTree, "Query afterfixing not null intent:");
        }
        
        stopwatch.stop();
        
        stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - include date filters");
        
        try {
            queryTree = addDateFilters(queryTree, scannerFactory, metadataHelper, dateIndexHelper, config, settings);
        } catch (TableNotFoundException e1) {
            throw new DatawaveQueryException("Unable to resolve date index", e1);
        }
        if (log.isDebugEnabled()) {
            logQuery(queryTree, "Query after adding date filters:");
        }
        
        stopwatch.stop();
        
        stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - cap date range");
        
        // note this must be called after we do the date adjustements per the query date type in addDateFilters
        capDateRange(config);
        
        stopwatch.stop();
        
        stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - flatten");
        
        // flatten the tree
        queryTree = TreeFlatteningRebuildingVisitor.flatten(queryTree);
        
        if (log.isDebugEnabled()) {
            logQuery(queryTree, "Query after initial flatten:");
        }
        
        stopwatch.stop();
        
        validateQuerySize("initial parse", queryTree, config);
        
        stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - initial re-write");
        
        queryTree = applyRules(queryTree, scannerFactory, metadataHelper, config);
        
        if (log.isDebugEnabled()) {
            logQuery(queryTree, "Query after applying pushdown rules:");
        }
        
        stopwatch.stop();
        stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - Restructure negative numbers");
        
        queryTree = FixNegativeNumbersVisitor.fix(queryTree);
        
        if (log.isDebugEnabled()) {
            logQuery(queryTree, "Query after restructuring negative numbers:");
        }
        
        stopwatch.stop();
        stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - Uppercase all field names");
        
        // Ensure that all ASTIdentifier nodes (field names) are upper-case, as
        // this
        // is enforced at ingest time
        CaseSensitivityVisitor.upperCaseIdentifiers(config, metadataHelper, queryTree);
        
        if (log.isDebugEnabled()) {
            logQuery(queryTree, "Query after uppercase'ing field names:");
        }
        
        stopwatch.stop();
        stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - Rewrite negated equality operators.");
        
        Negations.rewrite(queryTree);
        if (log.isDebugEnabled()) {
            logQuery(queryTree, "Query after rewriting negated equality operators:");
        }
        
        stopwatch.stop();
        stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - Apply query model");
        
        QueryModel queryModel = null;
        QueryModelProvider queryModelProvider = this.queryModelProviderFactory.createQueryModelProvider();
        if (queryModelProvider instanceof MetadataHelperQueryModelProvider) {
            ((MetadataHelperQueryModelProvider) queryModelProvider).setMetadataHelper(metadataHelper);
            ((MetadataHelperQueryModelProvider) queryModelProvider).setConfig(config);
        }
        queryModel = queryModelProvider.getQueryModel();
        
        if (null != queryModel) {
            queryTree = applyQueryModel(metadataHelper, config, stopwatch, queryTree, queryModel);
        }
        
        if (log.isDebugEnabled()) {
            logQuery(queryTree, "Query after applying query model:");
        }
        
        stopwatch.stop();
        
        Set<String> indexOnlyFields;
        try {
            indexOnlyFields = metadataHelper.getIndexOnlyFields(config.getDatatypeFilter());
        } catch (TableNotFoundException e) {
            QueryException qe = new QueryException(DatawaveErrorCode.INDEX_ONLY_FIELDS_RETRIEVAL_ERROR, e);
            throw new DatawaveFatalQueryException(qe);
        }
        if (disableBoundedLookup) {
            // protection mechanism. If we disable bounded ranges and have a
            // LT,GT or ER node, we should expand it
            if (BoundedRangeDetectionVisitor.mustExpandBoundedRange(config, metadataHelper, queryTree))
                disableBoundedLookup = false;
        }
        if (!indexOnlyFields.isEmpty()) {
            // rebuild the query tree
            queryTree = RegexFunctionVisitor.expandRegex(config, metadataHelper, indexOnlyFields, queryTree);
        }
        
        queryTree = processTree(queryTree, config, settings, metadataHelper, scannerFactory, queryData, timers, queryModel);
        
        // ExpandCompositeTerms was here
        
        stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - Determine if query contains index-only fields");
        
        // Figure out if the query contained any index only terms so we know
        // if we have to force it down the field-index path with event-specific
        // ranges
        boolean containsIndexOnlyFields = false;
        if (!indexOnlyFields.isEmpty() && !disableBoundedLookup) {
            boolean functionsEnabled = config.isIndexOnlyFilterFunctionsEnabled();
            containsIndexOnlyFields = !SetMembershipVisitor.getMembers(indexOnlyFields, config, metadataHelper, dateIndexHelper, queryTree, functionsEnabled)
                            .isEmpty();
        }
        
        // Print the nice log message
        if (log.isDebugEnabled()) {
            logQuery(queryTree, "Computed that the query " + (containsIndexOnlyFields ? " contains " : " does not contain any ") + " index only field(s)");
        }
        
        config.setContainsIndexOnlyTerms(containsIndexOnlyFields);
        
        stopwatch.stop();
        
        stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - Determine if query contains composite fields");
        
        Multimap<String,String> compositeFields;
        try {
            compositeFields = metadataHelper.getCompositeToFieldMap(config.getDatatypeFilter());
        } catch (TableNotFoundException e) {
            stopwatch.stop();
            QueryException qe = new QueryException(DatawaveErrorCode.COMPOSITES_RETRIEVAL_ERROR, e);
            throw new DatawaveFatalQueryException(qe);
        }
        
        // Figure out if the query contained any index-only terms so we know
        // if we have to force it down the field-index path with event-specific
        // ranges
        boolean containsComposites = false;
        if (!compositeFields.isEmpty()) {
            boolean functionsEnabled = config.isCompositeFilterFunctionsEnabled();
            containsComposites = !SetMembershipVisitor.getMembers(compositeFields.keySet(), config, metadataHelper, dateIndexHelper, queryTree,
                            functionsEnabled).isEmpty();
        }
        
        // Print the nice log message
        if (log.isDebugEnabled()) {
            logQuery(queryTree, "Computed that the query " + (containsComposites ? " contains " : " does not contain any ") + " composite field(s)");
        }
        
        config.setContainsCompositeTerms(containsComposites);
        
        stopwatch.stop();
        
        stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - Determine if we can deal with unsorted UIDs");
        
        // determine whether sortedUIDs are required. Normally they are, however if the query contains
        // only one indexed term, then there is no need to sort which can be a lot faster if an ivarator
        // is required.
        boolean sortedUIDs = areSortedUIDsRequired(queryTree, config);
        
        // Print the nice log message
        if (log.isDebugEnabled()) {
            logQuery(queryTree, "Computed that the query " + (sortedUIDs ? " requires " : " does not require ") + " sorted UIDs");
        }
        
        config.setSortedUIDs(sortedUIDs);
        
        stopwatch.stop();
        
        stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - Determine if query contains term frequency (tokenized) fields");
        
        // Figure out if the query contained any term frequency terms so we know
        // if we may use the term frequencies instead of the fields index in some cases
        Set<String> queryTfFields = Collections.emptySet();
        Set<String> termFrequencyFields;
        try {
            termFrequencyFields = metadataHelper.getTermFrequencyFields(config.getDatatypeFilter());
        } catch (TableNotFoundException e) {
            stopwatch.stop();
            QueryException qe = new QueryException(DatawaveErrorCode.TERM_FREQUENCY_FIELDS_RETRIEVAL_ERROR, e);
            throw new DatawaveFatalQueryException(qe);
        }
        if (!termFrequencyFields.isEmpty()) {
            queryTfFields = SetMembershipVisitor.getMembers(termFrequencyFields, config, metadataHelper, dateIndexHelper, queryTree);
            
            // Print the nice log message
            if (log.isDebugEnabled()) {
                logQuery(queryTree, "Computed that the query " + (queryTfFields.isEmpty() ? " does not contain any " : "contains " + queryTfFields + " as ")
                                + " term frequency field(s)");
            }
        }
        
        config.setQueryTermFrequencyFields(queryTfFields);
        
        // now determine if we actually require gathering term frequencies
        if (!queryTfFields.isEmpty()) {
            Multimap<String,Function> contentFunctions = TermOffsetPopulator.getContentFunctions(queryTree);
            config.setTermFrequenciesRequired(!contentFunctions.isEmpty());
            
            // Print the nice log message
            if (log.isDebugEnabled()) {
                logQuery(queryTree, "Computed that the query " + (contentFunctions.isEmpty() ? " does not require " : "requires") + " term frequency lookup");
            }
        }
        
        stopwatch.stop();
        
        return queryTree;
    }
    
    protected ASTJexlScript processTree(final ASTJexlScript originalQueryTree, ShardQueryConfiguration config, Query settings, MetadataHelper metadataHelper,
                    ScannerFactory scannerFactory, QueryData queryData, QueryStopwatch timers, QueryModel queryModel) throws DatawaveQueryException {
        ASTJexlScript queryTree = originalQueryTree;
        // Find unfielded terms, and fully qualify them with an OR of all fields
        // found in the index
        // If the max term expansion is reached, then the original query tree is
        // returned.
        // If the max regex expansion is reached for a term, then it will be
        // left as a regex
        TraceStopwatch stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - Expand ANYFIELD regex nodes");
        Set<String> expansionFields = null;
        if (!disableAnyFieldLookup) {
            try {
                
                expansionFields = metadataHelper.getExpansionFields(config.getDatatypeFilter());
                queryTree = FixUnfieldedTermsVisitor.fixUnfieldedTree(config, scannerFactory, metadataHelper, queryTree, expansionFields);
            } catch (CannotExpandUnfieldedTermFatalException e) {
                // The visitor will only throw this if we cannot expand a term that is not nested in an or.
                // Hence this query would return no results.
                stopwatch.stop();
                NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.UNFIELDED_QUERY_ZERO_MATCHES, e, MessageFormat.format("Query: ",
                                queryData.getQuery()));
                log.info(qe);
                throw new NoResultsException(qe);
            } catch (InstantiationException | TableNotFoundException | IllegalAccessException e) {
                stopwatch.stop();
                QueryException qe = new QueryException(DatawaveErrorCode.METADATA_ACCESS_ERROR, e);
                log.info(qe);
                throw new DatawaveFatalQueryException(qe);
            }
            
            if (log.isDebugEnabled()) {
                logQuery(queryTree, "Query after fixing unfielded queries:");
            }
        }
        
        stopwatch.stop();
        if (!disableTestNonExistentFields) {
            stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - Test for non-existent fields");
            
            // Verify that the query does not contain fields we've never seen
            // before
            Set<String> nonexistentFields = FieldMissingFromSchemaVisitor.getNonExistentFields(metadataHelper, queryTree, config.getDatatypeFilter(),
                            Sets.newHashSet(QueryOptions.DEFAULT_DATATYPE_FIELDNAME, Constants.ANY_FIELD));// ,
                                                                                                           // "filter",
                                                                                                           // "countOf"));
            if (log.isDebugEnabled()) {
                log.debug("Testing for non-existent fields, found: " + nonexistentFields.size());
            }
            if (nonexistentFields.size() > 0) {
                String datatypeFilterSet = (null == config.getDatatypeFilter()) ? "none" : config.getDatatypeFilter().toString();
                if (log.isTraceEnabled()) {
                    try {
                        log.trace("current size of fields" + metadataHelper.getAllFields(config.getDatatypeFilter()));
                        log.trace("all fields: " + metadataHelper.getAllFields(config.getDatatypeFilter()).toString());
                    } catch (TableNotFoundException e) {
                        log.error("table not found when reading metadata", e);
                    }
                    log.trace("QueryModel:" + (null == queryModel ? "null" : queryModel));
                    log.trace("metadataHelper " + metadataHelper.toString());
                }
                log.trace("QueryModel:" + (null == queryModel ? "null" : queryModel));
                log.trace("metadataHelper " + metadataHelper.toString());
                
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.FIELDS_NOT_IN_DATA_DICTIONARY, MessageFormat.format(
                                "Datatype: {0}, datatype.filter.set: {1}, Auths: {2}", nonexistentFields, datatypeFilterSet, config.getAuthorizations()));
                log.error(qe);
                throw new InvalidQueryException(qe);
            }
            
            stopwatch.stop();
        }
        
        if (!disableExpandIndexFunction) {
            stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - Expand function index queries");
            
            // Coalesce any bounded ranges into separate AND subtrees
            queryTree = FunctionIndexQueryExpansionVisitor.expandFunctions(config, metadataHelper, dateIndexHelper, queryTree);
            if (log.isDebugEnabled()) {
                logQuery(queryTree, "Query after function index queries were expanded:");
            }
            
            stopwatch.stop();
        }
        
        if (!disableRangeCoalescing) {
            stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - Coalesce ranges");
            
            // Coalesce any bounded ranges into separate AND subtrees
            queryTree = RangeCoalescingVisitor.coalesceRanges(queryTree);
            
            if (log.isDebugEnabled()) {
                logQuery(queryTree, "Query after ranges were coalesced:");
            }
            
            stopwatch.stop();
            
        }
        
        Multimap<String,Type<?>> fieldToDatatypeMap = null;
        if (cacheDataTypes) {
            fieldToDatatypeMap = dataTypeMap.getIfPresent(String.valueOf(config.getDatatypeFilter().hashCode()));
            
        }
        stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - Fetch required dataTypes");
        try {
            if (null != fieldToDatatypeMap) {
                Set<String> indexedFields = Sets.newHashSet();
                Set<String> normalizedFields = Sets.newHashSet();
                
                loadDataTypeMetadata(fieldToDatatypeMap, indexedFields, normalizedFields, config, false);
                
                if (null == queryFieldsAsDataTypeMap) {
                    queryFieldsAsDataTypeMap = HashMultimap.create(Multimaps.filterKeys(fieldToDatatypeMap, input -> !cachedNormalizedFields.contains(input)));
                }
                
                if (null == normalizedFieldAsDataTypeMap) {
                    normalizedFieldAsDataTypeMap = HashMultimap
                                    .create(Multimaps.filterKeys(fieldToDatatypeMap, input -> cachedNormalizedFields.contains(input)));
                }
                
                setCachedFields(indexedFields, queryFieldsAsDataTypeMap, normalizedFieldAsDataTypeMap, config);
            } else {
                fieldToDatatypeMap = configureIndexedAndNormalizedFields(metadataHelper, config, queryTree);
                
                if (cacheDataTypes) {
                    loadDataTypeMetadata(null, null, null, config, true);
                    
                    dataTypeMap.put(String.valueOf(config.getDatatypeFilter().hashCode()), metadataHelper.getFieldsToDatatypes(config.getDatatypeFilter()));
                }
            }
            
        } catch (InstantiationException | IllegalAccessException | AccumuloException | AccumuloSecurityException | TableNotFoundException | ExecutionException e) {
            throw new DatawaveFatalQueryException(e);
        }
        
        stopwatch.stop();
        stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - Fix unindexed numerics");
        
        queryTree = FixUnindexedNumericTerms.fixNumerics(config, queryTree);
        
        if (log.isDebugEnabled()) {
            logQuery(queryTree, "Query after fixing unindexed numerics:");
        }
        
        stopwatch.stop();
        
        stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - Expand query from normalizers");
        
        queryTree = ExpandMultiNormalizedTerms.expandTerms(config, metadataHelper, queryTree);
        
        if (log.isDebugEnabled()) {
            logQuery(queryTree, "Query after normalizers were applied:");
        }
        
        stopwatch.stop();
        
        // if we have any index holes, then mark em
        if (!config.getIndexHoles().isEmpty()) {
            stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - Mark Index Holes");
            
            queryTree = PushdownMissingIndexRangeNodesVisitor.pushdownPredicates(queryTree, config, metadataHelper);
            if (log.isDebugEnabled()) {
                logQuery(queryTree, "Query after marking index holes:");
            }
            
            stopwatch.stop();
        }
        
        // lets precomputed the indexed fields and index only fields for the specific datatype if needed below
        Set<String> indexedFields = null;
        Set<String> indexOnlyFields = null;
        Set<String> nonEventFields = null;
        if (config.getMinSelectivity() > 0 || !disableBoundedLookup) {
            try {
                indexedFields = metadataHelper.getIndexedFields(config.getDatatypeFilter());
                indexOnlyFields = metadataHelper.getIndexOnlyFields(config.getDatatypeFilter());
                nonEventFields = metadataHelper.getNonEventFields(config.getDatatypeFilter());
            } catch (TableNotFoundException te) {
                QueryException qe = new QueryException(DatawaveErrorCode.METADATA_ACCESS_ERROR, te);
                throw new DatawaveFatalQueryException(qe);
            }
        }
        
        // push down terms that are over the min selectivity
        if (config.getMinSelectivity() > 0) {
            stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - Pushdown Low-Selective Terms");
            
            queryTree = PushdownLowSelectivityNodesVisitor.pushdownLowSelectiveTerms(queryTree, config, metadataHelper);
            if (log.isDebugEnabled()) {
                logQuery(queryTree, "Query after pushing down low-selective terms:");
            }
            
            List<String> debugOutput = null;
            if (log.isDebugEnabled()) {
                debugOutput = new ArrayList<>(32);
            }
            if (!ExecutableDeterminationVisitor.isExecutable(queryTree, config, indexedFields, indexOnlyFields, nonEventFields, debugOutput, metadataHelper)) {
                queryTree = (ASTJexlScript) PushdownUnexecutableNodesVisitor.pushdownPredicates(queryTree, config, indexedFields, indexOnlyFields,
                                nonEventFields, metadataHelper);
                if (log.isDebugEnabled()) {
                    logDebug(debugOutput, "Executable state after pushing low-selective terms:");
                    logQuery(queryTree, "Query after partially executable pushdown :");
                }
            }
            
            stopwatch.stop();
        }
        
        if (!disableCompositeFields) {
            stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - Expand composite terms");
            queryTree = ExpandCompositeTerms.expandTerms(config, metadataHelper, queryTree);
            stopwatch.stop();
        }
        
        if (!disableBoundedLookup) {
            stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - Expand bounded query ranges");
            
            // Expand any bounded ranges into a conjunction of discrete terms
            try {
                
                ParallelIndexExpansion regexExpansion = new ParallelIndexExpansion(config, scannerFactory, metadataHelper, expansionFields);
                queryTree = (ASTJexlScript) regexExpansion.visit(queryTree, null);
                queryTree = RangeConjunctionRebuildingVisitor.expandRanges(config, scannerFactory, metadataHelper, queryTree);
                queryTree = PushFunctionsIntoExceededValueRanges.pushFunctions(queryTree, metadataHelper, config.getDatatypeFilter());
                if (log.isDebugEnabled()) {
                    logQuery(queryTree, "Query after expanding regex:");
                }
                // if we now have an unexecutable tree because of delayed
                // predicates, then remove delayed predicates as needed and
                // reexpand
                List<String> debugOutput = null;
                if (log.isDebugEnabled()) {
                    debugOutput = new ArrayList<>(32);
                }
                
                // Unless config.isExandAllTerms is true, this may set some of
                // the terms to be delayed.
                if (!ExecutableDeterminationVisitor
                                .isExecutable(queryTree, config, indexedFields, indexOnlyFields, nonEventFields, debugOutput, metadataHelper)) {
                    queryTree = (ASTJexlScript) PullupUnexecutableNodesVisitor.pullupDelayedPredicates(queryTree, config, indexedFields, indexOnlyFields,
                                    nonEventFields, metadataHelper);
                    if (log.isDebugEnabled()) {
                        logDebug(debugOutput, "Executable state after expanding ranges:");
                        logQuery(queryTree, "Query after delayed pullup:");
                    }
                    
                    boolean expandAllTerms = config.isExpandAllTerms();
                    // set the expand all terms flag to avoid any more delayed
                    // predicates based on cost...
                    config.setExpandAllTerms(true);
                    
                    queryTree = (ASTJexlScript) regexExpansion.visit(queryTree, null);
                    queryTree = RangeConjunctionRebuildingVisitor.expandRanges(config, scannerFactory, metadataHelper, queryTree);
                    queryTree = PushFunctionsIntoExceededValueRanges.pushFunctions(queryTree, metadataHelper, config.getDatatypeFilter());
                    config.setExpandAllTerms(expandAllTerms);
                    if (log.isDebugEnabled()) {
                        logQuery(queryTree, "Query after expanding ranges and regex again:");
                    }
                }
                
                // if we now have an unexecutable tree because of missing
                // delayed predicates, then add delayed predicates where
                // possible
                if (log.isDebugEnabled()) {
                    debugOutput.clear();
                }
                if (!ExecutableDeterminationVisitor
                                .isExecutable(queryTree, config, indexedFields, indexOnlyFields, nonEventFields, debugOutput, metadataHelper)) {
                    queryTree = (ASTJexlScript) PushdownUnexecutableNodesVisitor.pushdownPredicates(queryTree, config, indexedFields, indexOnlyFields,
                                    nonEventFields, metadataHelper);
                    if (log.isDebugEnabled()) {
                        logDebug(debugOutput, "Executable state after expanding ranges and regex again:");
                        logQuery(queryTree, "Query after partially executable pushdown:");
                    }
                }
                
            } catch (TableNotFoundException | InstantiationException | IllegalAccessException e1) {
                stopwatch.stop();
                QueryException qe = new QueryException(DatawaveErrorCode.METADATA_ACCESS_ERROR, e1);
                throw new DatawaveFatalQueryException(qe);
            } catch (CannotExpandUnfieldedTermFatalException e) {
                if (null != e.getCause() && e.getCause() instanceof DoNotPerformOptimizedQueryException)
                    throw (DoNotPerformOptimizedQueryException) e.getCause();
            } catch (ExecutionException e) {
                log.error("Exception while expanding ranges", e);
            }
            stopwatch.stop();
        } else {
            
            if (log.isDebugEnabled()) {
                log.debug("Bounded range and regex conversion has been disabled");
            }
        }
        
        return queryTree;
    }
    
    /**
     * Load the metadata information.
     *
     * @param fieldToDatatypeMap
     *
     * @param indexedFields
     * @param normalizedFields
     * @param config
     * @param reload
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     * @throws TableNotFoundException
     * @throws ExecutionException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    private void loadDataTypeMetadata(Multimap<String,Type<?>> fieldToDatatypeMap, Set<String> indexedFields, Set<String> normalizedFields,
                    ShardQueryConfiguration config, boolean reload) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
                    ExecutionException, InstantiationException, IllegalAccessException {
        synchronized (dataTypeMap) {
            if (!reload && (null != cachedIndexedFields && null != indexedFields) && (null != cachedNormalizedFields && null != normalizedFields)) {
                indexedFields.addAll(cachedIndexedFields);
                normalizedFields.addAll(cachedNormalizedFields);
                
                return;
            }
            
            cachedIndexedFields = metadataHelper.getIndexedFields(null);
            cachedNormalizedFields = metadataHelper.getAllNormalized();
        }
    }
    
    // Overwrite projection and blacklist properties if the query model is
    // being used
    protected ASTJexlScript applyQueryModel(MetadataHelper metadataHelper, ShardQueryConfiguration config, TraceStopwatch stopwatch, ASTJexlScript queryTree,
                    QueryModel queryModel) {
        // generate the inverse of the reverse mapping; {display field name
        // => db field name}
        // a reverse mapping is always many to one, therefore the inverted
        // reverse mapping
        // can be one to many
        Multimap<String,String> inverseReverseModel = invertMultimap(queryModel.getReverseQueryMapping());
        
        inverseReverseModel.putAll(queryModel.getForwardQueryMapping());
        Collection<String> projectFields = config.getProjectFields(), blacklistedFields = config.getBlacklistedFields(), limitFields = config.getLimitFields(), groupFields = config
                        .getGroupFields(), uniqueFields = config.getUniqueFields();
        
        if (projectFields != null && !projectFields.isEmpty()) {
            projectFields = queryModel.remapParameter(projectFields, inverseReverseModel);
            if (log.isTraceEnabled()) {
                log.trace("Updated projection set using query model to: " + projectFields);
            }
            config.setProjectFields(Sets.newHashSet(projectFields));
        }
        
        if (groupFields != null && !groupFields.isEmpty()) {
            Collection<String> remappedGroupFields = queryModel.remapParameter(groupFields, inverseReverseModel);
            if (log.isTraceEnabled()) {
                log.trace("Updated grouping set using query model to: " + remappedGroupFields);
            }
            config.setGroupFields(Sets.newHashSet(remappedGroupFields));
            // if grouping is set, also set the projection to be the same
            config.setProjectFields(Sets.newHashSet(remappedGroupFields));
        }
        
        if (uniqueFields != null && !uniqueFields.isEmpty()) {
            Collection<String> remappedUniqueFields = queryModel.remapParameter(uniqueFields, inverseReverseModel);
            if (log.isTraceEnabled()) {
                log.trace("Updated unique set using query model to: " + remappedUniqueFields);
            }
            config.setUniqueFields(Sets.newHashSet(remappedUniqueFields));
        }
        
        if (config.getBlacklistedFields() != null && !config.getBlacklistedFields().isEmpty()) {
            blacklistedFields = queryModel.remapParameter(blacklistedFields, inverseReverseModel);
            if (log.isTraceEnabled()) {
                log.trace("Updated blacklist set using query model to: " + blacklistedFields);
            }
            config.setBlacklistedFields(Sets.newHashSet(blacklistedFields));
        }
        
        if (config.getLimitFields() != null && !config.getLimitFields().isEmpty()) {
            limitFields = queryModel.remapParameterEquation(limitFields, inverseReverseModel);
            if (log.isTraceEnabled()) {
                log.trace("Updated limitFields set using query model to: " + limitFields);
            }
            config.setLimitFields(Sets.newHashSet(limitFields));
        }
        
        Set<String> dataTypes = config.getDatatypeFilter();
        Set<String> allFields = null;
        try {
            String dataTypeHash = String.valueOf(dataTypes.hashCode());
            if (cacheDataTypes) {
                allFields = allFieldTypeMap.getIfPresent(dataTypeHash);
            }
            if (null == allFields) {
                allFields = metadataHelper.getAllFields(dataTypes);
                if (cacheDataTypes)
                    allFieldTypeMap.put(dataTypeHash, allFields);
            }
            
            if (log.isTraceEnabled()) {
                StringBuilder builder = new StringBuilder();
                for (String dataType : dataTypes) {
                    if (builder.length() > 0) {
                        builder.append(',');
                    }
                    builder.append(dataType);
                }
                log.trace("Datatypes: " + builder.toString());
                builder.delete(0, builder.length());
                
                for (String field : allFields) {
                    if (builder.length() > 0) {
                        builder.append(',');
                    }
                    builder.append(field);
                }
                log.trace("allFields: " + builder.toString());
            }
        } catch (TableNotFoundException e) {
            stopwatch.stop();
            QueryException qe = new QueryException(DatawaveErrorCode.FIELD_FETCH_ERROR, e);
            log.error(qe);
            throw new DatawaveFatalQueryException(qe);
        }
        queryTree = QueryModelVisitor.applyModel(queryTree, queryModel, allFields);
        if (log.isTraceEnabled())
            log.trace("queryTree:" + PrintingVisitor.formattedQueryString(queryTree));
        return queryTree;
    }
    
    /**
     * this is method-injected in QueryLogicFactory.xml to provide a new prototype bean This method's implementation should never be called in production
     *
     * @return
     */
    public QueryModelProvider.Factory getQueryModelProviderFactory() {
        return queryModelProviderFactory;
    }
    
    public void setQueryModelProviderFactory(QueryModelProvider.Factory queryModelProviderFactory) {
        this.queryModelProviderFactory = queryModelProviderFactory;
    }
    
    protected ASTJexlScript parseQueryAndValidatePattern(String query, TraceStopwatch stopwatch) {
        ASTJexlScript queryTree;
        try {
            queryTree = JexlASTHelper.parseJexlQuery(query);
            ValidPatternVisitor.check(queryTree);
        } catch (StackOverflowError soe) {
            if (log.isTraceEnabled()) {
                log.trace("Stack trace for overflow " + soe);
            }
            stopwatch.stop();
            PreConditionFailedQueryException qe = new PreConditionFailedQueryException(DatawaveErrorCode.QUERY_DEPTH_OR_TERM_THRESHOLD_EXCEEDED, soe);
            log.warn(qe);
            throw new DatawaveFatalQueryException(qe);
        } catch (ParseException e) {
            stopwatch.stop();
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.UNPARSEABLE_JEXL_QUERY, e, MessageFormat.format("Query: {0}", query));
            log.warn(qe);
            throw new DatawaveFatalQueryException(qe);
        } catch (PatternSyntaxException e) {
            stopwatch.stop();
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_REGEX, e, MessageFormat.format("Query: {0}", query));
            log.warn(qe);
            throw new DatawaveFatalQueryException(qe);
        }
        return queryTree;
    }
    
    public static void logDebug(List<String> output, String message) {
        if (log.isDebugEnabled()) {
            log.debug(message);
            for (String line : output) {
                log.debug(line);
            }
            log.debug("");
        }
    }
    
    public static void logQuery(ASTJexlScript queryTree, String message) {
        logDebug(PrintingVisitor.formattedQueryStringList(queryTree), message);
    }
    
    /**
     * Adding date filters if the query parameters specify that the dates are to be other than the default
     *
     * @param queryTree
     * @param scannerFactory
     * @param metadataHelper
     * @param config
     * @return the updated query tree
     * @throws TableNotFoundException
     */
    public ASTJexlScript addDateFilters(ASTJexlScript queryTree, ScannerFactory scannerFactory, MetadataHelper metadataHelper, DateIndexHelper dateIndexHelper,
                    ShardQueryConfiguration config, Query settings) throws TableNotFoundException, DatawaveQueryException {
        String defaultDateType = config.getDefaultDateTypeName();
        String dateType = defaultDateType;
        Parameter dateTypeParameter = settings.findParameter(QueryParameters.DATE_RANGE_TYPE);
        if (dateTypeParameter != null && dateTypeParameter.getParameterValue() != null) {
            String parm = dateTypeParameter.getParameterValue().trim();
            if (!parm.isEmpty()) {
                dateType = parm.toUpperCase();
            }
        }
        
        // if we are using something other than the default of EVENT date
        // time, then we need to modify the query
        if (!dateType.equals(defaultDateType)) {
            
            log.info("Using the date index for " + dateType);
            // if no date index helper configured, then we are in error
            if (dateIndexHelper == null) {
                throw new DatawaveQueryException("Requested date range of type " + dateType + " but no date index is configured");
            }
            // get all of the fields used for this date type
            DateIndexHelper.DateTypeDescription dateIndexData = dateIndexHelper.getTypeDescription(dateType, config.getBeginDate(), config.getEndDate(),
                            config.getDatatypeFilter());
            if (dateIndexData.getFields().isEmpty()) {
                throw new IllegalArgumentException("The specified date type: " + dateType + " is unknown for the specified data types");
            }
            log.info("Adding date filters for the following fields: " + dateIndexData.getFields());
            // now for each field, add an expression to filter that date
            List<JexlNode> andChildren = new ArrayList<>();
            for (int i = 0; i < queryTree.jjtGetNumChildren(); i++) {
                andChildren.add(JexlNodeFactory.createExpression(queryTree.jjtGetChild(i)));
            }
            List<JexlNode> orChildren = new ArrayList<>();
            for (String field : dateIndexData.getFields()) {
                orChildren.add(createDateFilter(dateType, field, config.getBeginDate(), config.getEndDate()));
            }
            if (orChildren.size() > 1) {
                andChildren.add(JexlNodeFactory.createOrNode(orChildren));
            } else {
                andChildren.addAll(orChildren);
            }
            JexlNode andNode = JexlNodeFactory.createAndNode(andChildren);
            JexlNodeFactory.setChildren(queryTree, Collections.singleton(andNode));
            
            // now lets update the query parameters with the correct start and
            // end dates
            log.info("Remapped " + dateType + " dates [" + config.getBeginDate() + "," + config.getEndDate() + "] to EVENT dates "
                            + dateIndexData.getBeginDate() + "," + dateIndexData.getEndDate());
            
            // reset the dates in the configuration, no need to reset then in
            // the Query settings object
            config.setBeginDate(dateIndexData.getBeginDate());
            config.setEndDate(dateIndexData.getEndDate());
        } else {
            log.info("Date index not needed for this query");
        }
        
        return queryTree;
    }
    
    /**
     * If configured, cap the start of the date range. If configured, throw an exception if the start AND end dates are outside the valid date range.
     *
     * @param config
     */
    protected void capDateRange(ShardQueryConfiguration config) throws DatawaveQueryException {
        if (config.getBeginDateCap() > 0) {
            long minStartTime = System.currentTimeMillis() - config.getBeginDateCap();
            if (config.getBeginDate().getTime() < minStartTime) {
                if (config.isFailOutsideValidDateRange() && config.getEndDate().getTime() < minStartTime) {
                    throw new DatawaveQueryException("This requested date range is outside of range of data on this system");
                } else {
                    config.setBeginDate(new Date(minStartTime));
                    log.info("Resetting begin date to the beginDateCap: " + config.getBeginDate());
                    if (config.getEndDate().getTime() < minStartTime) {
                        // setting the end date to the same as the begin date will result in no ranges being created (@see
                        // GenericQueryConfiguration.canRunQuery())
                        config.setEndDate(new Date(minStartTime - 1));
                        log.info("Resetting end date to the beginDateCap: " + config.getEndDate());
                    }
                }
            }
        }
    }
    
    /**
     * Create a date filter function node:
     *
     * @param dateType
     * @param field
     * @param begin
     * @param end
     * @return
     */
    protected JexlNode createDateFilter(String dateType, String field, Date begin, Date end) {
        String filterNameSpace = EvaluationPhaseFilterFunctions.EVAL_PHASE_FUNCTION_NAMESPACE;
        String filterFunction = (DateIndexUtil.LOADED_DATE_TYPE.equals(dateType) ? "betweenLoadDates" : "betweenDates");
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd:HH:mm:ss:SSSZ");
        return JexlNodeFactory.buildFunctionNode(filterNameSpace, filterFunction, field, format.format(begin), format.format(end));
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see PushDownPlanner#rewriteQuery( org.apache .commons.jexl2.parser.ASTJexlScript)
     */
    @Override
    public ASTJexlScript applyRules(ASTJexlScript queryTree, ScannerFactory scannerFactory, MetadataHelper metadataHelper, ShardQueryConfiguration config) {
        
        // The PushDownVisitor will decide what nodes are "delayed" in that they
        // do NOT
        // require a global index lookup due to cost or other reasons as defined
        // by the configured rules.
        PushDownVisitor pushDownPlanner = new PushDownVisitor(config, scannerFactory, metadataHelper, rules);
        
        return pushDownPlanner.applyRules(queryTree);
    }
    
    /**
     * Get the list of alternatives, randomizing the order so that the tserver spread out the disk usage.
     */
    protected String getIvaratorQueryCacheBaseUriAlternatives(ShardQueryConfiguration config) {
        StringBuilder baseUriAlternatives = new StringBuilder();
        List<String> alternatives = new LinkedList<>(config.getIvaratorCacheBaseURIsAsList());
        Random random = new Random();
        while (!alternatives.isEmpty()) {
            if (baseUriAlternatives.length() > 0) {
                baseUriAlternatives.append(',');
            }
            baseUriAlternatives.append(alternatives.remove(random.nextInt(alternatives.size())));
            
        }
        return baseUriAlternatives.toString();
    }
    
    /**
     * Extend to further configure QueryIterator
     *
     * @param config
     * @param cfg
     */
    protected void configureAdditionalOptions(ShardQueryConfiguration config, IteratorSetting cfg) {
        // no-op
    }
    
    protected Future<IteratorSetting> loadQueryIterator(final MetadataHelper metadataHelper, final ShardQueryConfiguration config, final Query settings,
                    final String queryString, final Boolean isFullTable) throws DatawaveQueryException {
        
        return builderThread
                        .submit(() -> {
                            // VersioningIterator is typically set at 20 on the table
                            IteratorSetting cfg = new IteratorSetting(config.getBaseIteratorPriority() + 40, "query", getQueryIteratorClass());
                            
                            addOption(cfg, Constants.RETURN_TYPE, config.getReturnType().toString(), false);
                            addOption(cfg, QueryOptions.FULL_TABLE_SCAN_ONLY, Boolean.toString(isFullTable), false);
                            
                            if (sourceLimit > 0) {
                                addOption(cfg, QueryOptions.LIMIT_SOURCES, Long.toString(sourceLimit), false);
                            }
                            if (config.getCollectTimingDetails()) {
                                addOption(cfg, QueryOptions.COLLECT_TIMING_DETAILS, Boolean.toString(true), false);
                            }
                            if (config.getSendTimingToStatsd()) {
                                addOption(cfg, QueryOptions.STATSD_HOST_COLON_PORT, config.getStatsdHost() + ':' + Integer.toString(config.getStatsdPort()),
                                                false);
                                addOption(cfg, QueryOptions.STATSD_MAX_QUEUE_SIZE, Integer.toString(config.getStatsdMaxQueueSize()), false);
                            }
                            if (config.getHdfsSiteConfigURLs() != null) {
                                addOption(cfg, QueryOptions.HDFS_SITE_CONFIG_URLS, config.getHdfsSiteConfigURLs(), false);
                            }
                            if (config.getHdfsFileCompressionCodec() != null) {
                                addOption(cfg, QueryOptions.HDFS_FILE_COMPRESSION_CODEC, config.getHdfsFileCompressionCodec(), false);
                            }
                            if (config.getZookeeperConfig() != null) {
                                addOption(cfg, QueryOptions.ZOOKEEPER_CONFIG, config.getZookeeperConfig(), false);
                            }
                            if (config.getIvaratorCacheBaseURIs() != null) {
                                addOption(cfg, QueryOptions.IVARATOR_CACHE_BASE_URI_ALTERNATIVES, getIvaratorQueryCacheBaseUriAlternatives(config), false);
                            }
                            addOption(cfg, QueryOptions.IVARATOR_CACHE_BUFFER_SIZE, Integer.toString(config.getIvaratorCacheBufferSize()), false);
                            addOption(cfg, QueryOptions.IVARATOR_SCAN_PERSIST_THRESHOLD, Long.toString(config.getIvaratorCacheScanPersistThreshold()), false);
                            addOption(cfg, QueryOptions.IVARATOR_SCAN_TIMEOUT, Long.toString(config.getIvaratorCacheScanTimeout()), false);
                            addOption(cfg, QueryOptions.COLLECT_TIMING_DETAILS, Boolean.toString(config.getCollectTimingDetails()), false);
                            addOption(cfg, QueryOptions.MAX_INDEX_RANGE_SPLIT, Integer.toString(config.getMaxFieldIndexRangeSplit()), false);
                            addOption(cfg, QueryOptions.MAX_IVARATOR_OPEN_FILES, Integer.toString(config.getIvaratorMaxOpenFiles()), false);
                            addOption(cfg, QueryOptions.MAX_EVALUATION_PIPELINES, Integer.toString(config.getMaxEvaluationPipelines()), false);
                            addOption(cfg, QueryOptions.MAX_PIPELINE_CACHED_RESULTS, Integer.toString(config.getMaxPipelineCachedResults()), false);
                            addOption(cfg, QueryOptions.MAX_IVARATOR_SOURCES, Integer.toString(config.getMaxIvaratorSources()), false);
                            
                            if (config.getYieldThresholdMs() != Long.MAX_VALUE && config.getYieldThresholdMs() > 0) {
                                addOption(cfg, QueryOptions.YIELD_THRESHOLD_MS, Long.toString(config.getYieldThresholdMs()), false);
                            }
                            
                            addOption(cfg, QueryOptions.SORTED_UIDS, Boolean.toString(config.isSortedUIDs()), false);
                            
                            configureTypeMappings(config, cfg, metadataHelper, compressMappings);
                            configureAdditionalOptions(config, cfg);
                            
                            try {
                                addOption(cfg, QueryOptions.INDEX_ONLY_FIELDS,
                                                QueryOptions.buildIndexOnlyFieldsString(metadataHelper.getIndexOnlyFields(config.getDatatypeFilter())), true);
                                addOption(cfg, QueryOptions.COMPOSITE_FIELDS, QueryOptions.buildIndexOnlyFieldsString(metadataHelper.getCompositeToFieldMap(
                                                config.getDatatypeFilter()).keySet()), true);
                            } catch (TableNotFoundException e) {
                                QueryException qe = new QueryException(DatawaveErrorCode.INDEX_ONLY_FIELDS_RETRIEVAL_ERROR, e);
                                throw new DatawaveQueryException(qe);
                            }
                            
                            try {
                                CompositeMetadata compositeMetadata = metadataHelper.getCompositeMetadata().filter(config.getQueryFieldsDatatypes().keySet());
                                if (compositeMetadata != null && !compositeMetadata.isEmpty())
                                    addOption(cfg, QueryOptions.COMPOSITE_METADATA,
                                                    java.util.Base64.getEncoder().encodeToString(CompositeMetadata.toBytes(compositeMetadata)), false);
                            } catch (TableNotFoundException e) {
                                QueryException qe = new QueryException(DatawaveErrorCode.COMPOSITE_METADATA_CONFIG_ERROR, e);
                                throw new DatawaveQueryException(qe);
                            }
                            
                            String datatypeFilter = config.getDatatypeFilterAsString();
                            
                            addOption(cfg, QueryOptions.DATATYPE_FILTER, datatypeFilter, false);
                            
                            try {
                                addOption(cfg, QueryOptions.CONTENT_EXPANSION_FIELDS,
                                                Joiner.on(',').join(metadataHelper.getContentFields(config.getDatatypeFilter())), false);
                            } catch (TableNotFoundException e) {
                                QueryException qe = new QueryException(DatawaveErrorCode.CONTENT_FIELDS_RETRIEVAL_ERROR, e);
                                throw new DatawaveQueryException(qe);
                            }
                            
                            if (config.isDebugMultithreadedSources()) {
                                addOption(cfg, QueryOptions.DEBUG_MULTITHREADED_SOURCES, Boolean.toString(config.isDebugMultithreadedSources()), false);
                            }
                            
                            if (config.isDataQueryExpressionFilterEnabled()) {
                                addOption(cfg, QueryOptions.DATA_QUERY_EXPRESSION_FILTER_ENABLED,
                                                Boolean.toString(config.isDataQueryExpressionFilterEnabled()), false);
                            }
                            
                            if (config.isLimitFieldsPreQueryEvaluation()) {
                                addOption(cfg, QueryOptions.LIMIT_FIELDS_PRE_QUERY_EVALUATION, Boolean.toString(config.isLimitFieldsPreQueryEvaluation()),
                                                false);
                            }
                            
                            if (config.getLimitFieldsField() != null) {
                                addOption(cfg, QueryOptions.LIMIT_FIELDS_FIELD, config.getLimitFieldsField(), false);
                            }
                            
                            return cfg;
                        });
    }
    
    protected IteratorSetting getQueryIterator(MetadataHelper metadataHelper, ShardQueryConfiguration config, Query settings, String queryString,
                    Boolean isFullTable) throws DatawaveQueryException {
        if (null == settingFuture)
            settingFuture = loadQueryIterator(metadataHelper, config, settings, queryString, isFullTable);
        if (settingFuture.isDone())
            try {
                return settingFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e.getCause());
            }
        else
            return null;
    }
    
    public static void configureTypeMappings(ShardQueryConfiguration config, IteratorSetting cfg, MetadataHelper metadataHelper, boolean compressMappings)
                    throws DatawaveQueryException {
        try {
            addOption(cfg, QueryOptions.QUERY_MAPPING_COMPRESS, Boolean.valueOf(compressMappings).toString(), false);
            
            // now lets filter the query field datatypes to those that are not
            // indexed
            Multimap<String,Type<?>> nonIndexedQueryFieldsDatatypes = HashMultimap.create(config.getQueryFieldsDatatypes());
            nonIndexedQueryFieldsDatatypes.keySet().removeAll(config.getIndexedFields());
            
            String nonIndexedTypes = QueryOptions.buildFieldNormalizerString(nonIndexedQueryFieldsDatatypes);
            String typeMetadataString = metadataHelper.getTypeMetadata(config.getDatatypeFilter()).toString();
            String requiredAuthsString = metadataHelper.getUsersMetadataAuthorizationSubset();
            
            if (compressMappings) {
                nonIndexedTypes = QueryOptions.compressOption(nonIndexedTypes, QueryOptions.UTF8);
                typeMetadataString = QueryOptions.compressOption(typeMetadataString, QueryOptions.UTF8);
                requiredAuthsString = QueryOptions.compressOption(requiredAuthsString, QueryOptions.UTF8);
            }
            addOption(cfg, QueryOptions.NON_INDEXED_DATATYPES, nonIndexedTypes, false);
            if (config.isTypeMetadataInHdfs() == false) {
                addOption(cfg, QueryOptions.TYPE_METADATA, typeMetadataString, false);
            }
            addOption(cfg, QueryOptions.TYPE_METADATA_AUTHS, requiredAuthsString, false);
            
            addOption(cfg, QueryOptions.METADATA_TABLE_NAME, config.getMetadataTableName(), false);
            
        } catch (TableNotFoundException | IOException e) {
            QueryException qe = new QueryException(DatawaveErrorCode.TYPE_MAPPING_CONFIG_ERROR, e);
            throw new DatawaveQueryException(qe);
        }
        
    }
    
    public static void addOption(IteratorSetting cfg, String option, String value, boolean allowBlankValue) {
        if (StringUtils.isNotBlank(option) && (allowBlankValue || StringUtils.isNotBlank(value))) {
            // If blank value, then we need to change it to something else or it
            // will fail in InputFormatBase when run
            // through the MapReduce api.
            if (StringUtils.isBlank(value) && allowBlankValue) {
                value = " ";
            }
            cfg.addOption(option, value);
        }
    }
    
    /**
     * Load the common iterator options for both the optimized and non-optimized query paths. Said options include: enrichers, filters (post-processing and
     * index), unevaluatedExpressions, begin/end datetimes, indexed fields and their normalizers, non-event key column families, and the query string
     *
     * @param config
     * @param cfg
     * @throws DatawaveQueryException
     */
    protected void setCommonIteratorOptions(ShardQueryConfiguration config, IteratorSetting cfg) throws DatawaveQueryException {
        // Applying filtering options, including classnames, whether applied to
        // post-processing or field index
        if (config.getUseFilters()) {
            // Initialize the flag to add options
            boolean addOptions = false;
            
            // Get any configured post-processing filter classes
            final List<String> postProcessingFilterClasses = config.getFilterClassNames();
            if ((null != postProcessingFilterClasses) && !postProcessingFilterClasses.isEmpty()) {
                addOption(cfg, QueryOptions.POSTPROCESSING_CLASSES, StringUtils.join(postProcessingFilterClasses, ','), false);
                addOptions = true;
            }
            
            // Get any configured index-filtering classes
            final List<String> indexFilteringClasses = config.getIndexFilteringClassNames();
            if ((null != indexFilteringClasses) && !indexFilteringClasses.isEmpty()) {
                addOption(cfg, IndexIterator.INDEX_FILTERING_CLASSES, StringUtils.join(indexFilteringClasses, ','), false);
                addOptions = true;
            }
            
            // Add options to iterator configuration
            final Map<String,String> filterOptions = config.getFilterOptions();
            if (addOptions && !filterOptions.isEmpty()) {
                for (Entry<String,String> optionValue : filterOptions.entrySet()) {
                    addOption(cfg, optionValue.getKey(), optionValue.getValue(), false);
                }
            }
        }
        
        // Whitelist and blacklist projection are mutually exclusive. You can't
        // have both.
        if (null != config.getProjectFields() && !config.getProjectFields().isEmpty()) {
            if (log.isDebugEnabled()) {
                final int maxLen = 100;
                String projectFields = config.getProjectFieldsAsString();
                if (projectFields.length() > maxLen) {
                    projectFields = projectFields.substring(0, maxLen) + "[TRUNCATED]";
                }
                log.debug("Setting scan option: " + QueryOptions.PROJECTION_FIELDS + " to " + projectFields);
            }
            
            addOption(cfg, QueryOptions.PROJECTION_FIELDS, config.getProjectFieldsAsString(), false);
        } else if (null != config.getBlacklistedFields() && !config.getBlacklistedFields().isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("Setting scan option: " + QueryOptions.BLACKLISTED_FIELDS + " to " + config.getBlacklistedFieldsAsString());
            }
            
            addOption(cfg, QueryOptions.BLACKLISTED_FIELDS, config.getBlacklistedFieldsAsString(), false);
        }
        
        // We don't need to do any expansion of the start or end date/time
        // because the webservice is handling
        // the HHmmss components and handing us proper Date objects.
        if (log.isDebugEnabled()) {
            log.debug("beginDate: " + config.getBeginDate().getTime());
            log.debug("endDate:   " + config.getEndDate().getTime());
        }
        
        // Set the start and end dates
        addOption(cfg, QueryOptions.START_TIME, Long.toString(config.getBeginDate().getTime()), false);
        addOption(cfg, QueryOptions.END_TIME, Long.toString(config.getEndDate().getTime()), false);
        
        // Set the list of nonEventKeyPrefixes
        if (null != config.getNonEventKeyPrefixes() && !config.getNonEventKeyPrefixes().isEmpty()) {
            addOption(cfg, QueryOptions.IGNORE_COLUMN_FAMILIES, QueryOptions.buildIgnoredColumnFamiliesString(Sets.newHashSet("d", "tf")), false);
        }
        
        // Include the option to filter masked values
        addOption(cfg, QueryOptions.FILTER_MASKED_VALUES, Boolean.toString(config.getFilterMaskedValues()), false);
        
        // Include the EVENT_DATATYPE as a field
        if (config.getIncludeDataTypeAsField()) {
            addOption(cfg, QueryOptions.INCLUDE_DATATYPE, Boolean.toString(true), false);
        }
        
        // Include the RECORD_ID as a field
        if (!config.getIncludeRecordId()) {
            addOption(cfg, QueryOptions.INCLUDE_RECORD_ID, Boolean.toString(false), false);
        }
        
        // Conditionally include CHILD_COUNT, DESCENDANT_COUNT, HAS_CHILDREN
        // and/or PARENT_UID fields, plus
        // various options for output and optimization
        if (config.getIncludeHierarchyFields()) {
            addOption(cfg, QueryOptions.INCLUDE_HIERARCHY_FIELDS, Boolean.toString(true), false);
            final Map<String,String> options = config.getHierarchyFieldOptions();
            if (null != options) {
                for (final Entry<String,String> entry : config.getHierarchyFieldOptions().entrySet()) {
                    final String key = entry.getKey();
                    if (null != key) {
                        addOption(cfg, key, entry.getValue(), false);
                    }
                }
            }
        }
        
        // Include the document permutations
        if (!config.getDocumentPermutations().isEmpty()) {
            StringBuilder docPermutationConfig = new StringBuilder();
            String sep = "";
            for (String perm : config.getDocumentPermutations()) {
                docPermutationConfig.append(sep).append(perm);
                sep = ",";
            }
            addOption(cfg, QueryOptions.DOCUMENT_PERMUTATION_CLASSES, docPermutationConfig.toString(), false);
        }
        
        addOption(cfg, QueryOptions.REDUCED_RESPONSE, Boolean.toString(config.isReducedResponse()), false);
        addOption(cfg, QueryOptions.DISABLE_EVALUATION, Boolean.toString(config.isDisableEvaluation()), false);
        addOption(cfg, QueryOptions.DISABLE_DOCUMENTS_WITHOUT_EVENTS, Boolean.toString(config.isDisableIndexOnlyDocuments()), false);
        addOption(cfg, QueryOptions.INCLUDE_GROUPING_CONTEXT, Boolean.toString(config.getIncludeGroupingContext()), false);
        addOption(cfg, QueryOptions.CONTAINS_INDEX_ONLY_TERMS, Boolean.toString(config.isContainsIndexOnlyTerms()), false);
        addOption(cfg, QueryOptions.CONTAINS_COMPOSITE_TERMS, Boolean.toString(config.isContainsCompositeTerms()), false);
        addOption(cfg, QueryOptions.ALLOW_FIELD_INDEX_EVALUATION, Boolean.toString(config.isAllowFieldIndexEvaluation()), false);
        addOption(cfg, QueryOptions.ALLOW_TERM_FREQUENCY_LOOKUP, Boolean.toString(config.isAllowTermFrequencyLookup()), false);
        addOption(cfg, QueryOptions.COMPRESS_SERVER_SIDE_RESULTS, Boolean.toString(config.isCompressServerSideResults()), false);
    }
    
    /**
     * Performs a lookup in the global index / reverse index and returns a RangeCalculator
     *
     * @param config
     * @param queryTree
     * @return range calculator
     */
    protected CloseableIterable<QueryPlan> getFullScanRange(ShardQueryConfiguration config, JexlNode queryTree) {
        if (log.isTraceEnabled()) {
            log.trace("Building full scan range ");
            PrintingVisitor.printQuery(queryTree);
        }
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        cal.setTime(config.getBeginDate());
        
        // Truncate the time on the startKey
        String startKey = config.getShardDateFormatter().format(DateUtils.truncate(cal, Calendar.DAY_OF_MONTH).getTime());
        
        // Truncate and bump the time on the endKey
        String endKey = config.getShardDateFormatter().format(getEndDateForIndexLookup(config.getEndDate()));
        
        Range r = new Range(startKey, true, endKey, false);
        
        if (log.isTraceEnabled()) {
            log.trace("Produced range is " + r);
        }
        
        return new CloseableListIterable<QueryPlan>(Collections.singletonList(new QueryPlan(queryTree, r)));
    }
    
    /**
     * Returns a Tuple2&lt;Iterable&lt;Range&gt;,Boolean&gt; whose elements represent the Ranges to use for querying the shard table and whether or not this is
     * a "full-table-scan" query.
     *
     * @param scannerFactory
     * @param metadataHelper
     * @param config
     * @param queryTree
     * @return
     * @throws DatawaveQueryException
     */
    public Tuple2<CloseableIterable<QueryPlan>,Boolean> getQueryRanges(ScannerFactory scannerFactory, MetadataHelper metadataHelper,
                    ShardQueryConfiguration config, JexlNode queryTree) throws DatawaveQueryException {
        Preconditions.checkNotNull(queryTree);
        
        boolean needsFullTable = false;
        CloseableIterable<QueryPlan> ranges = null;
        
        // if we still have an unexecutable tree, then a full table scan is
        // required
        List<String> debugOutput = null;
        if (log.isDebugEnabled()) {
            debugOutput = new ArrayList<>(32);
        }
        STATE state = ExecutableDeterminationVisitor.getState(queryTree, config, metadataHelper, debugOutput);
        if (log.isDebugEnabled()) {
            logDebug(debugOutput, "ExecutableDeterminationVisitor at getQueryRanges:");
        }
        if (state != STATE.EXECUTABLE) {
            if (state == STATE.ERROR) {
                log.warn("After expanding the query, it is determined that the query cannot be executed due to index-only fields mixed with expressions that cannot be run against the index.");
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INDEX_ONLY_FIELDS_MIXED_INVALID_EXPRESSIONS);
                throw new InvalidQueryException(qe);
            }
            log.warn("After expanding the query, it is determined that the query cannot be executed against the field index and a full table scan is required");
            needsFullTable = true;
        }
        
        // if a simple examination of the query has not forced a full table
        // scan, then lets try to compute ranges
        if (!needsFullTable) {
            
            // count the terms
            int termCount = TermCountingVisitor.countTerms(queryTree);
            if (termCount >= pushdownThreshold) {
                if (log.isTraceEnabled()) {
                    log.trace("pushing down query because it has " + termCount + " when our max is " + pushdownThreshold);
                }
                config.setCollapseUids(true);
            }
            TraceStopwatch stopwatch = config.getTimers().newStartedStopwatch("DefaultQueryPlanner - Begin stream of ranges from inverted index");
            
            RangeStream stream = initializeRangeStream(config, scannerFactory, metadataHelper);
            
            stream.setCondenseUids(condenseUidsInRangeStream);
            
            stream.setCompressUids(compressUidsInRangeStream);
            
            ranges = stream.streamPlans(queryTree);
            
            if (log.isTraceEnabled()) {
                log.trace("query stream is " + stream.context());
            }
            
            // if a term threshold is exceeded and we cannot handle that, then
            // throw unsupported
            boolean thresholdExceeded = StreamContext.EXCEEDED_TERM_THRESHOLD.equals(stream.context());
            if (thresholdExceeded && !config.canHandleExceededTermThreshold()) {
                throw new UnsupportedOperationException(EXCEED_TERM_EXPANSION_ERROR);
            }
            
            if (StreamContext.UNINDEXED.equals(stream.context())) {
                log.debug("Needs full table scan because of unindexed fields");
                needsFullTable = true;
            } else if (StreamContext.DELAYED_FIELD.equals(stream.context())) {
                log.debug("Needs full table scan because query consists of only delayed expressions");
                needsFullTable = true;
            }
            // if a value threshold is exceeded and we cannot handle that, then
            // force a full table scan
            else if (StreamContext.EXCEEDED_VALUE_THRESHOLD.equals(stream.context()) && !config.canHandleExceededValueThreshold()) {
                log.debug("Needs full table scan because we exceeded the value threshold and config.canHandleExceededValueThreshold() is false");
                needsFullTable = true;
            }
            
            stopwatch.stop();
        }
        if (needsFullTable) {
            if (config.getFullTableScanEnabled()) {
                ranges = this.getFullScanRange(config, queryTree);
            } else {
                if (log.isTraceEnabled())
                    log.trace("Full table scans are not enabled, query will not be run");
                QueryException qe = new QueryException(DatawaveErrorCode.FULL_TABLE_SCAN_REQUIRED_BUT_DISABLED);
                throw new FullTableScansDisallowedException(qe);
            }
            if (log.isTraceEnabled())
                log.trace("Ranges are " + ranges);
        }
        
        return new Tuple2<CloseableIterable<QueryPlan>,Boolean>(ranges, needsFullTable);
    }
    
    /**
     * Initializes the range stream, whether it is configured to be a different class than the Default Range stream or not.
     *
     * @param config
     * @param scannerFactory
     * @param metadataHelper
     * @return
     */
    private RangeStream initializeRangeStream(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelper metadataHelper) {
        Class<? extends RangeStream> rstream;
        try {
            rstream = Class.forName(rangeStreamClass).asSubclass(RangeStream.class);
            
            RangeStream stream = rstream.getConstructor(ShardQueryConfiguration.class, ScannerFactory.class, MetadataHelper.class).newInstance(config,
                            scannerFactory, metadataHelper);
            
            return stream.setUidIntersector(uidIntersector).setLimitScanners(limitScanners).setCreateUidsIteratorClass(createUidsIteratorClass);
            
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
                        | NoSuchMethodException | SecurityException e) {
            throw new RuntimeException(e);
        }
    }
    
    public boolean isLimitScanners() {
        return limitScanners;
    }
    
    public final void setLimitScanners(boolean limitScanners) {
        this.limitScanners = limitScanners;
    }
    
    public void setSourceLimit(long sourcesPerScan) {
        this.sourceLimit = sourcesPerScan;
    }
    
    public long getSourceLimit() {
        return sourceLimit;
    }
    
    /**
     * Allows us to disable the bounded range and regex conversion. If you only have discrete terms, this is an unncessary step. Provisions have been put into
     * place to allow this to be automated in a future release.
     *
     * @param disableBoundedLookup
     */
    public final void setDisableBoundedLookup(boolean disableBoundedLookup) {
        this.disableBoundedLookup = disableBoundedLookup;
    }
    
    public final void setDisableAnyFieldLookup(boolean disableAnyFieldLookup) {
        this.disableAnyFieldLookup = disableAnyFieldLookup;
    }
    
    public boolean getDisableCompositeFields() {
        return disableCompositeFields;
    }
    
    public final void setDisableCompositeFields(boolean disableCompositeFields) {
        this.disableCompositeFields = disableCompositeFields;
    }
    
    public boolean getCacheDataTypes() {
        return cacheDataTypes;
    }
    
    public void setCacheDataTypes(boolean cacheDataTypes) {
        this.cacheDataTypes = cacheDataTypes;
    }
    
    private Multimap<String,String> invertMultimap(Map<String,String> multi) {
        Multimap<String,String> inverse = HashMultimap.create();
        for (Entry<String,String> entry : multi.entrySet()) {
            inverse.put(entry.getValue(), entry.getKey());
        }
        return inverse;
    }
    
    public static <T> CloseableIterable<T> emptyCloseableIterator() {
        return new CloseableIterable<T>() {
            
            @Override
            public Iterator<T> iterator() {
                return Collections.<T> emptyList().iterator();
            }
            
            @Override
            public void close() throws IOException {}
            
        };
    }
    
    protected boolean areSortedUIDsRequired(ASTJexlScript script, ShardQueryConfiguration config) {
        boolean sortedUIDs = true;
        if (config.getUnsortedUIDsEnabled()) {
            // Note: this visitor is not taking delayed predicates into account as the VisitorFunction may end up moving them around.
            sortedUIDs = SortedUIDsRequiredVisitor.isRequired(script, config.getIndexedFields(), false);
        }
        return sortedUIDs;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see QueryPlanner#maxRangesPerQueryPiece()
     */
    @Override
    public long maxRangesPerQueryPiece() {
        return this.maxRangesPerQueryPiece;
    }
    
    public void setCompressOptionMappings(boolean compressMappings) {
        this.compressMappings = compressMappings;
    }
    
    public boolean getCompressOptionMappings() {
        return compressMappings;
    }
    
    /*
     * 
     * (non-Javadoc)
     * 
     * @see QueryPlanner#setRules(java.util. Collection )
     */
    @Override
    public void setRules(Collection<PushDownRule> rules) {
        this.rules.addAll(rules);
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see QueryPlanner#getRules()
     */
    @Override
    public Collection<PushDownRule> getRules() {
        return Collections.unmodifiableCollection(rules);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see QueryPlanner#getPlannedScript()
     */
    @Override
    public String getPlannedScript() {
        return plannedScript;
    }
    
    protected Multimap<String,Type<?>> configureIndexedAndNormalizedFields(MetadataHelper metadataHelper, ShardQueryConfiguration config,
                    ASTJexlScript queryTree) throws DatawaveQueryException {
        // Fetch the mapping of fields to Types from the DatawaveMetadata table
        
        Multimap<String,Type<?>> fieldToDatatypeMap = FetchDataTypesVisitor.fetchDataTypes(metadataHelper, config.getDatatypeFilter(), queryTree, false);
        
        try {
            return configureIndexedAndNormalizedFields(fieldToDatatypeMap, metadataHelper.getIndexedFields(null), metadataHelper.getAllNormalized(), config,
                            queryTree);
        } catch (InstantiationException | IllegalAccessException | TableNotFoundException e) {
            throw new DatawaveFatalQueryException(e);
        }
        
    }
    
    protected void setCachedFields(final Set<String> indexedFields, Multimap<String,Type<?>> queryFieldMap, Multimap<String,Type<?>> normalizedFieldMap,
                    ShardQueryConfiguration config) {
        config.setIndexedFields(indexedFields);
        config.setQueryFieldsDatatypes(queryFieldMap);
        config.setNormalizedFieldsDatatypes(normalizedFieldMap);
    }
    
    protected Multimap<String,Type<?>> configureIndexedAndNormalizedFields(Multimap<String,Type<?>> fieldToDatatypeMap, final Set<String> indexedFields,
                    final Set<String> normalizedFields, ShardQueryConfiguration config, ASTJexlScript queryTree) throws DatawaveQueryException,
                    TableNotFoundException, InstantiationException, IllegalAccessException {
        log.debug("config.getDatatypeFilter() = " + config.getDatatypeFilter());
        log.debug("fieldToDatatypeMap.keySet() is " + fieldToDatatypeMap.keySet());
        
        config.setIndexedFields(indexedFields);
        
        log.debug("normalizedFields = " + normalizedFields);
        
        config.setQueryFieldsDatatypes(HashMultimap.create(Multimaps.filterKeys(fieldToDatatypeMap, input -> !normalizedFields.contains(input))));
        log.debug("IndexedFields Datatypes: " + config.getQueryFieldsDatatypes());
        
        config.setNormalizedFieldsDatatypes(HashMultimap.create(Multimaps.filterKeys(fieldToDatatypeMap, normalizedFields::contains)));
        log.debug("NormalizedFields Datatypes: " + config.getNormalizedFieldsDatatypes());
        if (log.isTraceEnabled()) {
            log.trace("Normalizers:");
            for (String field : fieldToDatatypeMap.keySet()) {
                log.trace(field + ": " + fieldToDatatypeMap.get(field));
            }
        }
        
        return fieldToDatatypeMap;
        
    }
    
    public void setDisableTestNonExistentFields(boolean disableTestNonExistentFields) {
        this.disableTestNonExistentFields = disableTestNonExistentFields;
    }
    
    public boolean getDisableTestNonExistentFields() {
        return disableTestNonExistentFields;
    }
    
    public void setDisableExpandIndexFunction(boolean disableExpandIndexFunction) {
        this.disableExpandIndexFunction = disableExpandIndexFunction;
    }
    
    public boolean getDisableExpandIndexFunction() {
        return disableExpandIndexFunction;
    }
    
    public void setDisableRangeCoalescing(boolean disableRangeCoalescing) {
        this.disableRangeCoalescing = disableRangeCoalescing;
    }
    
    public boolean getDisableRangeCoalescing() {
        return disableRangeCoalescing;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see QueryPlanner#getQueryIteratorClass()
     */
    @Override
    public Class<? extends SortedKeyValueIterator<Key,Value>> getQueryIteratorClass() {
        return queryIteratorClazz;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see QueryPlanner#setQueryIteratorClass( java.lang.Class)
     */
    @Override
    public void setQueryIteratorClass(Class<? extends SortedKeyValueIterator<Key,Value>> clazz) {
        queryIteratorClazz = clazz;
    }
    
    public void setPreloadOptions(boolean preloadOptions) {
        this.preloadOptions = preloadOptions;
    }
    
    public boolean getPreloadOptions() {
        return preloadOptions;
    }
    
    @Override
    public DefaultQueryPlanner clone() {
        return new DefaultQueryPlanner(this);
    }
    
    public void setDocSpecificOverride(boolean docSpecificOverride) {
        this.docSpecificOverride = docSpecificOverride;
    }
    
    public boolean getDocSpecificOverride() {
        return docSpecificOverride;
    }
    
    public void setMaxRangeWaitMillis(long maxRangeWaitMillis) {
        this.maxRangeWaitMillis = maxRangeWaitMillis;
    }
    
    public long getMaxRangeWaitMillis() {
        return maxRangeWaitMillis;
    }
    
    public void setPushdownThreshold(long pushdownThreshold) {
        this.pushdownThreshold = pushdownThreshold;
    }
    
    public long getPushdownThreshold() {
        return pushdownThreshold;
    }
    
    public int getDocsToCombineForEvaluation() {
        return docsToCombineForEvaluation;
    }
    
    public void setDocsToCombineForEvaluation(final int docsToCombineForEvaluation) {
        this.docsToCombineForEvaluation = docsToCombineForEvaluation;
    }
    
    public boolean getCondenseUidsInRangeStream() {
        return condenseUidsInRangeStream;
    }
    
    public void setCondenseUidsInRangeStream(boolean condenseUidsInRangeStream) {
        this.condenseUidsInRangeStream = condenseUidsInRangeStream;
    }
    
    public boolean getCompressUids() {
        return compressUidsInRangeStream;
    }
    
    public void setCompressUids(boolean compressUidsInRangeStream) {
        this.compressUidsInRangeStream = compressUidsInRangeStream;
    }
    
    /**
     * Given a date, truncate it to year, month, date and increment the day by one to determine the following day.
     *
     * @param endDate
     * @return
     */
    public static Date getEndDateForIndexLookup(Date endDate) {
        Date newDate = DateUtils.truncate(endDate, Calendar.DATE);
        return DateUtils.addDays(newDate, 1);
    }
    
    @Override
    public void finalize() {
        if (null != builderThread) {
            builderThread.shutdown();
        }
    }
}
