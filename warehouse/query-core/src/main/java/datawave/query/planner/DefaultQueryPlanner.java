package datawave.query.planner;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.BOUNDED_RANGE;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_VALUE;

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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;

import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.core.iterators.querylock.QueryLock;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.configuration.QueryData;
import datawave.data.type.AbstractGeometryType;
import datawave.data.type.Type;
import datawave.ingest.mapreduce.handler.dateindex.DateIndexUtil;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl.Parameter;
import datawave.query.CloseableIterable;
import datawave.query.Constants;
import datawave.query.QueryParameters;
import datawave.query.attributes.ExcerptFields;
import datawave.query.attributes.UniqueFields;
import datawave.query.common.grouping.GroupFields;
import datawave.query.composite.CompositeMetadata;
import datawave.query.composite.CompositeUtils;
import datawave.query.config.ScanHintRule;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.CannotExpandUnfieldedTermFatalException;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.DatawaveQueryException;
import datawave.query.exceptions.DoNotPerformOptimizedQueryException;
import datawave.query.exceptions.EmptyUnfieldedTermExpansionException;
import datawave.query.exceptions.FullTableScansDisallowedException;
import datawave.query.exceptions.InvalidQueryException;
import datawave.query.exceptions.NoResultsException;
import datawave.query.function.JexlEvaluation;
import datawave.query.index.lookup.RangeStream;
import datawave.query.iterator.CloseableListIterable;
import datawave.query.iterator.QueryIterator;
import datawave.query.iterator.QueryOptions;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.iterator.logic.IndexIterator;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.NodeTypeCount;
import datawave.query.jexl.functions.EvaluationPhaseFilterFunctions;
import datawave.query.jexl.functions.QueryFunctions;
import datawave.query.jexl.lookups.IndexLookup;
import datawave.query.jexl.visitors.AddShardsAndDaysVisitor;
import datawave.query.jexl.visitors.BoundedRangeDetectionVisitor;
import datawave.query.jexl.visitors.BoundedRangeIndexExpansionVisitor;
import datawave.query.jexl.visitors.CaseSensitivityVisitor;
import datawave.query.jexl.visitors.ConjunctionEliminationVisitor;
import datawave.query.jexl.visitors.DepthVisitor;
import datawave.query.jexl.visitors.DisjunctionEliminationVisitor;
import datawave.query.jexl.visitors.ExecutableDeterminationVisitor;
import datawave.query.jexl.visitors.ExecutableDeterminationVisitor.STATE;
import datawave.query.jexl.visitors.ExecutableExpansionVisitor;
import datawave.query.jexl.visitors.ExpandCompositeTerms;
import datawave.query.jexl.visitors.ExpandMultiNormalizedTerms;
import datawave.query.jexl.visitors.FetchDataTypesVisitor;
import datawave.query.jexl.visitors.FieldMissingFromSchemaVisitor;
import datawave.query.jexl.visitors.FieldToFieldComparisonVisitor;
import datawave.query.jexl.visitors.FixNegativeNumbersVisitor;
import datawave.query.jexl.visitors.FixUnindexedNumericTerms;
import datawave.query.jexl.visitors.FunctionIndexQueryExpansionVisitor;
import datawave.query.jexl.visitors.GeoWavePruningVisitor;
import datawave.query.jexl.visitors.IndexedTermCountingVisitor;
import datawave.query.jexl.visitors.IngestTypePruningVisitor;
import datawave.query.jexl.visitors.IngestTypeVisitor;
import datawave.query.jexl.visitors.InvertNodeVisitor;
import datawave.query.jexl.visitors.IsNotNullIntentVisitor;
import datawave.query.jexl.visitors.IsNotNullPruningVisitor;
import datawave.query.jexl.visitors.IvaratorRequiredVisitor;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.NodeTypeCountVisitor;
import datawave.query.jexl.visitors.PrintingVisitor;
import datawave.query.jexl.visitors.PullupUnexecutableNodesVisitor;
import datawave.query.jexl.visitors.PushFunctionsIntoExceededValueRanges;
import datawave.query.jexl.visitors.PushdownLowSelectivityNodesVisitor;
import datawave.query.jexl.visitors.PushdownMissingIndexRangeNodesVisitor;
import datawave.query.jexl.visitors.PushdownUnexecutableNodesVisitor;
import datawave.query.jexl.visitors.QueryFieldsVisitor;
import datawave.query.jexl.visitors.QueryModelVisitor;
import datawave.query.jexl.visitors.QueryOptionsFromQueryVisitor;
import datawave.query.jexl.visitors.QueryPropertyMarkerSourceConsolidator;
import datawave.query.jexl.visitors.QueryPruningVisitor;
import datawave.query.jexl.visitors.RebuildingVisitor;
import datawave.query.jexl.visitors.RegexFunctionVisitor;
import datawave.query.jexl.visitors.RegexIndexExpansionVisitor;
import datawave.query.jexl.visitors.RewriteNegationsVisitor;
import datawave.query.jexl.visitors.RewriteNullFunctionsVisitor;
import datawave.query.jexl.visitors.SetMembershipVisitor;
import datawave.query.jexl.visitors.SortedUIDsRequiredVisitor;
import datawave.query.jexl.visitors.TermCountingVisitor;
import datawave.query.jexl.visitors.TreeFlatteningRebuildingVisitor;
import datawave.query.jexl.visitors.UnfieldedIndexExpansionVisitor;
import datawave.query.jexl.visitors.UniqueExpressionTermsVisitor;
import datawave.query.jexl.visitors.UnmarkedBoundedRangeDetectionVisitor;
import datawave.query.jexl.visitors.ValidComparisonVisitor;
import datawave.query.jexl.visitors.ValidPatternVisitor;
import datawave.query.jexl.visitors.ValidateFilterFunctionVisitor;
import datawave.query.jexl.visitors.order.OrderByCostVisitor;
import datawave.query.jexl.visitors.whindex.WhindexVisitor;
import datawave.query.model.QueryModel;
import datawave.query.planner.comparator.DefaultQueryPlanComparator;
import datawave.query.planner.comparator.GeoWaveQueryPlanComparator;
import datawave.query.planner.pushdown.PushDownVisitor;
import datawave.query.planner.pushdown.rules.PushDownRule;
import datawave.query.planner.rules.NodeTransformRule;
import datawave.query.planner.rules.NodeTransformVisitor;
import datawave.query.postprocessing.tf.Function;
import datawave.query.postprocessing.tf.TermOffsetPopulator;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.async.event.ReduceFields;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MetadataHelper;
import datawave.query.util.QueryStopwatch;
import datawave.query.util.Tuple2;
import datawave.query.util.TypeMetadata;
import datawave.util.time.TraceStopwatch;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NotFoundQueryException;
import datawave.webservice.query.exception.PreConditionFailedQueryException;
import datawave.webservice.query.exception.QueryException;

public class DefaultQueryPlanner extends QueryPlanner implements Cloneable {

    private static final Logger log = ThreadConfigurableLogger.getLogger(DefaultQueryPlanner.class);

    public static final String EXCEED_TERM_EXPANSION_ERROR = "Query failed because it exceeded the query term expansion threshold";

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
     * Disables Whindex (value-specific) field mappings for GeoWave functions.
     *
     * @see WhindexVisitor
     */
    protected boolean disableWhindexFieldMappings = false;

    /**
     * Disables the index expansion function
     */
    protected boolean disableExpandIndexFunction = false;

    /**
     * Allows developers to cache data types
     */
    protected boolean cacheDataTypes = false;

    /**
     * The max number of child nodes that we will print with the PrintingVisitor. If trace is enabled, all nodes will be printed.
     */
    public static int maxChildNodesToPrint = 10;

    public static int maxTermsToPrint = 100;

    private final long maxRangesPerQueryPiece;

    private static Cache<String,Set<String>> allFieldTypeMap = CacheBuilder.newBuilder().maximumSize(100).concurrencyLevel(100)
                    .expireAfterAccess(24, TimeUnit.HOURS).build();

    private static Cache<String,Multimap<String,Type<?>>> dataTypeMap = CacheBuilder.newBuilder().maximumSize(100).concurrencyLevel(100)
                    .expireAfterAccess(24, TimeUnit.HOURS).build();

    private static Multimap<String,Type<?>> queryFieldsAsDataTypeMap;

    private static Multimap<String,Type<?>> normalizedFieldAsDataTypeMap;

    // These are caches of the complete set of indexed and normalized fields
    private static Set<String> cachedIndexedFields = null;
    private static Set<String> cachedReverseIndexedFields = null;
    private static Set<String> cachedNormalizedFields = null;

    protected List<PushDownRule> rules = Lists.newArrayList();

    // A set of node plans. Basically these are transforms that will be applied to nodes. One example use is to
    // force certain regex patterns to be pushed down to evaluation
    private List<NodeTransformRule> transformRules = Lists.newArrayList();

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

    /**
     * Should the ExecutableExpansionVisitor be run
     */
    protected boolean executableExpansion = true;

    /**
     * Control if automated logical query reduction should be done
     */
    protected boolean reduceQuery = true;

    /**
     * Control if when applying logical query reduction the pruned query should be shown via an assignment node in the resulting query. There may be a
     * performance impact.
     */
    protected boolean showReducedQueryPrune = true;

    // handles boilerplate operations that surround a visitor's execution (e.g., timers, logging, validating)
    private TimedVisitorManager visitorManager = new TimedVisitorManager();

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
        rules.addAll(other.rules);
        queryIteratorClazz = other.queryIteratorClazz;
        setMetadataHelper(other.getMetadataHelper());
        setDateIndexHelper(other.getDateIndexHelper());
        setCompressOptionMappings(other.getCompressOptionMappings());
        buildQueryModel = other.buildQueryModel;
        preloadOptions = other.preloadOptions;
        rangeStreamClass = other.rangeStreamClass;
        setSourceLimit(other.sourceLimit);
        setPushdownThreshold(other.getPushdownThreshold());
        setVisitorManager(other.getVisitorManager());
        setTransformRules(other.getTransformRules() == null ? null : new ArrayList<>(other.transformRules));
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
        visitorManager.setDebugEnabled(log.isDebugEnabled());

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
        settingFuture = null;

        IteratorSetting cfg = null;

        if (preloadOptions) {
            cfg = getQueryIterator(metadataHelper, config, settings, "", false, true);
        }

        try {
            config.setQueryTree(updateQueryTree(scannerFactory, metadataHelper, dateIndexHelper, config, query, settings));
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

        boolean isFullTable = false;
        Tuple2<CloseableIterable<QueryPlan>,Boolean> queryRanges = null;

        if (!config.isGeneratePlanOnly()) {
            queryRanges = getQueryRanges(scannerFactory, metadataHelper, config, config.getQueryTree());

            // a full table scan is required if
            isFullTable = queryRanges.second();

            // abort if we cannot handle full table scans
            if (isFullTable && !config.getFullTableScanEnabled()) {
                PreConditionFailedQueryException qe = new PreConditionFailedQueryException(DatawaveErrorCode.FULL_TABLE_SCAN_REQUIRED_BUT_DISABLED);
                throw new FullTableScansDisallowedException(qe);
            }
        }

        final QueryStopwatch timers = config.getTimers();

        TraceStopwatch stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - Rebuild JEXL String from AST");

        // Set the final query after we're done mucking with it
        String newQueryString = JexlStringBuildingVisitor.buildQuery(config.getQueryTree());
        if (log.isTraceEnabled())
            log.trace("newQueryString is " + newQueryString);
        if (StringUtils.isBlank(newQueryString)) {
            stopwatch.stop();
            QueryException qe = new QueryException(DatawaveErrorCode.EMPTY_QUERY_STRING_AFTER_MODIFICATION);
            throw new DatawaveFatalQueryException(qe);
        }

        stopwatch.stop();
        stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - Construct IteratorSettings");

        if (!config.isGeneratePlanOnly()) {
            while (null == cfg) {
                cfg = getQueryIterator(metadataHelper, config, settings, "", false, false);
            }
            configureIterator(config, cfg, newQueryString, isFullTable);
        }

        final QueryData queryData = new QueryData().withQuery(newQueryString).withSettings(Lists.newArrayList(cfg));

        stopwatch.stop();

        this.plannedScript = newQueryString;
        config.setQueryString(this.plannedScript);

        if (!config.isGeneratePlanOnly()) {
            // add the geo query comparator to sort by geo range granularity if this is a geo query
            List<Comparator<QueryPlan>> queryPlanComparators = null;
            if (config.isSortGeoWaveQueryRanges()) {
                List<String> geoFields = new ArrayList<>();
                for (String fieldName : config.getIndexedFields()) {
                    for (Type type : config.getQueryFieldsDatatypes().get(fieldName)) {
                        if (type instanceof AbstractGeometryType) {
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
                    .setQueryTree(config.getQueryTree())
                    .setRanges(queryRanges.first())
                    .setMaxRanges(maxRangesPerQueryPiece())
                    .setSettings(settings)
                    .setMaxRangeWaitMillis(getMaxRangeWaitMillis())
                    .setQueryPlanComparators(queryPlanComparators)
                    .setNumRangesToBuffer(config.getNumRangesToBuffer())
                    .setRangeBufferTimeoutMillis(config.getRangeBufferTimeoutMillis())
                    .setRangeBufferPollMillis(config.getRangeBufferPollMillis())
                    .build();
            // @formatter:on
        } else {
            return null;
        }
    }

    private void configureIterator(ShardQueryConfiguration config, IteratorSetting cfg, String newQueryString, boolean isFullTable)
                    throws DatawaveQueryException {

        // Load enrichers, filters, unevaluatedExpressions, and projection
        // fields
        setCommonIteratorOptions(config, cfg);

        configureExcerpts(config, cfg);

        addOption(cfg, QueryOptions.LIMIT_FIELDS, config.getLimitFieldsAsString(), false);
        addOption(cfg, QueryOptions.MATCHING_FIELD_SETS, config.getMatchingFieldSetsAsString(), false);
        addOption(cfg, QueryOptions.GROUP_FIELDS, config.getGroupFields().toString(), true);
        addOption(cfg, QueryOptions.GROUP_FIELDS_BATCH_SIZE, config.getGroupFieldsBatchSizeAsString(), true);
        addOption(cfg, QueryOptions.UNIQUE_FIELDS, config.getUniqueFields().toString(), true);
        addOption(cfg, QueryOptions.HIT_LIST, Boolean.toString(config.isHitList()), false);
        addOption(cfg, QueryOptions.TERM_FREQUENCY_FIELDS, Joiner.on(',').join(config.getQueryTermFrequencyFields()), false);
        addOption(cfg, QueryOptions.TERM_FREQUENCIES_REQUIRED, Boolean.toString(config.isTermFrequenciesRequired()), false);
        addOption(cfg, QueryOptions.QUERY, newQueryString, false);
        addOption(cfg, QueryOptions.QUERY_ID, config.getQuery().getId().toString(), false);
        addOption(cfg, QueryOptions.FULL_TABLE_SCAN_ONLY, Boolean.toString(isFullTable), false);
        addOption(cfg, QueryOptions.TRACK_SIZES, Boolean.toString(config.isTrackSizes()), false);
        addOption(cfg, QueryOptions.ACTIVE_QUERY_LOG_NAME, config.getActiveQueryLogName(), false);
        // Set the start and end dates
        configureTypeMappings(config, cfg, metadataHelper, getCompressOptionMappings());
    }

    /**
     * Configure excerpt fields and iterator class, provided that term frequencies are required for the query
     *
     * @param config
     *            the ShardQueryConfiguration
     * @param cfg
     *            the IteratorSetting
     */
    private void configureExcerpts(ShardQueryConfiguration config, IteratorSetting cfg) {
        if (!config.getExcerptFields().isEmpty()) {
            addOption(cfg, QueryOptions.EXCERPT_FIELDS, config.getExcerptFields().toString(), true);
            addOption(cfg, QueryOptions.EXCERPT_ITERATOR, config.getExcerptIterator().getName(), false);
        }
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

        if (null != builderThread) {
            builderThread.shutdown();
        }
    }

    private QueryLock getQueryLock(ShardQueryConfiguration config, Query settings) throws Exception {
        return new QueryLock.Builder().forQueryId(settings.getId() == null ? null : settings.getId().toString()).forZookeeper(config.getZookeeperConfig(), 0)
                        .forHdfs(config.getHdfsSiteConfigURLs()).forFstDirs(config.getIvaratorFstHdfsBaseURIs()).forIvaratorDirs(config
                                        .getIvaratorCacheDirConfigs().stream().map(IvaratorCacheDirConfig::getBasePathURI).collect(Collectors.joining(",")))
                        .build();
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

    /**
     * NOT THREAD SAFE - relies on QueryStopwatch which is not thread safe
     *
     * @param lastOperation
     *            the last operation string
     * @param queryTree
     *            the query tree
     * @param config
     *            the configuration
     */
    public static void validateQuerySize(String lastOperation, final JexlNode queryTree, ShardQueryConfiguration config) {
        validateQuerySize(lastOperation, queryTree, config.getMaxDepthThreshold(), config.getInitialMaxTermThreshold(), config.getMaxIvaratorTerms(),
                        config.getTimers());
    }

    public static void validateQuerySize(String lastOperation, final JexlNode queryTree, int maxDepthThreshold, int maxTermThreshold,
                    int maxIvaratorThreshold) {
        validateQuerySize(lastOperation, queryTree, maxDepthThreshold, maxTermThreshold, maxIvaratorThreshold, null);
    }

    /**
     * NOT THREAD SAFE when called with timed=true
     *
     * @param lastOperation
     *            the last operation string
     * @param queryTree
     *            the query tree
     * @param maxDepthThreshold
     *            the max depth threshold
     * @param maxTermThreshold
     *            max term threshold
     * @param maxIvaratorThreshold
     *            max ivarators
     * @param timers
     *            timers for metrics
     */
    public static void validateQuerySize(String lastOperation, final JexlNode queryTree, int maxDepthThreshold, int maxTermThreshold, int maxIvaratorThreshold,
                    QueryStopwatch timers) {
        TraceStopwatch stopwatch = null;

        if (timers != null) {
            stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - Validate against term and depth thresholds");
        }

        // check the query depth (up to config.getMaxDepthThreshold() + 1)
        int depth = DepthVisitor.getDepth(queryTree, maxDepthThreshold);
        if (depth > maxDepthThreshold) {
            PreConditionFailedQueryException qe = new PreConditionFailedQueryException(DatawaveErrorCode.QUERY_DEPTH_THRESHOLD_EXCEEDED,
                            MessageFormat.format("{0} > {1}, last operation: {2}", depth, maxDepthThreshold, lastOperation));
            throw new DatawaveFatalQueryException(qe);
        }

        // count the terms
        int termCount = TermCountingVisitor.countTerms(queryTree);
        if (termCount > maxTermThreshold) {
            PreConditionFailedQueryException qe = new PreConditionFailedQueryException(DatawaveErrorCode.QUERY_TERM_THRESHOLD_EXCEEDED,
                            MessageFormat.format("{0} > {1}, last operation: {2}", termCount, maxTermThreshold, lastOperation));
            throw new DatawaveFatalQueryException(qe);
        }

        // now check whether we are over the ivarator limit
        if (maxIvaratorThreshold >= 0) {
            NodeTypeCount nodeCount = JexlASTHelper.getIvarators(queryTree);
            int totalIvarators = JexlASTHelper.getIvaratorCount(nodeCount);
            if (totalIvarators > maxIvaratorThreshold) {
                QueryException qe = new QueryException(DatawaveErrorCode.EXPAND_QUERY_TERM_SYSTEM_LIMITS, Integer.toString(totalIvarators)
                                + " terms require server side expansion which is greater than the max of " + maxIvaratorThreshold);
                throw new DatawaveFatalQueryException(qe);
            }
        }

        if (stopwatch != null) {
            stopwatch.stop();
        }
    }

    protected ASTJexlScript updateQueryTree(ScannerFactory scannerFactory, MetadataHelper metadataHelper, DateIndexHelper dateIndexHelper,
                    ShardQueryConfiguration config, String query, Query settings) throws DatawaveQueryException {
        final QueryStopwatch timers = config.getTimers();

        TraceStopwatch stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - Parse query");

        config.setQueryTree(parseQueryAndValidatePattern(query, stopwatch));

        if (log.isDebugEnabled()) {
            logQuery(config.getQueryTree(), "Query after initial parse:");
        }

        stopwatch.stop();

        Map<String,String> optionsMap = new HashMap<>();
        if (query.contains(QueryFunctions.QUERY_FUNCTION_NAMESPACE + ':')) {
            // only do the extra tree visit if the function is present
            stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - parse out queryOptions from options function");
            config.setQueryTree(QueryOptionsFromQueryVisitor.collect(config.getQueryTree(), optionsMap));
            if (!optionsMap.isEmpty()) {
                QueryOptionsSwitch.apply(optionsMap, config);
            }
            stopwatch.stop();
        }

        // groom the query so that any nodes with the literal on the left and the identifier on
        // the right will be re-ordered to simplify subsequent processing

        config.setQueryTree(timedInvertSwappedNodes(timers, config.getQueryTree()));

        config.setQueryTree(timedFixNotNullIntent(timers, config.getQueryTree()));

        config.setQueryTree(timedIncludeDateFilters(timers, config.getQueryTree(), config, metadataHelper, scannerFactory, dateIndexHelper, settings));

        // note this must be called after we do the date adjustments per the query date type in addDateFilters
        timedCapDateRange(timers, config);

        // Find unmarked bounded ranges
        if (UnmarkedBoundedRangeDetectionVisitor.findUnmarkedBoundedRanges(config.getQueryTree())) {
            throw new DatawaveFatalQueryException("Found incorrectly marked bounded ranges");
        }

        if (optionsMap.containsKey(QueryParameters.SHARDS_AND_DAYS)) {
            config.setQueryTree(timedAddShardsAndDaysFromOptions(timers, config.getQueryTree(), optionsMap));
        } else {
            // look for the shards and days hint in the query settings
            // the shards and days hint cannot always be specified in the query string when using certain query parsers
            Parameter parameter = settings.findParameter(QueryParameters.SHARDS_AND_DAYS);
            if (StringUtils.isNotBlank(parameter.getParameterValue())) {
                optionsMap.put(QueryParameters.SHARDS_AND_DAYS, parameter.getParameterValue());
                config.setQueryTree(timedAddShardsAndDaysFromOptions(timers, config.getQueryTree(), optionsMap));
            }
        }

        // flatten the tree
        config.setQueryTree(timedFlatten(timers, config.getQueryTree()));

        validateQuerySize("initial parse", config.getQueryTree(), config);

        config.setQueryTree(timedApplyRules(timers, config.getQueryTree(), config, metadataHelper, scannerFactory));

        config.setQueryTree(timedFixNegativeNumbers(timers, config.getQueryTree()));

        // Fix any query property markers that have multiple unwrapped sources.
        config.setQueryTree(timedFixQueryPropertyMarkers(timers, config.getQueryTree()));

        // Ensure that all ASTIdentifier nodes (field names) are upper-case to be consistent with what is enforced at ingest time
        // this will also ensure that various configure fields for projections, grouping, etc are upper cased as well
        config.setQueryTree(timedUpperCaseIdentifiers(timers, config.getQueryTree(), config, metadataHelper));

        config.setQueryTree(timedRewriteNegations(timers, config.getQueryTree()));

        QueryModel queryModel = loadQueryModel(config);

        config.setQueryTree(timedApplyQueryModel(timers, config.getQueryTree(), config, metadataHelper, queryModel));

        // +-------------------------------------+
        // | Post Query Model Expansion Clean Up |
        // +-------------------------------------+

        Set<String> indexOnlyFields = loadIndexedFields(config);

        if (!indexOnlyFields.isEmpty()) {
            // filter:includeRegex and filter:excludeRegex functions cannot be run against index-only fields, clean that up
            config.setQueryTree(expandRegexFunctionNodes(config.getQueryTree(), config, metadataHelper, indexOnlyFields));
        }

        // validate filter functions are not running against index-only fields
        config.setQueryTree(timedValidateFilterFunctions(timers, config.getQueryTree(), indexOnlyFields));

        // rewrite filter:isNull and filter:isNotNull functions into their EQ and !(EQ) equivalents
        config.setQueryTree(timedRewriteNullFunctions(timers, config.getQueryTree()));

        // might be possible to completely eliminate rewritten isNotNull terms...
        config.setQueryTree(timedPruneIsNotNullNodes(timers, config.getQueryTree()));

        // Enforce unique terms within an AND or OR expression.
        if (config.getEnforceUniqueTermsWithinExpressions()) {
            config.setQueryTree(timedEnforceUniqueTermsWithinExpressions(timers, config.getQueryTree()));
        }

        // Enforce unique AND'd terms within OR expressions.
        if (config.getEnforceUniqueConjunctionsWithinExpression()) {
            config.setQueryTree(timedEnforceUniqueConjunctionsWithinExpressions(timers, config.getQueryTree()));
        }

        // Enforce unique OR'd terms within AND expressions.
        if (config.getEnforceUniqueDisjunctionsWithinExpression()) {
            config.setQueryTree(timedEnforceUniqueDisjunctionsWithinExpressions(timers, config.getQueryTree()));
        }

        if (disableBoundedLookup) {
            // protection mechanism. If we disable bounded ranges and have a
            // LT,GT or ER node, we should expand it
            if (BoundedRangeDetectionVisitor.mustExpandBoundedRange(config, metadataHelper, config.getQueryTree())) {
                disableBoundedLookup = false;
            }
        }

        config.setQueryTree(processTree(config.getQueryTree(), config, settings, metadataHelper, scannerFactory, timers, queryModel));

        // ExpandCompositeTerms was here

        if (!indexOnlyFields.isEmpty() && !disableBoundedLookup) {

            // Figure out if the query contained any index only terms so we know
            // if we have to force it down the field-index path with event-specific
            // ranges
            timedCheckForIndexOnlyFieldsInQuery(timers, "Check for Index-Only Fields", config.getQueryTree(), config, indexOnlyFields);
        }

        timedCheckForCompositeFields(timers, "Check for Composite Fields", config, metadataHelper);

        timedCheckForSortedUids(timers, "Check for Sorted UIDs", config);

        // check the query for any fields that are term frequencies
        // if any exist, populate the shard query config with these fields
        timedCheckForTokenizedFields(timers, "Check for term frequency (tokenized) fields", config, metadataHelper);

        if (reduceQuery) {
            config.setQueryTree(timedReduce(timers, "Reduce Query Final", config.getQueryTree()));
        }

        timeScanHintRules(timers, "Apply scan hint rules", config);

        return config.getQueryTree();
    }

    protected ASTJexlScript processTree(final ASTJexlScript originalQueryTree, ShardQueryConfiguration config, Query settings, MetadataHelper metadataHelper,
                    ScannerFactory scannerFactory, QueryStopwatch timers, QueryModel queryModel) throws DatawaveQueryException {
        config.setQueryTree(originalQueryTree);

        TraceStopwatch stopwatch = null;

        if (!disableWhindexFieldMappings) {
            // apply the value-specific field mappings for GeoWave functions
            config.setQueryTree(timedApplyWhindexFieldMappings(timers, config.getQueryTree(), config, metadataHelper, settings));
        }

        if (!disableExpandIndexFunction) {
            // expand the index queries for the functions
            config.setQueryTree(timedExpandIndexQueriesForFunctions(timers, config.getQueryTree(), config, metadataHelper));
        }

        // apply the node transform rules
        // running it here before any unfielded expansions to enable potentially pushing down terms before index lookups
        config.setQueryTree(timedApplyNodeTransformRules(timers, "Apply Node Transform Rules - Pre Unfielded Expansions", config.getQueryTree(), config,
                        metadataHelper, getTransformRules()));

        // Find unfielded terms, and fully qualify them with an OR of all fields
        // found in the index
        // If the max term expansion is reached, then the original query tree is
        // returned.
        // If the max regex expansion is reached for a term, then it will be
        // left as a regex
        if (!disableAnyFieldLookup) {
            config.setQueryTree(timedExpandAnyFieldRegexNodes(timers, config.getQueryTree(), config, metadataHelper, scannerFactory, settings.getQuery()));
        }

        if (reduceQuery) {
            config.setQueryTree(timedReduce(timers, "Reduce Query After ANYFIELD Expansions", config.getQueryTree()));
        }

        if (!disableTestNonExistentFields && (!config.getIgnoreNonExistentFields())) {
            timedTestForNonExistentFields(timers, config.getQueryTree(), config, metadataHelper, queryModel, settings);
        } else {
            log.debug("Skipping check for nonExistentFields..");
        }

        // apply the node transform rules
        // running it here before any regex or range expansions to enable potentially pushing down terms before index lookups
        config.setQueryTree(timedApplyNodeTransformRules(timers, "Apply Node Transform Rules - Pre Regex/Range Expansions", config.getQueryTree(), config,
                        metadataHelper, getTransformRules()));

        timedFetchDatatypes(timers, "Fetch Required Datatypes", config.getQueryTree(), config);

        config.setQueryTree(timedFixUnindexedNumerics(timers, config.getQueryTree(), config));

        config.setQueryTree(timedExpandMultiNormalizedTerms(timers, config.getQueryTree(), config, metadataHelper));

        // if we have any index holes, then mark em
        if (!config.getIndexHoles().isEmpty()) {
            config.setQueryTree(timedMarkIndexHoles(timers, config.getQueryTree(), config, metadataHelper));
        }

        // lets precompute the indexed fields and index only fields for the specific datatype if needed below
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

        // apply the node transform rules
        config.setQueryTree(timedApplyNodeTransformRules(timers, "Apply Node Transform Rules - Pre Pushdown/Pullup Expansions", config.getQueryTree(), config,
                        metadataHelper, getTransformRules()));

        // push down terms that are over the min selectivity
        if (config.getMinSelectivity() > 0) {
            config.setQueryTree(timedPushdownLowSelectiveTerms(timers, config));
        }

        config.setQueryTree(timedForceFieldToFieldComparison(timers, config.getQueryTree()));

        if (!disableCompositeFields) {
            config.setQueryTree(timedExpandCompositeFields(timers, config.getQueryTree(), config));
        }

        if (!disableBoundedLookup) {
            stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - Expand bounded query ranges (total)");

            // Expand any bounded ranges into a conjunction of discrete terms
            try {
                Map<String,IndexLookup> indexLookupMap = new HashMap<>();

                // Check if there is any regex to expand.
                NodeTypeCount nodeCount = NodeTypeCountVisitor.countNodes(config.getQueryTree(), ASTNRNode.class, ASTERNode.class, BOUNDED_RANGE,
                                ASTFunctionNode.class, EXCEEDED_VALUE);
                if (nodeCount.hasAny(ASTNRNode.class, ASTERNode.class)) {
                    config.setQueryTree(
                                    timedExpandRegex(timers, "Expand Regex", config.getQueryTree(), config, metadataHelper, scannerFactory, indexLookupMap));
                }

                // Check if there are any bounded ranges to expand.
                if (nodeCount.isPresent(BOUNDED_RANGE)) {
                    config.setQueryTree(timedExpandRanges(timers, "Expand Ranges", config.getQueryTree(), config, metadataHelper, scannerFactory));
                }

                // NOTE: GeoWavePruningVisitor should run before QueryPruningVisitor. If it runs after, there is a chance
                // that GeoWavePruningVisitor will prune all of the remaining indexed terms, which would leave a GeoWave
                // function without any indexed terms or ranges, which should evaluate to false. That case won't be handled
                // properly if we run GeoWavePruningVisitor after QueryPruningVisitor.
                config.setQueryTree(timedPruneGeoWaveTerms(timers, config.getQueryTree(), metadataHelper));

                if (reduceQuery) {
                    config.setQueryTree(timedReduce(timers, "Reduce Query After Range Expansion", config.getQueryTree()));
                }

                // Check if there are functions that can be pushed into exceeded value ranges.
                if (nodeCount.isPresent(ASTFunctionNode.class) && nodeCount.isPresent(EXCEEDED_VALUE)) {
                    config.setQueryTree(timedPushFunctions(timers, config.getQueryTree(), config, metadataHelper));
                }

                if (executableExpansion) {
                    config.setQueryTree(timedExecutableExpansion(timers, config.getQueryTree(), config, metadataHelper));
                }

                LinkedList<String> debugOutput = null;
                if (log.isDebugEnabled()) {
                    debugOutput = new LinkedList<>();
                }

                // Unless config.isExpandAllTerms is true, this may set some of
                // the terms to be delayed.
                if (!ExecutableDeterminationVisitor.isExecutable(config.getQueryTree(), config, indexedFields, indexOnlyFields, nonEventFields, debugOutput,
                                metadataHelper)) {

                    // if we now have an unexecutable tree because of delayed
                    // predicates, then remove delayed predicates as needed and
                    // reexpand
                    config.setQueryTree(timedRemoveDelayedPredicates(timers, "Remove Delayed Predicates", config.getQueryTree(), config, metadataHelper,
                                    indexedFields, indexOnlyFields, nonEventFields, indexLookupMap, scannerFactory, metadataHelper, debugOutput));
                }

                // if we now have an unexecutable tree because of missing
                // delayed predicates, then add delayed predicates where
                // possible
                config.setQueryTree(timedAddDelayedPredicates(timers, "Add Delayed Predicates", config.getQueryTree(), config, metadataHelper, indexedFields,
                                indexOnlyFields, nonEventFields, debugOutput));

            } catch (TableNotFoundException e) {
                stopwatch.stop();
                QueryException qe = new QueryException(DatawaveErrorCode.METADATA_ACCESS_ERROR, e);
                throw new DatawaveFatalQueryException(qe);
            } catch (CannotExpandUnfieldedTermFatalException e) {
                if (null != e.getCause() && e.getCause() instanceof DoNotPerformOptimizedQueryException) {
                    throw (DoNotPerformOptimizedQueryException) e.getCause();
                }
                QueryException qe = new QueryException(DatawaveErrorCode.INDETERMINATE_INDEX_STATUS, e);
                throw new DatawaveFatalQueryException(qe);
            }
            stopwatch.stop();
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Bounded range and regex conversion has been disabled");
            }
        }

        return config.getQueryTree();
    }

    protected void timedFetchDatatypes(QueryStopwatch timers, String stage, final ASTJexlScript script, ShardQueryConfiguration config)
                    throws DatawaveQueryException {
        TraceStopwatch stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - " + stage);
        try {
            Multimap<String,Type<?>> fieldToDatatypeMap = null;
            if (cacheDataTypes) {
                fieldToDatatypeMap = dataTypeMap.getIfPresent(String.valueOf(config.getDatatypeFilter().hashCode()));
            }

            if (null != fieldToDatatypeMap) {
                Set<String> indexedFields = Sets.newHashSet();
                Set<String> reverseIndexedFields = Sets.newHashSet();
                Set<String> normalizedFields = Sets.newHashSet();

                loadDataTypeMetadata(fieldToDatatypeMap, indexedFields, reverseIndexedFields, normalizedFields, false);

                if (null == queryFieldsAsDataTypeMap) {
                    queryFieldsAsDataTypeMap = HashMultimap.create(Multimaps.filterKeys(fieldToDatatypeMap, input -> !cachedNormalizedFields.contains(input)));
                }

                if (null == normalizedFieldAsDataTypeMap) {
                    normalizedFieldAsDataTypeMap = HashMultimap
                                    .create(Multimaps.filterKeys(fieldToDatatypeMap, input -> cachedNormalizedFields.contains(input)));
                }

                setCachedFields(indexedFields, reverseIndexedFields, queryFieldsAsDataTypeMap, normalizedFieldAsDataTypeMap, config);
            } else {
                fieldToDatatypeMap = configureIndexedAndNormalizedFields(metadataHelper, config, script);

                if (cacheDataTypes) {
                    loadDataTypeMetadata(null, null, null, null, true);

                    dataTypeMap.put(String.valueOf(config.getDatatypeFilter().hashCode()), metadataHelper.getFieldsToDatatypes(config.getDatatypeFilter()));
                }
            }

        } catch (InstantiationException | IllegalAccessException | AccumuloException | AccumuloSecurityException | TableNotFoundException
                        | ExecutionException e) {
            throw new DatawaveFatalQueryException(e);
        } finally {
            stopwatch.stop();
        }
    }

    protected boolean timedCheckForCompositeFields(QueryStopwatch timers, String stage, ShardQueryConfiguration config, MetadataHelper metadataHelper) {
        boolean containsComposites = false;
        TraceStopwatch stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - " + stage);

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
        if (!compositeFields.isEmpty()) {
            boolean functionsEnabled = config.isCompositeFilterFunctionsEnabled();
            containsComposites = !SetMembershipVisitor.getMembers(compositeFields.keySet(), config, config.getQueryTree(), functionsEnabled).isEmpty();
        }

        // Print the nice log message
        if (log.isDebugEnabled()) {
            logQuery(config.getQueryTree(),
                            "Computed that the query " + (containsComposites ? " contains " : " does not contain any ") + " composite field(s)");
        }

        config.setContainsCompositeTerms(containsComposites);
        stopwatch.stop();

        return containsComposites;
    }

    protected boolean timedCheckForSortedUids(QueryStopwatch timers, String stage, ShardQueryConfiguration config) {
        TraceStopwatch stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - " + stage);

        // determine whether sortedUIDs are required. Normally they are, however if the query contains
        // only one indexed term, then there is no need to sort which can be a lot faster if an ivarator
        // is required.
        boolean sortedUIDs = areSortedUIDsRequired(config.getQueryTree(), config);

        // Print the nice log message
        if (log.isDebugEnabled()) {
            logQuery(config.getQueryTree(), "Computed that the query " + (sortedUIDs ? " requires " : " does not require ") + " sorted UIDs");
        }

        config.setSortedUIDs(sortedUIDs);
        stopwatch.stop();

        return sortedUIDs;
    }

    protected void timeScanHintRules(QueryStopwatch timers, String stage, ShardQueryConfiguration config) {
        TraceStopwatch stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - " + stage);

        // apply runtime scan hints
        if (config.isUseQueryTreeScanHintRules()) {
            // get the current table hints
            final Map<String,Map<String,String>> allHints = config.getTableHints();

            // loop over all rules
            for (ScanHintRule<JexlNode> hintRule : config.getQueryTreeScanHintRules()) {
                final String hintTable = hintRule.getTable();
                // does the hint have a table specified and apply given the query tree?
                if (hintTable != null && hintRule.apply(config.getQueryTree())) {
                    // get the table hints
                    Map<String,String> tableHints = allHints.get(hintTable);
                    if (tableHints == null) {
                        tableHints = new HashMap<>();
                        config.getTableHints().put(hintTable, tableHints);
                    }

                    // is the hint well defined?
                    if (hintRule.getHintName() == null || hintRule.getHintValue() == null) {
                        log.warn("Skipping invalid ScanHintRule. No hint name or value set. " + hintRule);
                        continue;
                    }

                    // check for overwrite for logging
                    if (tableHints.get(hintRule.getHintName()) != null) {
                        // overwriting, log it
                        log.info("Overwriting scan hint for table " + hintRule.getTable() + " " + hintRule.getHintName() + "="
                                        + tableHints.get(hintRule.getHintName()) + " to " + hintRule.getHintValue());
                    }

                    // apply the new hint
                    tableHints.put(hintRule.getHintName(), hintRule.getHintValue());

                    // check if any other rules should be evaluated
                    if (!hintRule.isChainable()) {
                        log.info("Unchainable ScanHintRule applied, " + hintRule);
                        break;
                    }
                }
            }

            // push any changes back to config
            config.setTableHints(allHints);
        }
        log.info("applying query tree scan hints: " + config.getTableHints());

        stopwatch.stop();
    }

    protected void timedCheckForTokenizedFields(QueryStopwatch timers, String stage, ShardQueryConfiguration config, MetadataHelper metadataHelper) {
        TraceStopwatch stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - " + stage);

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
            queryTfFields = SetMembershipVisitor.getMembers(termFrequencyFields, config, config.getQueryTree());

            // Print the nice log message
            if (log.isDebugEnabled()) {
                logQuery(config.getQueryTree(), "Computed that the query "
                                + (queryTfFields.isEmpty() ? " does not contain any " : "contains " + queryTfFields + " as ") + " term frequency field(s)");
            }
        }

        config.setQueryTermFrequencyFields(queryTfFields);

        // now determine if we actually require gathering term frequencies
        if (!queryTfFields.isEmpty()) {
            Multimap<String,Function> contentFunctions = TermOffsetPopulator.getContentFunctions(config.getQueryTree());
            config.setTermFrequenciesRequired(!contentFunctions.isEmpty());

            // Print the nice log message
            if (log.isDebugEnabled()) {
                logQuery(config.getQueryTree(),
                                "Computed that the query " + (contentFunctions.isEmpty() ? " does not require " : "requires") + " term frequency lookup");
            }
        }

        stopwatch.stop();
    }

    protected QueryModel loadQueryModel(ShardQueryConfiguration config) {
        QueryModelProvider queryModelProvider = this.queryModelProviderFactory.createQueryModelProvider();
        if (queryModelProvider instanceof MetadataHelperQueryModelProvider) {
            ((MetadataHelperQueryModelProvider) queryModelProvider).setMetadataHelper(metadataHelper);
            ((MetadataHelperQueryModelProvider) queryModelProvider).setConfig(config);
        }
        return queryModelProvider.getQueryModel();
    }

    /*


     */

    protected Set<String> loadIndexedFields(ShardQueryConfiguration config) {
        try {
            return metadataHelper.getIndexOnlyFields(config.getDatatypeFilter());
        } catch (TableNotFoundException e) {
            QueryException qe = new QueryException(DatawaveErrorCode.INDEX_ONLY_FIELDS_RETRIEVAL_ERROR, e);
            throw new DatawaveFatalQueryException(qe);
        }
    }

    /**
     * Loads expansion fields filtered by datatype. If an error occurs that error is rethrown as a {@link DatawaveFatalQueryException}
     *
     * @param config
     *            a configuration
     * @return list of expansion fields
     */
    protected Set<String> loadExpansionFields(ShardQueryConfiguration config) {
        try {
            return metadataHelper.getExpansionFields(config.getDatatypeFilter());
        } catch (TableNotFoundException e) {
            QueryException qe = new QueryException(DatawaveErrorCode.METADATA_ACCESS_ERROR, e);
            log.info(qe);
            throw new DatawaveFatalQueryException(qe);
        }
    }

    /*
     * Start methods that operate on the query tree
     */

    /**
     * Handle case when input field value pairs are swapped
     *
     * @param script
     *            the jexl script
     * @param timers
     *            timers for metrics
     * @return a script with swapped nodes
     * @throws DatawaveQueryException
     *             for issues with queries
     */
    protected ASTJexlScript timedInvertSwappedNodes(QueryStopwatch timers, final ASTJexlScript script) throws DatawaveQueryException {
        return visitorManager.timedVisit(timers, "Invert Swapped Nodes", () -> (InvertNodeVisitor.invertSwappedNodes(script)));
    }

    /**
     * Handle special case where a regex node can be replaced with a 'not equals null' node
     *
     * @param script
     *            the jexl script
     * @param timers
     *            timers for metrics
     * @return jexl query tree
     * @throws DatawaveQueryException
     *             for issues with datawave queries
     */
    protected ASTJexlScript timedFixNotNullIntent(QueryStopwatch timers, final ASTJexlScript script) throws DatawaveQueryException {
        try {
            return visitorManager.timedVisit(timers, "Fix Not Null Intent", () -> (IsNotNullIntentVisitor.fixNotNullIntent(script)));
        } catch (Exception e) {
            throw new DatawaveQueryException("Something bad happened", e);
        }
    }

    protected ASTJexlScript timedIncludeDateFilters(QueryStopwatch timers, final ASTJexlScript script, ShardQueryConfiguration config,
                    MetadataHelper metadataHelper, ScannerFactory scannerFactory, DateIndexHelper dateIndexHelper, Query settings)
                    throws DatawaveQueryException {
        return visitorManager.timedVisit(timers, "Include Date Filters", () -> {
            try {
                return (addDateFilters(script, scannerFactory, metadataHelper, dateIndexHelper, config, settings));
            } catch (TableNotFoundException e) {
                throw new DatawaveQueryException("Unable to resolve date index", e);
            }
        });
    }

    protected void timedCapDateRange(QueryStopwatch timers, ShardQueryConfiguration config) throws DatawaveQueryException {
        TraceStopwatch stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - Cap Date Range");

        // note this must be called after we do the date adjustments per the query date type in addDateFilters
        capDateRange(config);

        stopwatch.stop();
    }

    protected ASTJexlScript timedFlatten(QueryStopwatch timers, final ASTJexlScript script) throws DatawaveQueryException {
        return visitorManager.timedVisit(timers, "Flatten", () -> (TreeFlatteningRebuildingVisitor.flatten(script)));
    }

    protected ASTJexlScript timedAddShardsAndDaysFromOptions(QueryStopwatch timers, final ASTJexlScript script, Map<String,String> optionsMap)
                    throws DatawaveQueryException {
        String shardsAndDays = optionsMap.get(QueryParameters.SHARDS_AND_DAYS);
        return visitorManager.timedVisit(timers, "Add SHARDS_AND_DAYS From Options", () -> (AddShardsAndDaysVisitor.update(script, shardsAndDays)));
    }

    protected ASTJexlScript timedApplyRules(QueryStopwatch timers, ASTJexlScript script, ShardQueryConfiguration config, MetadataHelper metadataHelper,
                    ScannerFactory scannerFactory) throws DatawaveQueryException {
        return visitorManager.timedVisit(timers, "Apply Pushdown Rules", () -> (applyRules(script, scannerFactory, metadataHelper, config)));
    }

    protected ASTJexlScript timedFixNegativeNumbers(QueryStopwatch timers, final ASTJexlScript script) throws DatawaveQueryException {
        return visitorManager.timedVisit(timers, "Restructure Negative Numbers", () -> (FixNegativeNumbersVisitor.fix(script)));
    }

    protected ASTJexlScript timedUpperCaseIdentifiers(QueryStopwatch timers, final ASTJexlScript script, ShardQueryConfiguration config,
                    MetadataHelper metadataHelper) throws DatawaveQueryException {
        return visitorManager.timedVisit(timers, "Uppercase Field Names", () -> (upperCaseIdentifiers(metadataHelper, config, script)));
    }

    protected ASTJexlScript timedRewriteNegations(QueryStopwatch timers, final ASTJexlScript script) throws DatawaveQueryException {
        return visitorManager.timedVisit(timers, "Rewrite Negated Equality Operators", () -> (RewriteNegationsVisitor.rewrite(script)));
    }

    protected ASTJexlScript timedPruneIsNotNullNodes(QueryStopwatch timers, final ASTJexlScript script) throws DatawaveQueryException {
        return visitorManager.timedVisit(timers, "Prune IsNotNull Nodes", () -> (ASTJexlScript) IsNotNullPruningVisitor.prune(script));
    }

    protected ASTJexlScript timedApplyQueryModel(QueryStopwatch timers, final ASTJexlScript script, ShardQueryConfiguration config,
                    MetadataHelper metadataHelper, QueryModel queryModel) throws DatawaveQueryException {
        if (queryModel != null) {
            return visitorManager.timedVisit(timers, "Apply Query Model", () -> (applyQueryModel(metadataHelper, config, script, queryModel)));
        } else {
            log.warn("Query Model was null, will not apply to query tree. This could be a fatal error.");
            return script;
        }
    }

    protected ASTJexlScript timedValidateFilterFunctions(QueryStopwatch timers, ASTJexlScript queryTree, Set<String> indexOnlyFields)
                    throws DatawaveQueryException {
        return visitorManager.timedVisit(timers, "Validate Filter Functions",
                        () -> (ASTJexlScript) ValidateFilterFunctionVisitor.validate(queryTree, indexOnlyFields));
    }

    protected ASTJexlScript timedRewriteNullFunctions(QueryStopwatch timers, ASTJexlScript queryTree) throws DatawaveQueryException {
        return visitorManager.timedVisit(timers, "Rewrite Null Functions", () -> RewriteNullFunctionsVisitor.rewriteNullFunctions(queryTree));
    }

    protected ASTJexlScript timedEnforceUniqueTermsWithinExpressions(QueryStopwatch timers, final ASTJexlScript script) throws DatawaveQueryException {
        return visitorManager.timedVisit(timers, "Enforce Unique Terms within AND and OR expressions", () -> (UniqueExpressionTermsVisitor.enforce(script)));
    }

    protected ASTJexlScript timedEnforceUniqueConjunctionsWithinExpressions(QueryStopwatch timers, final ASTJexlScript script) throws DatawaveQueryException {
        return visitorManager.timedVisit(timers, "Enforce Unique AND'd Terms within OR expressions", () -> (ConjunctionEliminationVisitor.optimize(script)));
    }

    protected ASTJexlScript timedEnforceUniqueDisjunctionsWithinExpressions(QueryStopwatch timers, final ASTJexlScript script) throws DatawaveQueryException {
        return visitorManager.timedVisit(timers, "Enforce Unique OR'd Terms within AND expressions", () -> (DisjunctionEliminationVisitor.optimize(script)));
    }

    protected ASTJexlScript expandRegexFunctionNodes(final ASTJexlScript script, ShardQueryConfiguration config, MetadataHelper metadataHelper,
                    Set<String> indexOnlyFields) throws DatawaveQueryException {
        return visitorManager.validateAndVisit(() -> (RegexFunctionVisitor.expandRegex(config, metadataHelper, indexOnlyFields, script)));
    }

    protected ASTJexlScript timedApplyWhindexFieldMappings(QueryStopwatch timers, final ASTJexlScript script, ShardQueryConfiguration config,
                    MetadataHelper metadataHelper, Query settings) throws DatawaveQueryException {
        try {
            config.setWhindexCreationDates(metadataHelper.getWhindexCreationDateMap(config.getDatatypeFilter()));
        } catch (TableNotFoundException e) {
            QueryException qe = new QueryException(DatawaveErrorCode.METADATA_ACCESS_ERROR, e);
            throw new DatawaveFatalQueryException(qe);
        }
        return visitorManager.timedVisit(timers, "Apply Whindex Field Mappings",
                        () -> (WhindexVisitor.apply(script, config, settings.getBeginDate(), metadataHelper)));
    }

    protected ASTJexlScript timedExpandIndexQueriesForFunctions(QueryStopwatch timers, final ASTJexlScript script, ShardQueryConfiguration config,
                    MetadataHelper metadataHelper) throws DatawaveQueryException {
        return visitorManager.timedVisit(timers, "Expand Function Index Queries",
                        () -> (FunctionIndexQueryExpansionVisitor.expandFunctions(config, metadataHelper, dateIndexHelper, script)));
    }

    protected ASTJexlScript timedApplyNodeTransformRules(QueryStopwatch timers, String timerStage, final ASTJexlScript script, ShardQueryConfiguration config,
                    MetadataHelper metadataHelper, List<NodeTransformRule> transformRules) throws DatawaveQueryException {
        return visitorManager.timedVisit(timers, timerStage, () -> (NodeTransformVisitor.transform(script, transformRules, config, metadataHelper)));
    }

    protected ASTJexlScript timedExpandAnyFieldRegexNodes(QueryStopwatch timers, final ASTJexlScript script, ShardQueryConfiguration config,
                    MetadataHelper metadataHelper, ScannerFactory scannerFactory, String query) throws DatawaveQueryException {
        try {
            config.setIndexedFields(metadataHelper.getIndexedFields(config.getDatatypeFilter()));
            config.setReverseIndexedFields(metadataHelper.getReverseIndexedFields(config.getDatatypeFilter()));

            //  @formatter:off
            return visitorManager.timedVisit(timers, "Expand ANYFIELD Regex Nodes", () -> {
                try {
                    return UnfieldedIndexExpansionVisitor.expandUnfielded(config, scannerFactory, metadataHelper, script);
                } catch (InstantiationException | IllegalAccessException | TableNotFoundException e) {
                    //  rethrow as a datawave query exception because method contracts
                    throw new DatawaveQueryException(e);
                }
            });
            //  @formatter:on

        } catch (EmptyUnfieldedTermExpansionException e) {
            // The visitor will only throw this if we cannot expand anything resulting in empty query
            NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.UNFIELDED_QUERY_ZERO_MATCHES, e, MessageFormat.format("Query: ", query));
            log.info(qe);
            throw new NoResultsException(qe);
        } catch (TableNotFoundException e) {
            QueryException qe = new QueryException(DatawaveErrorCode.METADATA_ACCESS_ERROR, e);
            log.info(qe);
            throw new DatawaveFatalQueryException(qe);
        }
    }

    protected ASTJexlScript timedReduce(QueryStopwatch timers, String timerStage, final ASTJexlScript script) throws DatawaveQueryException {

        // only show pruned sections of the tree's via assignments if debug to reduce runtime when possible
        return visitorManager.timedVisit(timers, timerStage, () -> (ASTJexlScript) QueryPruningVisitor.reduce(script, showReducedQueryPrune));
    }

    protected void timedTestForNonExistentFields(QueryStopwatch timers, final ASTJexlScript script, ShardQueryConfiguration config,
                    MetadataHelper metadataHelper, QueryModel queryModel, Query settings) throws DatawaveQueryException {

        TraceStopwatch stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - Test for Non-Existent Fields");

        // Verify that the query does not contain fields we've never seen
        // before
        Set<String> specialFields = Sets.newHashSet(QueryOptions.DEFAULT_DATATYPE_FIELDNAME, Constants.ANY_FIELD, Constants.NO_FIELD);
        Set<String> nonexistentFields = FieldMissingFromSchemaVisitor.getNonExistentFields(metadataHelper, script, config.getDatatypeFilter(), specialFields);

        if (log.isDebugEnabled()) {
            log.debug("Testing for non-existent fields, found: " + nonexistentFields.size());
        }
        // ensure that all of the fields actually exist in the data dictionary
        Set<String> allFields = null;
        try {
            allFields = metadataHelper.getAllFields(config.getDatatypeFilter());
        } catch (TableNotFoundException e) {
            throw new DatawaveQueryException("Unable get get data dictionary", e);
        }

        // get the unique fields (should already be normalized / uppercased)
        Set<String> fields = config.getUniqueFields().getFields();

        // for the unique fields we need to also look for any model aliases (forward or reverse) and fields generated post evaluation (e.g. HIT_TERM)
        // this is because unique fields operate on the fields as returned to the user. We essentially leave all variants of the fields
        // in the unique field list to ensure we catch everything
        Set<String> uniqueFields = new HashSet<>(allFields);
        if (queryModel != null) {
            uniqueFields.addAll(queryModel.getForwardQueryMapping().keySet());
            uniqueFields.addAll(queryModel.getReverseQueryMapping().values());
        }

        uniqueFields.add(JexlEvaluation.HIT_TERM_FIELD);
        if (!uniqueFields.containsAll(fields)) {
            Set<String> missingFields = Sets.newHashSet(config.getUniqueFields().getFields());
            missingFields.removeAll(uniqueFields);
            nonexistentFields.addAll(missingFields);
        }

        if (!nonexistentFields.isEmpty()) {
            String datatypeFilterSet = (null == config.getDatatypeFilter()) ? "none" : config.getDatatypeFilter().toString();
            if (log.isTraceEnabled()) {
                try {
                    log.trace("current size of fields" + metadataHelper.getAllFields(config.getDatatypeFilter()));
                    log.trace("all fields: " + metadataHelper.getAllFields(config.getDatatypeFilter()));
                } catch (TableNotFoundException e) {
                    log.error("table not found when reading metadata", e);
                }
                log.trace("QueryModel:" + (null == queryModel ? "null" : queryModel));
                log.trace("metadataHelper " + metadataHelper);
            }
            log.trace("QueryModel:" + (null == queryModel ? "null" : queryModel));
            log.trace("metadataHelper " + metadataHelper);

            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.FIELDS_NOT_IN_DATA_DICTIONARY, MessageFormat.format(
                            "Datatype Filter: {0}, Missing Fields: {1}, Auths: {2}", datatypeFilterSet, nonexistentFields, settings.getQueryAuthorizations()));
            log.error(qe);
            throw new InvalidQueryException(qe);
        }

        stopwatch.stop();
    }

    protected ASTJexlScript timedFixUnindexedNumerics(QueryStopwatch timers, final ASTJexlScript script, ShardQueryConfiguration config)
                    throws DatawaveQueryException {
        return visitorManager.timedVisit(timers, "Fix Unindex Numerics", () -> (FixUnindexedNumericTerms.fixNumerics(config, script)));
    }

    protected ASTJexlScript timedExpandMultiNormalizedTerms(QueryStopwatch timers, final ASTJexlScript script, ShardQueryConfiguration config,
                    MetadataHelper metadataHelper) throws DatawaveQueryException {
        return visitorManager.timedVisit(timers, "Expand Query From Normalizers",
                        () -> (ExpandMultiNormalizedTerms.expandTerms(config, metadataHelper, script)));
    }

    protected ASTJexlScript timedMarkIndexHoles(QueryStopwatch timers, final ASTJexlScript script, ShardQueryConfiguration config,
                    MetadataHelper metadataHelper) throws DatawaveQueryException {
        return visitorManager.timedVisit(timers, "Mark Index Holes",
                        () -> (PushdownMissingIndexRangeNodesVisitor.pushdownPredicates(script, config, metadataHelper)));
    }

    protected ASTJexlScript timedExecutableExpansion(QueryStopwatch timers, final ASTJexlScript script, ShardQueryConfiguration config,
                    MetadataHelper metadataHelper) throws DatawaveQueryException {

        // apply distributive property to deal with executability if necessary
        return visitorManager.timedVisit(timers, "Executable Expansion", () -> (ExecutableExpansionVisitor.expand(script, config, metadataHelper)));
    }

    protected ASTJexlScript timedPushdownLowSelectiveTerms(QueryStopwatch timers, ShardQueryConfiguration config) {
        TraceStopwatch stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - Pushdown Low-Selective Terms");

        config.setQueryTree(PushdownLowSelectivityNodesVisitor.pushdownLowSelectiveTerms(config.getQueryTree(), config, metadataHelper));
        if (log.isDebugEnabled()) {
            logQuery(config.getQueryTree(), "Query after pushing down low-selective terms:");
        }

        stopwatch.stop();
        return config.getQueryTree();
    }

    protected ASTJexlScript timedForceFieldToFieldComparison(QueryStopwatch timers, final ASTJexlScript script) throws DatawaveQueryException {
        return visitorManager.timedVisit(timers, "Force Field-to-Field Comparison to Evaluation Only",
                        () -> (FieldToFieldComparisonVisitor.forceEvaluationOnly(script)));
    }

    protected ASTJexlScript timedExpandCompositeFields(QueryStopwatch timers, ASTJexlScript script, ShardQueryConfiguration config)
                    throws DatawaveQueryException {
        try {
            config.setCompositeToFieldMap(metadataHelper.getCompositeToFieldMap(config.getDatatypeFilter()));
            config.setCompositeTransitionDates(metadataHelper.getCompositeTransitionDateMap(config.getDatatypeFilter()));
            config.setCompositeFieldSeparators(metadataHelper.getCompositeFieldSeparatorMap(config.getDatatypeFilter()));
            config.setFieldToDiscreteIndexTypes(CompositeUtils.getFieldToDiscreteIndexTypeMap(config.getQueryFieldsDatatypes()));
        } catch (TableNotFoundException e) {
            QueryException qe = new QueryException(DatawaveErrorCode.METADATA_ACCESS_ERROR, e);
            throw new DatawaveFatalQueryException(qe);
        }
        return visitorManager.timedVisit(timers, "Expand Composite Terms", () -> (ExpandCompositeTerms.expandTerms(config, script)));
    }

    protected ASTJexlScript timedPruneGeoWaveTerms(QueryStopwatch timers, ASTJexlScript script, MetadataHelper metadataHelper) throws DatawaveQueryException {
        Multimap<String,String> prunedTerms = HashMultimap.create();

        ASTJexlScript finalScript = script;
        script = visitorManager.timedVisit(timers, "Prune GeoWave Terms", () -> GeoWavePruningVisitor.pruneTree(finalScript, prunedTerms, metadataHelper));

        if (log.isDebugEnabled()) {
            log.debug("Pruned the following GeoWave terms: ["
                            + prunedTerms.entries().stream().map(x -> x.getKey() + "==" + x.getValue()).collect(Collectors.joining(",")) + "]");
        }
        return script;
    }

    protected ASTJexlScript timedPushFunctions(QueryStopwatch timers, final ASTJexlScript script, ShardQueryConfiguration config, MetadataHelper metadataHelper)
                    throws DatawaveQueryException {
        return visitorManager.timedVisit(timers, "Push Functions into ExceededValue ranges",
                        () -> PushFunctionsIntoExceededValueRanges.pushFunctions(script, metadataHelper, config.getDatatypeFilter()));
    }

    protected boolean timedCheckForIndexOnlyFieldsInQuery(QueryStopwatch timers, String stage, final ASTJexlScript script, ShardQueryConfiguration config,
                    Set<String> indexOnlyFields) {
        boolean containsIndexOnlyFields;
        TraceStopwatch stopwatch = timers.newStartedStopwatch(stage);
        try {
            boolean functionsEnabled = config.isIndexOnlyFilterFunctionsEnabled();
            containsIndexOnlyFields = !SetMembershipVisitor.getMembers(indexOnlyFields, config, script, functionsEnabled).isEmpty();

            // Print the nice log message
            if (log.isDebugEnabled()) {
                logQuery(script, "Computed that the query " + (containsIndexOnlyFields ? " contains " : " does not contain any ") + " index only field(s)");
            }

            config.setContainsIndexOnlyTerms(containsIndexOnlyFields);

        } finally {
            stopwatch.stop();
        }
        return containsIndexOnlyFields;
    }

    protected ASTJexlScript timedExpandRegex(QueryStopwatch timers, String stage, final ASTJexlScript script, ShardQueryConfiguration config,
                    MetadataHelper metadataHelper, ScannerFactory scannerFactory, Map<String,IndexLookup> indexLookupMap) throws DatawaveQueryException {
        return visitorManager.timedVisit(timers, stage, () -> {
            try {
                return RegexIndexExpansionVisitor.expandRegex(config, scannerFactory, metadataHelper, indexLookupMap, script);
            } catch (TableNotFoundException e) {
                throw new DatawaveQueryException("Failed to Expand Ranges", e);
            }
        });
    }

    private ASTJexlScript timedExpandRanges(QueryStopwatch timers, String stage, final ASTJexlScript script, ShardQueryConfiguration config,
                    MetadataHelper metadataHelper, ScannerFactory scannerFactory) throws DatawaveQueryException {
        config.setQueryTree(script);
        TraceStopwatch innerStopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - " + stage);
        try {
            config.setQueryTree(BoundedRangeIndexExpansionVisitor.expandBoundedRanges(config, scannerFactory, metadataHelper, config.getQueryTree()));
        } catch (TableNotFoundException e) {
            throw new DatawaveQueryException("Failed to Expand Ranges", e);
        }

        if (log.isDebugEnabled()) {
            logQuery(config.getQueryTree(), "Query after expanding ranges:");
        }

        innerStopwatch.stop();
        return config.getQueryTree();
    }

    protected ASTJexlScript timedAddDelayedPredicates(QueryStopwatch timers, String stage, final ASTJexlScript script, ShardQueryConfiguration config,
                    MetadataHelper metadataHelper, Set<String> indexedFields, Set<String> indexOnlyFields, Set<String> nonEventFields,
                    LinkedList<String> debugOutput) {
        TraceStopwatch stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - " + stage);
        config.setQueryTree(script);
        if (log.isDebugEnabled()) {
            debugOutput.clear();
        }

        if (!ExecutableDeterminationVisitor.isExecutable(config.getQueryTree(), config, indexedFields, indexOnlyFields, nonEventFields, debugOutput,
                        metadataHelper)) {

            config.setQueryTree((ASTJexlScript) PushdownUnexecutableNodesVisitor.pushdownPredicates(config.getQueryTree(), false, config, indexedFields,
                            indexOnlyFields, nonEventFields, metadataHelper));

            if (log.isDebugEnabled()) {
                logDebug(debugOutput, "Executable state after expanding ranges and regex again:");
                logQuery(config.getQueryTree(), "Query after partially executable pushdown:");
            }
        }

        stopwatch.stop();
        return config.getQueryTree();
    }

    protected ASTJexlScript timedRemoveDelayedPredicates(QueryStopwatch timers, String stage, ASTJexlScript script, ShardQueryConfiguration config,
                    MetadataHelper metadataHelper, Set<String> indexedFields, Set<String> indexOnlyFields, Set<String> nonEventFields,
                    Map<String,IndexLookup> indexLookupMap, ScannerFactory scannerFactory, MetadataHelper helper, List<String> debugOutput)
                    throws TableNotFoundException {

        TraceStopwatch stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - " + stage);

        config.setQueryTree((ASTJexlScript) PullupUnexecutableNodesVisitor.pullupDelayedPredicates(config.getQueryTree(), false, config, indexedFields,
                        indexOnlyFields, nonEventFields, metadataHelper));
        if (log.isDebugEnabled()) {
            logDebug(debugOutput, "Executable state after expanding ranges:");
            logQuery(config.getQueryTree(), "Query after delayed pullup:");
        }

        boolean expandAllTerms = config.isExpandAllTerms();
        // set the expand all terms flag to avoid any more delayed
        // predicates based on cost...
        config.setExpandAllTerms(true);

        // Check if there is any regex to expand after pulling up delayed predicates.
        NodeTypeCount nodeCount = NodeTypeCountVisitor.countNodes(config.getQueryTree(), ASTNRNode.class, ASTERNode.class, BOUNDED_RANGE, ASTFunctionNode.class,
                        EXCEEDED_VALUE);
        if (nodeCount.hasAny(ASTNRNode.class, ASTERNode.class)) {
            config.setQueryTree(RegexIndexExpansionVisitor.expandRegex(config, scannerFactory, helper, indexLookupMap, config.getQueryTree()));
            if (log.isDebugEnabled()) {
                logQuery(config.getQueryTree(), "Query after expanding regex again:");
            }
        }

        // Check if there are any bounded ranges to expand.
        if (nodeCount.isPresent(BOUNDED_RANGE)) {

            try {
                config.setQueryTree(BoundedRangeIndexExpansionVisitor.expandBoundedRanges(config, scannerFactory, metadataHelper, config.getQueryTree()));
            } catch (TableNotFoundException e) {
                QueryException qe = new QueryException(DatawaveErrorCode.METADATA_ACCESS_ERROR, e);
                throw new DatawaveFatalQueryException(qe);
            }

            if (log.isDebugEnabled()) {
                logQuery(config.getQueryTree(), "Query after expanding ranges again:");
            }
        }

        if (reduceQuery) {
            // only show pruned sections of the tree's via assignments if debug to reduce runtime when possible
            config.setQueryTree((ASTJexlScript) QueryPruningVisitor.reduce(config.getQueryTree(), showReducedQueryPrune));

            if (log.isDebugEnabled()) {
                logQuery(config.getQueryTree(), "Query after range expansion reduction again:");
            }
        }

        // Check if there are functions that can be pushed into exceeded value ranges.
        if (nodeCount.isPresent(ASTFunctionNode.class) && nodeCount.isPresent(EXCEEDED_VALUE)) {
            config.setQueryTree(PushFunctionsIntoExceededValueRanges.pushFunctions(config.getQueryTree(), metadataHelper, config.getDatatypeFilter()));
            if (log.isDebugEnabled()) {
                logQuery(config.getQueryTree(), "Query after expanding pushing functions into exceeded value ranges again:");
            }
        }

        // Reset the original expandAllTerms value.
        config.setExpandAllTerms(expandAllTerms);
        stopwatch.stop();
        return config.getQueryTree();
    }

    protected ASTJexlScript timedFixQueryPropertyMarkers(QueryStopwatch timers, ASTJexlScript script) throws DatawaveQueryException {
        // Fix query property markers with multiple sources.
        TraceStopwatch stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - fix query property markers with multiple sources");
        try {
            script = QueryPropertyMarkerSourceConsolidator.consolidate(script);
        } catch (Exception e) {
            throw new DatawaveQueryException("Failed to fix query property markers with multiple sources", e);
        }
        if (log.isDebugEnabled()) {
            logQuery(script, "Query after fixing query property markers with multiple sources");
        }
        stopwatch.stop();
        return script;
    }

    /*
     * End methods that operate on the query tree
     */

    /**
     * Load the metadata information.
     *
     * @param fieldToDatatypeMap
     *            the field mapping
     * @param indexedFields
     *            set of indexed fields
     * @param reverseIndexedFields
     *            reverse indexed fields
     * @param normalizedFields
     *            the normalized fields
     * @param reload
     *            the reload flag
     * @throws AccumuloException
     *             for issues with accumulo
     * @throws AccumuloSecurityException
     *             for accumulo authentication issues
     * @throws TableNotFoundException
     *             if the table is not found
     * @throws ExecutionException
     *             for execuition errors
     * @throws InstantiationException
     *             for issues with instantiation
     * @throws IllegalAccessException
     *             for issues with access
     */
    private void loadDataTypeMetadata(Multimap<String,Type<?>> fieldToDatatypeMap, Set<String> indexedFields, Set<String> reverseIndexedFields,
                    Set<String> normalizedFields, boolean reload) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
                    ExecutionException, InstantiationException, IllegalAccessException {
        synchronized (dataTypeMap) {
            if (!reload && (null != cachedIndexedFields && null != indexedFields) && (null != cachedNormalizedFields && null != normalizedFields)
                            && (null != cachedReverseIndexedFields && null != reverseIndexedFields)) {
                indexedFields.addAll(cachedIndexedFields);
                reverseIndexedFields.addAll(cachedReverseIndexedFields);
                normalizedFields.addAll(cachedNormalizedFields);

                return;
            }

            cachedIndexedFields = metadataHelper.getIndexedFields(null);
            cachedReverseIndexedFields = metadataHelper.getReverseIndexedFields(null);
            cachedNormalizedFields = metadataHelper.getAllNormalized();

            if ((null != cachedIndexedFields && null != indexedFields) && (null != cachedNormalizedFields && null != normalizedFields)
                            && (null != cachedReverseIndexedFields && null != reverseIndexedFields)) {
                indexedFields.addAll(cachedIndexedFields);
                reverseIndexedFields.addAll(cachedReverseIndexedFields);
                normalizedFields.addAll(cachedNormalizedFields);
            }
        }
    }

    protected Set<String> upcase(Set<String> fields) {
        return fields.stream().map(s -> s.toUpperCase()).collect(Collectors.toSet());
    }

    protected ASTJexlScript upperCaseIdentifiers(MetadataHelper metadataHelper, ShardQueryConfiguration config, ASTJexlScript script) {
        GroupFields groupFields = config.getGroupFields();
        if (groupFields != null && groupFields.hasGroupByFields()) {
            groupFields.setMaxFields(upcase(groupFields.getMaxFields()));
            groupFields.setSumFields(upcase(groupFields.getSumFields()));
            groupFields.setGroupByFields(upcase(groupFields.getGroupByFields()));
            groupFields.setAverageFields(upcase(groupFields.getAverageFields()));
            groupFields.setCountFields(upcase(groupFields.getCountFields()));
            groupFields.setMinFields(upcase(groupFields.getMinFields()));

            // If grouping is set, we must make the projection fields match all the group-by fields and aggregation fields.
            config.setProjectFields(groupFields.getProjectionFields());
        } else {
            Set<String> projectFields = config.getProjectFields();

            if (projectFields != null && !projectFields.isEmpty()) {
                config.setProjectFields(upcase(projectFields));
            }
        }

        UniqueFields uniqueFields = config.getUniqueFields();
        if (uniqueFields != null && !uniqueFields.isEmpty()) {
            Sets.newHashSet(uniqueFields.getFields()).stream().forEach(s -> uniqueFields.replace(s, s.toUpperCase()));
        }

        ExcerptFields excerptFields = config.getExcerptFields();
        if (excerptFields != null && !excerptFields.isEmpty()) {
            Sets.newHashSet(excerptFields.getFields()).stream().forEach(s -> excerptFields.replace(s, s.toUpperCase()));
        }

        Set<String> userProjection = config.getRenameFields();
        if (userProjection != null && !userProjection.isEmpty()) {
            config.setRenameFields(upcase(userProjection));
        }

        Set<String> disallowlistedFields = config.getDisallowlistedFields();
        if (disallowlistedFields != null && !disallowlistedFields.isEmpty()) {
            config.setDisallowlistedFields(upcase(disallowlistedFields));
        }

        Set<String> limitFields = config.getLimitFields();
        if (limitFields != null && !limitFields.isEmpty()) {
            config.setLimitFields(upcase(limitFields));
        }

        return (CaseSensitivityVisitor.upperCaseIdentifiers(config, metadataHelper, script));
    }

    // Overwrite projection and disallowlist properties if the query model is
    // being used
    protected ASTJexlScript applyQueryModel(MetadataHelper metadataHelper, ShardQueryConfiguration config, ASTJexlScript script, QueryModel queryModel) {
        // generate the inverse of the reverse mapping; {display field name
        // => db field name}
        // a reverse mapping is always many to one, therefore the inverted
        // reverse mapping
        // can be one to many
        Multimap<String,String> inverseReverseModel = invertMultimap(queryModel.getReverseQueryMapping());

        inverseReverseModel.putAll(queryModel.getForwardQueryMapping());
        Collection<String> projectFields = config.getProjectFields(), disallowlistedFields = config.getDisallowlistedFields(),
                        limitFields = config.getLimitFields();

        if (projectFields != null && !projectFields.isEmpty()) {
            projectFields = queryModel.remapParameter(projectFields, inverseReverseModel);
            if (log.isTraceEnabled()) {
                log.trace("Updated projection set using query model to: " + projectFields);
            }
            config.setProjectFields(Sets.newHashSet(projectFields));
        }

        GroupFields groupFields = config.getGroupFields();
        if (groupFields != null && groupFields.hasGroupByFields()) {
            groupFields.remapFields(inverseReverseModel, queryModel.getReverseQueryMapping());
            if (log.isTraceEnabled()) {
                log.trace("Updating group-by fields using query model to " + groupFields);
            }
            config.setGroupFields(groupFields);

            // If grouping is set, we must make the projection fields match all the group-by fields and aggregation fields.
            config.setProjectFields(groupFields.getProjectionFields());
        }

        UniqueFields uniqueFields = config.getUniqueFields();
        if (uniqueFields != null && !uniqueFields.isEmpty()) {
            uniqueFields.remapFields(inverseReverseModel);
            if (log.isTraceEnabled()) {
                log.trace("Updated unique set using query model to: " + uniqueFields.getFields());
            }
            config.setUniqueFields(uniqueFields);
        }

        ExcerptFields excerptFields = config.getExcerptFields();
        if (excerptFields != null && !excerptFields.isEmpty()) {
            excerptFields.expandFields(inverseReverseModel);
            if (log.isTraceEnabled()) {
                log.trace("Updated excerpt fields using query model to " + excerptFields.getFields());
            }
            config.setExcerptFields(excerptFields);
        }

        Set<String> userProjection = config.getRenameFields();
        if (userProjection != null && !userProjection.isEmpty()) {
            userProjection = Sets.newHashSet(queryModel.remapParameterEquation(userProjection, inverseReverseModel));
            if (log.isTraceEnabled()) {
                log.trace("Updated user projection fields using query model to " + userProjection);
            }
            config.setRenameFields(userProjection);
        }

        if (config.getDisallowlistedFields() != null && !config.getDisallowlistedFields().isEmpty()) {
            disallowlistedFields = queryModel.remapParameter(disallowlistedFields, inverseReverseModel);
            if (log.isTraceEnabled()) {
                log.trace("Updated disallowlist set using query model to: " + disallowlistedFields);
            }
            config.setDisallowlistedFields(Sets.newHashSet(disallowlistedFields));
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
                log.trace("Datatypes: " + builder);
                builder.delete(0, builder.length());

                for (String field : allFields) {
                    if (builder.length() > 0) {
                        builder.append(',');
                    }
                    builder.append(field);
                }
                log.trace("allFields: " + builder);
            }
        } catch (TableNotFoundException e) {
            QueryException qe = new QueryException(DatawaveErrorCode.FIELD_FETCH_ERROR, e);
            log.error(qe);
            throw new DatawaveFatalQueryException(qe);
        }

        return (QueryModelVisitor.applyModel(script, queryModel, allFields, config.getNoExpansionFields(), config.getLenientFields(),
                        config.getStrictFields()));
    }

    /**
     * this is method-injected in QueryLogicFactory.xml to provide a new prototype bean This method's implementation should never be called in production
     *
     * @return the query model provider factory
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
            queryTree = JexlASTHelper.parseAndFlattenJexlQuery(query);
            ValidPatternVisitor.check(queryTree);
            ValidComparisonVisitor.check(queryTree);
        } catch (StackOverflowError soe) {
            if (log.isTraceEnabled()) {
                log.trace("Stack trace for overflow " + soe);
            }
            if (stopwatch != null) {
                stopwatch.stop();
            }
            PreConditionFailedQueryException qe = new PreConditionFailedQueryException(DatawaveErrorCode.QUERY_DEPTH_OR_TERM_THRESHOLD_EXCEEDED, soe);
            log.warn(qe);
            throw new DatawaveFatalQueryException(qe);
        } catch (ParseException e) {
            if (stopwatch != null) {
                stopwatch.stop();
            }
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.UNPARSEABLE_JEXL_QUERY, e, MessageFormat.format("Query: {0}", query));
            log.warn(qe);
            throw new DatawaveFatalQueryException(qe);
        } catch (PatternSyntaxException e) {
            if (stopwatch != null) {
                stopwatch.stop();
            }
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

    public static void logTrace(List<String> output, String message) {
        if (log.isTraceEnabled()) {
            log.trace(message);
            for (String line : output) {
                log.trace(line);
            }
            log.trace("");
        }
    }

    public static void logQuery(final ASTJexlScript queryTree, String message) {
        if (log.isTraceEnabled()) {
            logTrace(PrintingVisitor.formattedQueryStringList(queryTree, maxChildNodesToPrint, maxTermsToPrint), message);
        } else if (log.isDebugEnabled()) {
            logDebug(PrintingVisitor.formattedQueryStringList(queryTree, maxChildNodesToPrint, maxTermsToPrint), message);
        }
    }

    /**
     * Adding date filters if the query parameters specify that the dates are to be other than the default
     *
     * @param queryTree
     *            the query tree
     * @param scannerFactory
     *            the scanner factory
     * @param metadataHelper
     *            the metadata helper
     * @param config
     *            a config
     * @param dateIndexHelper
     *            the date index helper
     * @param settings
     *            the query settings
     * @return the updated query tree
     * @throws TableNotFoundException
     *             if the table is not found
     * @throws DatawaveQueryException
     *             for issues with running the query
     */
    public ASTJexlScript addDateFilters(final ASTJexlScript queryTree, ScannerFactory scannerFactory, MetadataHelper metadataHelper,
                    DateIndexHelper dateIndexHelper, ShardQueryConfiguration config, Query settings) throws TableNotFoundException, DatawaveQueryException {
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
                log.warn("The specified date type: " + dateType + " is unknown for the specified data types");
                // If this is the case, then essentially we have no dates to search. Adding the filter function with _NO_FIELD_ will have the desired effect.
                // Also it will be understandable from the plan as to why no results were returned.
                dateIndexData.getFields().add(Constants.NO_FIELD);
            }
            log.info("Adding date filters for the following fields: " + dateIndexData.getFields());
            // now for each field, add an expression to filter that date
            List<JexlNode> andChildren = new ArrayList<>();
            for (int i = 0; i < queryTree.jjtGetNumChildren(); i++) {
                if (queryTree.jjtGetChild(i) instanceof ASTAndNode) {
                    andChildren.add(queryTree.jjtGetChild(i));
                } else {
                    andChildren.add(JexlNodeFactory.createExpression(queryTree.jjtGetChild(i)));
                }
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
     *            the config
     * @throws DatawaveQueryException
     *             if there is a query error
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
     *            the date type
     * @param field
     *            the field
     * @param begin
     *            begin date range
     * @param end
     *            the end date range
     * @return the jexl node
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
     * @see PushDownPlanner#rewriteQuery( org.apache.commons.jexl3.parser.ASTJexlScript)
     */
    @Override
    public ASTJexlScript applyRules(final ASTJexlScript queryTree, ScannerFactory scannerFactory, MetadataHelper metadataHelper,
                    ShardQueryConfiguration config) {

        // The PushDownVisitor will decide what nodes are "delayed" in that they
        // do NOT
        // require a global index lookup due to cost or other reasons as defined
        // by the configured rules.
        PushDownVisitor pushDownPlanner = new PushDownVisitor(config, scannerFactory, metadataHelper, rules);

        return pushDownPlanner.applyRules(queryTree);
    }

    /**
     * Extend to further configure QueryIterator
     *
     * @param config
     *            the config
     * @param cfg
     *            the iterator configuration
     */
    protected void configureAdditionalOptions(ShardQueryConfiguration config, IteratorSetting cfg) {
        // no-op
    }

    protected Future<IteratorSetting> loadQueryIterator(final MetadataHelper metadataHelper, final ShardQueryConfiguration config, final Query settings,
                    final String queryString, final Boolean isFullTable, boolean isPreload) throws DatawaveQueryException {

        return builderThread.submit(() -> {
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
                addOption(cfg, QueryOptions.STATSD_HOST_COLON_PORT, config.getStatsdHost() + ':' + Integer.toString(config.getStatsdPort()), false);
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
            if (config.getIvaratorCacheDirConfigs() != null && !config.getIvaratorCacheDirConfigs().isEmpty()) {
                addOption(cfg, QueryOptions.IVARATOR_CACHE_DIR_CONFIG, IvaratorCacheDirConfig.toJson(getShuffledIvaratoCacheDirConfigs(config)), false);
            }
            addOption(cfg, QueryOptions.IVARATOR_CACHE_BUFFER_SIZE, Integer.toString(config.getIvaratorCacheBufferSize()), false);
            addOption(cfg, QueryOptions.IVARATOR_SCAN_PERSIST_THRESHOLD, Long.toString(config.getIvaratorCacheScanPersistThreshold()), false);
            addOption(cfg, QueryOptions.IVARATOR_SCAN_TIMEOUT, Long.toString(config.getIvaratorCacheScanTimeout()), false);
            addOption(cfg, QueryOptions.COLLECT_TIMING_DETAILS, Boolean.toString(config.getCollectTimingDetails()), false);
            addOption(cfg, QueryOptions.MAX_INDEX_RANGE_SPLIT, Integer.toString(config.getMaxFieldIndexRangeSplit()), false);
            addOption(cfg, QueryOptions.MAX_IVARATOR_OPEN_FILES, Integer.toString(config.getIvaratorMaxOpenFiles()), false);
            addOption(cfg, QueryOptions.MAX_IVARATOR_RESULTS, Long.toString(config.getMaxIvaratorResults()), false);
            addOption(cfg, QueryOptions.IVARATOR_NUM_RETRIES, Integer.toString(config.getIvaratorNumRetries()), false);
            addOption(cfg, QueryOptions.IVARATOR_PERSIST_VERIFY, Boolean.toString(config.isIvaratorPersistVerify()), false);
            addOption(cfg, QueryOptions.IVARATOR_PERSIST_VERIFY_COUNT, Integer.toString(config.getIvaratorPersistVerifyCount()), false);
            addOption(cfg, QueryOptions.MAX_EVALUATION_PIPELINES, Integer.toString(config.getMaxEvaluationPipelines()), false);
            addOption(cfg, QueryOptions.MAX_PIPELINE_CACHED_RESULTS, Integer.toString(config.getMaxPipelineCachedResults()), false);
            addOption(cfg, QueryOptions.MAX_IVARATOR_SOURCES, Integer.toString(config.getMaxIvaratorSources()), false);
            addOption(cfg, QueryOptions.MAX_IVARATOR_SOURCE_WAIT, Long.toString(config.getMaxIvaratorSourceWait()), false);

            if (config.getYieldThresholdMs() != Long.MAX_VALUE && config.getYieldThresholdMs() > 0) {
                addOption(cfg, QueryOptions.YIELD_THRESHOLD_MS, Long.toString(config.getYieldThresholdMs()), false);
            }

            addOption(cfg, QueryOptions.SORTED_UIDS, Boolean.toString(config.isSortedUIDs()), false);

            configureTypeMappings(config, cfg, metadataHelper, getCompressOptionMappings(), isPreload);
            configureAdditionalOptions(config, cfg);

            loadFields(cfg, config, isPreload);
            configureSeekingOptions(cfg, config);

            try {
                CompositeMetadata compositeMetadata = metadataHelper.getCompositeMetadata().filter(config.getQueryFieldsDatatypes().keySet());
                if (compositeMetadata != null && !compositeMetadata.isEmpty()) {
                    addOption(cfg, QueryOptions.COMPOSITE_METADATA, java.util.Base64.getEncoder().encodeToString(CompositeMetadata.toBytes(compositeMetadata)),
                                    false);
                }
            } catch (TableNotFoundException e) {
                QueryException qe = new QueryException(DatawaveErrorCode.COMPOSITE_METADATA_CONFIG_ERROR, e);
                throw new DatawaveQueryException(qe);
            }

            String datatypeFilter = config.getDatatypeFilterAsString();

            addOption(cfg, QueryOptions.DATATYPE_FILTER, datatypeFilter, false);

            try {
                addOption(cfg, QueryOptions.CONTENT_EXPANSION_FIELDS, Joiner.on(',').join(metadataHelper.getContentFields(config.getDatatypeFilter())), false);
            } catch (TableNotFoundException e) {
                QueryException qe = new QueryException(DatawaveErrorCode.CONTENT_FIELDS_RETRIEVAL_ERROR, e);
                throw new DatawaveQueryException(qe);
            }

            if (config.isDebugMultithreadedSources()) {
                addOption(cfg, QueryOptions.DEBUG_MULTITHREADED_SOURCES, Boolean.toString(config.isDebugMultithreadedSources()), false);
            }

            if (config.isLimitFieldsPreQueryEvaluation()) {
                addOption(cfg, QueryOptions.LIMIT_FIELDS_PRE_QUERY_EVALUATION, Boolean.toString(config.isLimitFieldsPreQueryEvaluation()), false);
            }

            if (config.getLimitFieldsField() != null) {
                addOption(cfg, QueryOptions.LIMIT_FIELDS_FIELD, config.getLimitFieldsField(), false);
            }

            return cfg;
        });
    }

    /**
     * Load indexed, index only, and composite fields into the query iterator
     *
     * @param cfg
     *            iterator setting
     * @param config
     *            shard query config
     * @param isPreload
     *            boolean indicating if this method is called prior to query planning
     * @throws DatawaveQueryException
     *             if the metadata helper cannot get the fields
     */
    private void loadFields(IteratorSetting cfg, ShardQueryConfiguration config, boolean isPreload) throws DatawaveQueryException {
        try {
            Set<String> compositeFields = metadataHelper.getCompositeToFieldMap(config.getDatatypeFilter()).keySet();
            Set<String> indexedFields = metadataHelper.getIndexedFields(config.getDatatypeFilter());
            Set<String> indexOnlyFields = metadataHelper.getIndexOnlyFields(config.getDatatypeFilter());

            // only reduce the query fields if planning has occurred
            if (!isPreload && config.getReduceQueryFields()) {
                Set<String> queryFields = ReduceFields.getQueryFields(config.getQueryTree());
                indexedFields = ReduceFields.intersectFields(queryFields, indexedFields);
                indexOnlyFields = ReduceFields.intersectFields(queryFields, indexOnlyFields);
                compositeFields = ReduceFields.intersectFields(queryFields, compositeFields);
            }

            addOption(cfg, QueryOptions.COMPOSITE_FIELDS, QueryOptions.buildFieldStringFromSet(compositeFields), true);
            addOption(cfg, QueryOptions.INDEXED_FIELDS, QueryOptions.buildFieldStringFromSet(indexedFields), true);
            addOption(cfg, QueryOptions.INDEX_ONLY_FIELDS, QueryOptions.buildFieldStringFromSet(indexOnlyFields), true);

        } catch (TableNotFoundException e) {
            QueryException qe = new QueryException(DatawaveErrorCode.INDEX_ONLY_FIELDS_RETRIEVAL_ERROR, e);
            throw new DatawaveQueryException(qe);
        }
    }

    /**
     * Configure options related to seek thresholds
     *
     * @param cfg
     *            iterator setting
     * @param config
     *            shard query config
     */
    protected void configureSeekingOptions(IteratorSetting cfg, ShardQueryConfiguration config) {
        if (config.getFiFieldSeek() > 0) {
            addOption(cfg, QueryOptions.FI_FIELD_SEEK, String.valueOf(config.getFiFieldSeek()), false);
        }
        if (config.getFiNextSeek() > 0) {
            addOption(cfg, QueryOptions.FI_NEXT_SEEK, String.valueOf(config.getFiNextSeek()), false);
        }
        if (config.getEventFieldSeek() > 0) {
            addOption(cfg, QueryOptions.EVENT_FIELD_SEEK, String.valueOf(config.getEventFieldSeek()), false);
        }
        if (config.getEventNextSeek() > 0) {
            addOption(cfg, QueryOptions.EVENT_NEXT_SEEK, String.valueOf(config.getEventNextSeek()), false);
        }
        if (config.getTfFieldSeek() > 0) {
            addOption(cfg, QueryOptions.TF_FIELD_SEEK, String.valueOf(config.getTfFieldSeek()), false);
        }
        if (config.getTfNextSeek() > 0) {
            addOption(cfg, QueryOptions.TF_NEXT_SEEK, String.valueOf(config.getTfNextSeek()), false);
        }

        if (config.isSeekingEventAggregation()) {
            addOption(cfg, QueryOptions.SEEKING_EVENT_AGGREGATION, String.valueOf(config.isSeekingEventAggregation()), false);
        }
    }

    /**
     * Get the list of ivarator cache dirs, randomizing the order (while respecting priority) so that the tservers spread out the disk usage.
     *
     * @param config
     *            the shard config
     * @return a list of ivarator cache dirs
     */
    private List<IvaratorCacheDirConfig> getShuffledIvaratoCacheDirConfigs(ShardQueryConfiguration config) {
        List<IvaratorCacheDirConfig> shuffledIvaratorCacheDirs = new ArrayList<>();

        // group the ivarator cache dirs by their priority
        Map<Integer,List<IvaratorCacheDirConfig>> groupedConfigs = config.getIvaratorCacheDirConfigs().stream()
                        .collect(Collectors.groupingBy(IvaratorCacheDirConfig::getPriority));

        // iterate over the sorted priorities, and shuffle the subsets
        for (Integer priority : new TreeSet<>(groupedConfigs.keySet())) {
            List<IvaratorCacheDirConfig> cacheDirs = groupedConfigs.get(priority);
            Collections.shuffle(cacheDirs);
            shuffledIvaratorCacheDirs.addAll(cacheDirs);
        }

        return shuffledIvaratorCacheDirs;
    }

    /**
     * Get the loaded {@link IteratorSetting}
     *
     * @param metadataHelper
     *            the {@link MetadataHelper}
     * @param config
     *            the {@link ShardQueryConfiguration}
     * @param settings
     *            the {@link Query}
     * @param queryString
     *            the raw query string
     * @param isFullTable
     *            a flag indicating if this is a full table scan
     * @param isPreload
     *            a flag indicating the iterator is being loaded prior to planning
     * @return a loaded {@link IteratorSetting}
     * @throws DatawaveQueryException
     *             if something goes wrong
     */
    protected IteratorSetting getQueryIterator(MetadataHelper metadataHelper, ShardQueryConfiguration config, Query settings, String queryString,
                    Boolean isFullTable, boolean isPreload) throws DatawaveQueryException {
        if (null == settingFuture)
            settingFuture = loadQueryIterator(metadataHelper, config, settings, queryString, isFullTable, isPreload);
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
        configureTypeMappings(config, cfg, metadataHelper, compressMappings, false);
    }

    public static void configureTypeMappings(ShardQueryConfiguration config, IteratorSetting cfg, MetadataHelper metadataHelper, boolean compressMappings,
                    boolean isPreload) throws DatawaveQueryException {
        try {
            addOption(cfg, QueryOptions.QUERY_MAPPING_COMPRESS, Boolean.toString(compressMappings), false);

            // now lets filter the query field datatypes to those that are not
            // indexed
            Multimap<String,Type<?>> nonIndexedQueryFieldsDatatypes = HashMultimap.create(config.getQueryFieldsDatatypes());
            nonIndexedQueryFieldsDatatypes.keySet().removeAll(config.getIndexedFields());

            String nonIndexedTypes = QueryOptions.buildFieldNormalizerString(nonIndexedQueryFieldsDatatypes);
            String requiredAuthsString = metadataHelper.getUsersMetadataAuthorizationSubset();

            TypeMetadata typeMetadata = metadataHelper.getTypeMetadata(config.getDatatypeFilter());

            if (config.getReduceTypeMetadata() && !isPreload) {
                Set<String> fieldsToRetain = ReduceFields.getQueryFields(config.getQueryTree());
                typeMetadata = typeMetadata.reduce(fieldsToRetain);
            }

            String serializedTypeMetadata = typeMetadata.toString();

            if (compressMappings) {
                nonIndexedTypes = QueryOptions.compressOption(nonIndexedTypes, QueryOptions.UTF8);
                requiredAuthsString = QueryOptions.compressOption(requiredAuthsString, QueryOptions.UTF8);
                if (!config.getReduceTypeMetadataPerShard()) {
                    // if we're reducing later, don't compress the type metadata
                    serializedTypeMetadata = QueryOptions.compressOption(serializedTypeMetadata, QueryOptions.UTF8);
                }
            }
            addOption(cfg, QueryOptions.NON_INDEXED_DATATYPES, nonIndexedTypes, false);
            addOption(cfg, QueryOptions.TYPE_METADATA, serializedTypeMetadata, false);
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
     *            the config
     * @param cfg
     *            the iterator configuration
     * @throws DatawaveQueryException
     *             for issues with running the query
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

        // Allowlist and disallowlist projection are mutually exclusive. You can't
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
        } else if (null != config.getDisallowlistedFields() && !config.getDisallowlistedFields().isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("Setting scan option: " + QueryOptions.DISALLOWLISTED_FIELDS + " to " + config.getDisallowlistedFieldsAsString());
            }

            addOption(cfg, QueryOptions.DISALLOWLISTED_FIELDS, config.getDisallowlistedFieldsAsString(), false);
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

        // if groupby function is used, force include.grouping.context to be true
        if (config.getGroupFields() != null && config.getGroupFields().hasGroupByFields()) {
            addOption(cfg, QueryOptions.INCLUDE_GROUPING_CONTEXT, Boolean.toString(true), false);
        } else {
            addOption(cfg, QueryOptions.INCLUDE_GROUPING_CONTEXT, Boolean.toString(config.getIncludeGroupingContext()), false);
        }

        addOption(cfg, QueryOptions.REDUCED_RESPONSE, Boolean.toString(config.isReducedResponse()), false);
        addOption(cfg, QueryOptions.DISABLE_EVALUATION, Boolean.toString(config.isDisableEvaluation()), false);
        addOption(cfg, QueryOptions.DISABLE_DOCUMENTS_WITHOUT_EVENTS, Boolean.toString(config.isDisableIndexOnlyDocuments()), false);
        addOption(cfg, QueryOptions.CONTAINS_INDEX_ONLY_TERMS, Boolean.toString(config.isContainsIndexOnlyTerms()), false);
        addOption(cfg, QueryOptions.CONTAINS_COMPOSITE_TERMS, Boolean.toString(config.isContainsCompositeTerms()), false);
        addOption(cfg, QueryOptions.ALLOW_FIELD_INDEX_EVALUATION, Boolean.toString(config.isAllowFieldIndexEvaluation()), false);
        addOption(cfg, QueryOptions.ALLOW_TERM_FREQUENCY_LOOKUP, Boolean.toString(config.isAllowTermFrequencyLookup()), false);
        addOption(cfg, QueryOptions.COMPRESS_SERVER_SIDE_RESULTS, Boolean.toString(config.isCompressServerSideResults()), false);
    }

    /**
     * Performs a lookup in the global index / reverse index and returns a {@link CloseableIterable} of QueryPlans
     *
     * @param config
     *            the shard query configuration
     * @param queryTree
     *            the query tree
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

        Range range = new Range(startKey, true, endKey, false);

        if (log.isTraceEnabled()) {
            log.trace("Produced range is " + range);
        }

        //  @formatter:off
        QueryPlan queryPlan = new QueryPlan()
                        .withTableName(config.getShardTableName())
                        .withQueryTree(queryTree)
                        .withRanges(Collections.singleton(range));
        //  @formatter:on

        return new CloseableListIterable<>(Collections.singletonList(queryPlan));
    }

    /**
     * Returns a ImmutablePair&lt;Iterable&lt;Range&gt;,Boolean&gt; whose elements represent the Ranges to use for querying the shard table and whether or not
     * this is a "full-table-scan" query.
     *
     * @param scannerFactory
     *            the scanner factory
     * @param metadataHelper
     *            the metadata helper
     * @param config
     *            the shard configuration
     * @param queryTree
     *            the query tree
     * @return the query range tuples
     * @throws DatawaveQueryException
     *             for issues with the query
     */
    public Tuple2<CloseableIterable<QueryPlan>,Boolean> getQueryRanges(ScannerFactory scannerFactory, MetadataHelper metadataHelper,
                    ShardQueryConfiguration config, JexlNode queryTree) throws DatawaveQueryException {
        Preconditions.checkNotNull(queryTree);

        boolean needsFullTable = false;
        String fullTableScanReason = null;
        CloseableIterable<QueryPlan> ranges = null;

        // if the query has already been reduced to false there is no reason to do more
        if (QueryPruningVisitor.getState(queryTree) == QueryPruningVisitor.TruthState.FALSE) {
            return new Tuple2<>(emptyCloseableIterator(), false);
        }

        // if we still have an unexecutable tree, then a full table scan is
        // required
        LinkedList<String> debugOutput = null;
        if (log.isDebugEnabled()) {
            debugOutput = new LinkedList<>();
        }
        ExecutableDeterminationVisitor.StateAndReason state = ExecutableDeterminationVisitor.getStateAndReason(queryTree, config, metadataHelper, debugOutput);
        if (log.isDebugEnabled()) {
            logDebug(debugOutput, "ExecutableDeterminationVisitor at getQueryRanges:");
        }

        if (state.state != STATE.EXECUTABLE) {
            if (state.state == STATE.ERROR) {
                log.warn("After expanding the query, it is determined that the query cannot be executed due to index-only fields mixed with expressions that cannot be run against the index.");
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INDEX_ONLY_FIELDS_MIXED_INVALID_EXPRESSIONS, state.reason);
                throw new InvalidQueryException(qe);
            }
            log.warn("After expanding the query, it is determined that the query cannot be executed against the field index and a full table scan is required");
            needsFullTable = true;
            fullTableScanReason = state.reason;
        }

        // optionally build/rebuild the datatype filter with the fully planned query
        if (config.isRebuildDatatypeFilter()) {
            Set<String> ingestTypes = IngestTypeVisitor.getIngestTypes(config.getQueryTree(), getTypeMetadata());

            if (ingestTypes.contains(IngestTypeVisitor.UNKNOWN_TYPE) || ingestTypes.contains(IngestTypeVisitor.IGNORED_TYPE)) {
                // could not reduce ingest types based on the query structure, do nothing
            } else if (config.getDatatypeFilter().isEmpty()) {
                // if no filter specified, build and set filter from query fields
                config.setDatatypeFilter(ingestTypes);
            } else {
                Set<String> parameterTypes = config.getDatatypeFilter();
                Set<String> intersectedTypes = Sets.intersection(ingestTypes, parameterTypes);

                if (intersectedTypes.isEmpty()) {
                    throw new DatawaveQueryException("User requested datatypes did not overlap with query fields");
                }

                // only update filter if it is smaller
                if (intersectedTypes.size() < parameterTypes.size()) {
                    config.setDatatypeFilter(intersectedTypes);
                }
            }
        }

        // only reduce datatype filter if not rebuilding and there's a filter to reduce
        if (!config.getDatatypeFilter().isEmpty() && !config.isRebuildDatatypeFilter() && config.getReduceIngestTypes()) {
            Set<String> parameterTypes = config.getDatatypeFilter();
            Set<String> ingestTypes = IngestTypeVisitor.getIngestTypes(queryTree, getTypeMetadata());

            if (!ingestTypes.contains(IngestTypeVisitor.UNKNOWN_TYPE)) {
                Set<String> intersectedTypes = Sets.intersection(ingestTypes, parameterTypes);

                if (intersectedTypes.isEmpty()) {
                    throw new DatawaveQueryException("User requested datatypes did not overlap with query fields");
                }

                // only update filter if it is smaller
                if (intersectedTypes.size() < parameterTypes.size()) {
                    config.setDatatypeFilter(intersectedTypes);
                }
            }
        }

        // prune query by ingest types
        if (config.getPruneQueryByIngestTypes()) {
            JexlNode pruned = IngestTypePruningVisitor.prune(RebuildingVisitor.copy(queryTree), getTypeMetadata());

            if (config.getFullTableScanEnabled() || ExecutableDeterminationVisitor.isExecutable(pruned, config, metadataHelper)) {
                // always update the query for full table scans or in cases where the query is still executable
                queryTree = pruned;
                config.setQueryTree((ASTJexlScript) pruned);

                Set<String> types = IngestTypeVisitor.getIngestTypes(pruned, getTypeMetadata());
                if (!types.contains(IngestTypeVisitor.UNKNOWN_TYPE)) {
                    if (types.isEmpty()) {
                        throw new DatawaveQueryException("User requested datatypes did not overlap with query fields");
                    } else if (config.getDatatypeFilter().isEmpty() || (types.size() < config.getDatatypeFilter().size())) {
                        config.setDatatypeFilter(types);
                    }
                }
            } else {
                throw new DatawaveFatalQueryException("Check query for mutually exclusive ingest types, query was non-executable after pruning by ingest type");
            }
        }

        if (config.isSortQueryPreIndexWithFieldCounts()) {
            config.setQueryTree(timedSortQueryBeforeGlobalIndex(config, getMetadataHelper()));
        } else if (config.isSortQueryPreIndexWithImpliedCounts()) {
            config.setQueryTree(timedSortQueryBeforeGlobalIndex(config));
        }

        // if a simple examination of the query has not forced a full table
        // scan, then lets try to compute ranges
        if (!needsFullTable) {

            // count the terms
            int termCount = TermCountingVisitor.countTerms(queryTree);

            if (config.getIntermediateMaxTermThreshold() > 0 && termCount > config.getIntermediateMaxTermThreshold()) {
                throw new DatawaveFatalQueryException(
                                "Query with " + termCount + " exceeds the initial max term threshold of " + config.getIntermediateMaxTermThreshold());
            }

            if (config.getIndexedMaxTermThreshold() > 0) {
                int indexedEqualityTerms = IndexedTermCountingVisitor.countTerms(config.getQueryTree(), config.getIndexedFields());
                if (indexedEqualityTerms > config.getIndexedMaxTermThreshold()) {
                    throw new DatawaveQueryException("Query with " + indexedEqualityTerms + " indexed EQ nodes exceeds the indexedMaxTermThreshold of "
                                    + config.getIndexedMaxTermThreshold());
                }
            }

            if (termCount >= pushdownThreshold) {
                if (log.isTraceEnabled()) {
                    log.trace("pushing down query because it has " + termCount + " when our max is " + pushdownThreshold);
                }
                config.setCollapseUids(true);
            }
            TraceStopwatch stopwatch = config.getTimers().newStartedStopwatch("DefaultQueryPlanner - Begin stream of ranges from inverted index");

            RangeStream stream = initializeRangeStream(config, scannerFactory, metadataHelper);

            ranges = stream.streamPlans(queryTree);

            if (log.isTraceEnabled()) {
                log.trace("query stream is " + stream.context());
            }

            switch (stream.context()) {
                case EXCEEDED_TERM_THRESHOLD:
                    // throw an unsupported exception if the planner cannot handle term threshold exceeded
                    if (!config.canHandleExceededTermThreshold()) {
                        throw new UnsupportedOperationException(EXCEED_TERM_EXPANSION_ERROR);
                    }
                    break;
                case UNINDEXED:
                    if (log.isDebugEnabled()) {
                        log.debug("Full table scan required because of unindexed fields");
                    }
                    needsFullTable = true;
                    break;
                case DELAYED_FIELD:
                    if (log.isDebugEnabled()) {
                        log.debug("Full table scan required because query consists of only delayed expressions");
                    }
                    needsFullTable = true;
                    break;
                default:
                    // the context is good and does not prevent a query from executing
            }

            // check for the case where we cannot handle an ivarator but the query requires an ivarator
            if (IvaratorRequiredVisitor.isIvaratorRequired(queryTree) && !config.canHandleExceededValueThreshold()) {
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

                QueryException qe = new QueryException(DatawaveErrorCode.FULL_TABLE_SCAN_REQUIRED_BUT_DISABLED, fullTableScanReason);
                throw new FullTableScansDisallowedException(qe);
            }
            if (log.isTraceEnabled())
                log.trace("Ranges are " + ranges);
        }

        return new Tuple2<>(ranges, needsFullTable);
    }

    protected ASTJexlScript timedSortQueryBeforeGlobalIndex(ShardQueryConfiguration config, MetadataHelper metadataHelper) throws DatawaveQueryException {
        return visitorManager.timedVisit(config.getTimers(), "SortQueryBeforeGlobalIndex", () -> {
            Set<String> fields = QueryFieldsVisitor.parseQueryFields(config.getQueryTree(), getMetadataHelper());
            if (!fields.isEmpty()) {
                Set<String> datatypes = config.getDatatypeFilter();
                Map<String,Long> counts = metadataHelper.getCountsForFieldsInDateRange(fields, datatypes, config.getBeginDate(), config.getEndDate());
                if (!counts.isEmpty()) {
                    return OrderByCostVisitor.orderByFieldCount(config.getQueryTree(), counts);
                } else {
                    // fall back to sorting by implied cardinality
                    return OrderByCostVisitor.order(config.getQueryTree());
                }
            }
            return config.getQueryTree();
        });
    }

    protected ASTJexlScript timedSortQueryBeforeGlobalIndex(ShardQueryConfiguration config) throws DatawaveQueryException {
        return visitorManager.timedVisit(config.getTimers(), "SortQueryBeforeGlobalIndex", () -> {
            // sort by implied cardinality
            return OrderByCostVisitor.order(config.getQueryTree());
        });
    }

    private TypeMetadata getTypeMetadata() {
        try {
            return metadataHelper.getTypeMetadata();
        } catch (TableNotFoundException e) {
            throw new DatawaveFatalQueryException("Could not get TypeMetadata");
        }
    }

    /**
     * Initializes the range stream, whether it is configured to be a different class than the Default Range stream or not.
     *
     * @param config
     *            the shard configuration
     * @param scannerFactory
     *            the scanner factory
     * @param metadataHelper
     *            the metadata helper
     * @return the range stream
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
     *            setting for disabling bounded lookup
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

    public List<NodeTransformRule> getTransformRules() {
        return Collections.unmodifiableList(transformRules);
    }

    public void setTransformRules(List<NodeTransformRule> transformRules) {
        this.transformRules.addAll(transformRules);
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

    public void setPlannedScript(String plannedScript) {
        this.plannedScript = plannedScript;
    }

    protected Multimap<String,Type<?>> configureIndexedAndNormalizedFields(MetadataHelper metadataHelper, ShardQueryConfiguration config,
                    ASTJexlScript queryTree) throws DatawaveQueryException {
        // Fetch the mapping of fields to Types from the DatawaveMetadata table

        Multimap<String,Type<?>> fieldToDatatypeMap = FetchDataTypesVisitor.fetchDataTypes(metadataHelper, config.getDatatypeFilter(), queryTree, false);

        try {
            return configureIndexedAndNormalizedFields(fieldToDatatypeMap, metadataHelper.getIndexedFields(null), metadataHelper.getReverseIndexedFields(null),
                            metadataHelper.getAllNormalized(), config, queryTree);
        } catch (InstantiationException | IllegalAccessException | TableNotFoundException e) {
            throw new DatawaveFatalQueryException(e);
        }

    }

    protected void setCachedFields(Set<String> indexedFields, Set<String> reverseIndexedFields, Multimap<String,Type<?>> queryFieldMap,
                    Multimap<String,Type<?>> normalizedFieldMap, ShardQueryConfiguration config) {
        config.setIndexedFields(indexedFields);
        config.setReverseIndexedFields(reverseIndexedFields);
        config.setQueryFieldsDatatypes(queryFieldMap);
        config.setNormalizedFieldsDatatypes(normalizedFieldMap);
    }

    protected Multimap<String,Type<?>> configureIndexedAndNormalizedFields(Multimap<String,Type<?>> fieldToDatatypeMap, Set<String> indexedFields,
                    Set<String> reverseIndexedFields, Set<String> normalizedFields, ShardQueryConfiguration config, ASTJexlScript queryTree)
                    throws DatawaveQueryException, TableNotFoundException, InstantiationException, IllegalAccessException {
        log.debug("config.getDatatypeFilter() = " + config.getDatatypeFilter());
        log.debug("fieldToDatatypeMap.keySet() is " + fieldToDatatypeMap.keySet());

        config.setIndexedFields(indexedFields);
        config.setReverseIndexedFields(reverseIndexedFields);

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

    public void setDisableWhindexFieldMappings(boolean disableWhindexFieldMappings) {
        this.disableWhindexFieldMappings = disableWhindexFieldMappings;
    }

    public boolean getDisableWhindexFieldMappings() {
        return disableWhindexFieldMappings;
    }

    public void setDisableExpandIndexFunction(boolean disableExpandIndexFunction) {
        this.disableExpandIndexFunction = disableExpandIndexFunction;
    }

    public boolean getDisableExpandIndexFunction() {
        return disableExpandIndexFunction;
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

    public boolean getExecutableExpansion() {
        return executableExpansion;
    }

    public void setExecutableExpansion(boolean executableExpansion) {
        this.executableExpansion = executableExpansion;
    }

    public void setReduceQuery(boolean reduceQuery) {
        this.reduceQuery = reduceQuery;
    }

    public boolean isReduceQuery() {
        return reduceQuery;
    }

    public void setShowReducedQueryPrune(boolean showReducedQueryPrune) {
        this.showReducedQueryPrune = showReducedQueryPrune;
    }

    public boolean isShowReducedQueryPrune() {
        return showReducedQueryPrune;
    }

    public TimedVisitorManager getVisitorManager() {
        return visitorManager;
    }

    public void setVisitorManager(TimedVisitorManager visitorManager) {
        this.visitorManager = visitorManager;
    }

    public static int getMaxChildNodesToPrint() {
        return maxChildNodesToPrint;
    }

    public static void setMaxChildNodesToPrint(int maxChildNodesToPrint) {
        DefaultQueryPlanner.maxChildNodesToPrint = maxChildNodesToPrint;
    }

    public static int getMaxTermsToPrint() {
        return maxTermsToPrint;
    }

    public static void setMaxTermsToPrint(int maxTermsToPrint) {
        DefaultQueryPlanner.maxTermsToPrint = maxTermsToPrint;
    }

    /**
     * Given a date, truncate it to year, month, date and increment the day by one to determine the following day.
     *
     * @param endDate
     *            the end date
     * @return the date incremented by one
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
