package datawave.query.tables;

import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import datawave.data.type.NoOpType;
import datawave.data.type.Type;
import datawave.marking.MarkingFunctions;
import datawave.query.CloseableIterable;
import datawave.query.Constants;
import datawave.query.DocumentSerialization;
import datawave.query.QueryParameters;
import datawave.query.cardinality.CardinalityConfiguration;
import datawave.query.config.IndexHole;
import datawave.query.config.Profile;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.config.ShardQueryConfigurationFactory;
import datawave.query.enrich.DataEnricher;
import datawave.query.enrich.EnrichingMaster;
import datawave.query.index.lookup.CreateUidsIterator;
import datawave.query.index.lookup.IndexInfo;
import datawave.query.index.lookup.UidIntersector;
import datawave.query.iterator.QueryOptions;
import datawave.query.language.parser.ParseException;
import datawave.query.language.parser.QueryParser;
import datawave.query.language.tree.QueryNode;
import datawave.query.model.QueryModel;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.planner.MetadataHelperQueryModelProvider;
import datawave.query.planner.QueryModelProvider;
import datawave.query.planner.QueryPlanner;
import datawave.query.planner.pushdown.rules.PushDownRule;
import datawave.query.scheduler.PushdownScheduler;
import datawave.query.scheduler.Scheduler;
import datawave.query.scheduler.SequentialScheduler;
import datawave.query.tables.stats.ScanSessionStats;
import datawave.query.transformer.DocumentTransformer;
import datawave.query.transformer.EventQueryDataDecoratorTransformer;
import datawave.query.transformer.GroupingDocumentTransformer;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.DateIndexHelperFactory;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MetadataHelperFactory;
import datawave.query.util.QueryStopwatch;
import datawave.util.StringUtils;
import datawave.util.time.TraceStopwatch;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl.Parameter;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.configuration.QueryData;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.logic.QueryLogicTransformer;
import datawave.webservice.query.logic.WritesQueryMetrics;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <h1>Overview</h1> QueryTable implementation that works with the JEXL grammar. This QueryTable uses the DATAWAVE metadata, global index, and sharded event
 * table to return results based on the query. The runServerQuery method is the main method that is called from the web service, and it contains the logic used
 * to run the queries against ACCUMULO. Example queries:
 *
 * <pre>
 *  <b>Single Term Query</b>
 *  'foo' - looks in global index for foo, and if any entries are found, then the query
 *          is rewritten to be field1 == 'foo' or field2 == 'foo', etc. This is then passed
 *          down the optimized query path which uses the intersecting iterators on the shard
 *          table.
 * 
 *  <b>Boolean expression</b>
 *  field == 'foo' - For fielded queries, those that contain a field, an operator, and a literal (string or number),
 *                   the query is parsed and the set of eventFields in the query that are indexed is determined by
 *                   querying the metadata table. Depending on the conjunctions in the query (or, and, not) and the
 *                   eventFields that are indexed, the query may be sent down the optimized path or the full scan path.
 * </pre>
 *
 * We are not supporting all of the operators that JEXL supports at this time. We are supporting the following operators:
 * 
 * <pre>
 *  ==, !=, &gt;, &ge;, &lt;, &le;, =~, !~, and the reserved word 'null'
 * </pre>
 *
 * Custom functions can be created and registered with the Jexl engine. The functions can be used in the queries in conjunction with other supported operators.
 * A sample function has been created, called between, and is bound to the 'f' namespace. An example using this function is : "f:between(LATITUDE,60.0, 70.0)"
 * 
 * <h1>Constraints on Query Structure</h1> Queries that are sent to this class need to be formatted such that there is a space on either side of the operator.
 * We are rewriting the query in some cases and the current implementation is expecting a space on either side of the operator.
 * 
 * <h1>Notes on Optimization</h1> Queries that meet any of the following criteria will perform a full scan of the events in the sharded event table:
 *
 * <pre>
 *  1. An 'or' conjunction exists in the query but not all of the terms are indexed.
 *  2. No indexed terms exist in the query
 *  3. An unsupported operator exists in the query
 * </pre>
 *
 * <h1>Notes on Features</h1>
 *
 * <pre>
 *  1. If there is no type specified for a field in the metadata table, then it defaults to using the NoOpType. The default
 *     can be overriden by calling setDefaultType()
 *  2. We support fields that are indexed, but not in the event. An example of this is for text documents, where the text is tokenized
 *     and indexed, but the tokens are not stored with the event.
 *  3. We support fields that have term frequency records in the shard table containing lists of offsets.  This would be for tokens
 *     parsed out of content and can subsequently be used in various content functions such as 'within' or 'adjacent' or 'phrase'.
 *  4. We support the ability to define a list of {@link DataEnricher}s to add additional information to returned events. Found events are
 *     passed through the {@link EnrichingMaster} which passes the event through each configured data enricher class. Only the value
 *     can be modified. The key *cannot* be modified through this interface (as it could break the sorted order). Enriching must be enabled
 *     by setting {@link #useEnrichers} to true and providing a list of {@link datawave.query.enrich.DataEnricher} class names in
 *     {@link #enricherClassNames}.
 *  5. A list of {@link datawave.query.index.lookup.DataTypeFilter}s can be specified to remove found Events before they are returned to the user.
 *     These data filters can return a true/false value on whether the Event should be returned to the user or discarded. Additionally,
 *     the filter can return a {@code Map<String, Object>} that can be passed into a JexlContext (provides the necessary information for Jexl to
 *     evaluate an Event based on information not already present in the Event or information that doesn't need to be returned with the Event.
 *     Filtering must be enabled by setting {@link #useFilters} to true and providing a list of {@link datawave.query.index.lookup.DataTypeFilter} class
 *     names in {@link #filterClassNames}.
 *  6. The query limits the results (default: 5000) using the setMaxResults method. In addition, "max.results.override" can be passed to the
 *     query as part of the Parameters object which allows query specific limits (but will not be more than set default)
 *  7. Projection can be accomplished by setting the {@link QueryParameters RETURN_FIELDS} parameter to a '/'-separated list of field names.
 * 
 * </pre>
 * 
 * @see datawave.query.enrich
 */
public class ShardQueryLogic extends BaseQueryLogic<Entry<Key,Value>> {
    
    protected static final Logger log = ThreadConfigurableLogger.getLogger(ShardQueryLogic.class);
    
    /**
     * Set of datatypes to limit the query to.
     */
    public static final String DATATYPE_FILTER_SET = "datatype.filter.set";
    
    public static final String LIMIT_FIELDS = "limit.fields";
    
    public static final String GROUP_FIELDS = "group.fields";
    
    public static final String HIT_LIST = "hit.list";
    
    public static final String TYPE_METADATA_IN_HDFS = "type.metadata.in.hdfs";
    
    public static final String METADATA_TABLE_NAME = "model.table.name";
    
    public static final String RAW_TYPES = "raw.types";
    
    public static final String NULL_BYTE = "\0";
    
    private String dateIndexTableName;
    private String metadataTableName;
    private String indexTableName;
    private String reverseIndexTableName;
    private String tableName;
    private String modelName;
    
    private List<IndexHole> indexHoles = new ArrayList<IndexHole>();
    
    // should we remove the shards and days hint from the queries before sending
    // to the tservers?
    private boolean cleanupShardsAndDaysQueryHints = true;
    
    private String modelTableName = "DatawaveMetadata";
    private String indexStatsTableName = "shardIndexStats";
    
    private int queryThreads = 8;
    private int indexLookupThreads = 8;
    private int dateIndexThreads = 8;
    private int maxDocScanTimeout = -1;
    
    // the percent shards marked when querying the date index after which the
    // shards are collapsed down to the entire day.
    private float collapseDatePercentThreshold = 0.99f;
    
    private String readAheadQueueSize = "0";
    private String readAheadTimeOut = "0";
    
    private List<String> enricherClassNames = null;
    private List<String> filterClassNames = null;
    private List<String> indexFilteringClassNames = null;
    private Map<String,String> filterOptions = null;
    
    private boolean useEnrichers = false;
    private boolean useFilters = false;
    
    private List<String> unevaluatedFields = Collections.emptyList();
    private Class<? extends Type<?>> defaultType = NoOpType.class;
    private boolean fullTableScanEnabled = true;
    private List<String> realmSuffixExclusionPatterns = null;
    private String nonEventKeyColFams = "d" + Constants.PARAM_VALUE_SEP + "tf";
    private double minSelectivity = -1.0;
    // should we filter out masked values when the user can see the unmasked
    // value
    private boolean filterMaskedValues = true;
    
    private boolean includeRecordId = true;
    private boolean includeDataTypeAsField = false;
    private boolean includeHierarchyFields = false;
    private Map<String,String> hierarchyFieldOptions = Collections.emptyMap();
    private boolean includeGroupingContext = false;
    private boolean reducedResponse = false;
    private boolean disableEvaluation = false;
    protected boolean disableIndexOnlyDocuments = false;
    private boolean hitList = false;
    private boolean typeMetadataInHdfs = false;
    private Set<String> blacklistedFields = new HashSet<>(0);
    private Set<String> limitFields = new HashSet<>(0);
    private Set<String> groupFields = new HashSet<>(0);
    private boolean compressServerSideResults = false;
    private boolean indexOnlyFilterFunctionsEnabled = false;
    /**
     * By default enable shortcut evaluation
     */
    private boolean allowShortcutEvaluation = true;
    
    /**
     * By default enable field index only evaluation (aggregation of document post evaluation)
     */
    private boolean allowFieldIndexEvaluation = true;
    
    /**
     * By default enable using term frequency instead of field index when possible for value lookup
     */
    private boolean allowTermFrequencyLookup = true;
    
    /**
     * By default don't use speculative scanning.
     */
    private boolean speculativeScanning = false;
    
    // Map of syntax names to QueryParser classes
    private Map<String,QueryParser> querySyntaxParsers = new HashMap<>();
    private Set<String> mandatoryQuerySyntax = null;
    
    private QueryPlanner planner = null;
    
    protected Class<? extends SortedKeyValueIterator<Key,Value>> createUidsIteratorClass = CreateUidsIterator.class;
    
    protected UidIntersector uidIntersector = new IndexInfo();
    
    protected CloseableIterable<QueryData> queries = null;
    
    // Threshold values used in the new RangeCalculator
    private int eventPerDayThreshold = 10000;
    private int shardsPerDayThreshold = 10;
    private int maxTermThreshold = 2500;
    private int maxDepthThreshold = 2500;
    private int maxUnfieldedExpansionThreshold = 500;
    private int maxValueExpansionThreshold = 5000;
    private int maxOrExpansionThreshold = 500;
    private int maxOrExpansionFstThreshold = 750;
    
    private long yieldThresholdMs = Long.MAX_VALUE;
    
    protected int maxScannerBatchSize = 1000;
    
    protected int maxIndexBatchSize = 1000;
    
    private String hdfsSiteConfigURLs = null;
    private String hdfsFileCompressionCodec = null;
    
    private String zookeeperConfig = null;
    
    private List<String> ivaratorCacheBaseURIs = null;
    private String ivaratorFstHdfsBaseURIs = null;
    private int ivaratorCacheBufferSize = 10000;
    private long ivaratorCacheScanPersistThreshold = 100000L;
    private long ivaratorCacheScanTimeout = 1000L * 60 * 60;
    
    private int maxFieldIndexRangeSplit = 11;
    private int ivaratorMaxOpenFiles = 100;
    private int maxIvaratorSources = 33;
    private int maxEvaluationPipelines = 25;
    private int maxPipelineCachedResults = 25;
    private boolean expandAllTerms = false;
    
    private QueryParser parser = null;
    
    protected QueryModel queryModel = null;
    
    protected String password = "";
    
    protected boolean sequentialScheduler = false;
    
    protected ScannerFactory scannerFactory = null;
    
    protected Scheduler scheduler = null;
    
    public final static Class<? extends ShardQueryConfiguration> tableConfigurationType = ShardQueryConfiguration.class;
    
    private List<String> contentFieldNames = Collections.emptyList();
    protected EventQueryDataDecoratorTransformer eventQueryDataDecoratorTransformer = null;
    
    protected ShardQueryConfiguration config = null;
    private Query settings = null;
    
    protected List<PushDownRule> pushDownRules = Collections.emptyList();
    private boolean shouldLimitTermExpansionToModel = false;
    
    protected boolean collapseUids = false;
    
    protected long maxIndexScanTimeMillis = Long.MAX_VALUE;
    
    private String dateIndexHelperTableName = null;
    private Set<Authorizations> dateIndexHelperAuthorizations = Sets.newHashSet();
    
    protected MetadataHelperFactory metadataHelperFactory = null;
    
    protected DateIndexHelperFactory dateIndexHelperFactory = null;
    
    // Cap (or fail if failOutsideValidDateRange) the begin date with this value (subtracted from Now). 0 or less disables this feature.
    private long beginDateCap = -1;
    
    private boolean failOutsideValidDateRange = false;
    
    protected Function<String,String> queryMacroFunction;
    
    static final ListeningExecutorService reloader = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    
    protected boolean limitAnyFieldLookups = false;
    
    protected boolean cacheModel = false;
    
    private static Cache<String,QueryModel> queryModelMap = CacheBuilder.newBuilder().maximumSize(100).concurrencyLevel(100)
                    .expireAfterAccess(24, TimeUnit.HOURS).build();
    
    private boolean accrueStats = false;
    private boolean collectTimingDetails = false;
    private boolean logTimingDetails = false;
    private boolean sendTimingToStatsd = false;
    private String statsdHost = "localhost";
    private int statsdPort = 8125;
    protected int statsdMaxQueueSize = 500;
    
    private CardinalityConfiguration cardinalityConfiguration = null;
    
    protected Map<String,Profile> configuredProfiles = Maps.newHashMap();
    
    protected Profile selectedProfile = null;
    
    protected boolean backoffEnabled = false;
    
    protected boolean unsortedUIDsEnabled = true;
    
    protected boolean debugMultithreadedSources = false;
    
    protected boolean dataQueryExpressionFilterEnabled = false;
    
    protected boolean sortGeoWaveQueryRanges = false;
    
    protected int numRangesToBuffer = 0;
    
    protected long rangeBufferTimeoutMillis = 0;
    
    protected long rangeBufferPollMillis = 100;
    
    protected int geoWaveMaxExpansion = 800;
    
    protected int geoWaveMaxEnvelopes = 4;
    
    public ShardQueryLogic() {
        super();
        setBaseIteratorPriority(100);
        if (log.isTraceEnabled())
            log.trace("Creating ShardQueryLogic: " + System.identityHashCode(this));
    }
    
    public ShardQueryLogic(ShardQueryLogic other) {
        super(other);
        
        if (log.isTraceEnabled())
            log.trace("Creating Cloned ShardQueryLogic: " + System.identityHashCode(this) + " from " + System.identityHashCode(other));
        
        this.setMetadataTableName(other.getMetadataTableName());
        log.trace("copy CTOR setting metadataHelperFactory to " + other.getMetadataHelperFactory());
        this.setMetadataHelperFactory(other.getMetadataHelperFactory());
        this.setDateIndexHelperFactory(other.getDateIndexHelperFactory());
        this.setDateIndexTableName(other.getDateIndexTableName());
        this.setCollapseDatePercentThreshold(other.getCollapseDatePercentThreshold());
        this.setCleanupShardsAndDaysQueryHints(other.isCleanupShardsAndDaysQueryHints());
        this.setIndexTableName(other.getIndexTableName());
        this.setReverseIndexTableName(other.getReverseIndexTableName());
        this.setModelName(other.getModelName());
        this.setModelTableName(other.getModelTableName());
        this.setIndexStatsTableName(other.getIndexStatsTableName());
        this.setIndexHoles(other.getIndexHoles());
        this.setQueryThreads(other.getQueryThreads());
        this.setIndexLookupThreads(other.getIndexLookupThreads());
        this.setReadAheadQueueSize(other.getReadAheadQueueSize());
        this.setReadAheadTimeOut(other.getReadAheadTimeOut());
        this.setEnricherClassNames(other.getEnricherClassNames());
        this.setFilterClassNames(other.getFilterClassNames());
        this.setFilterOptions(other.getFilterOptions());
        this.setUseEnrichers(other.isUseEnrichers());
        this.setUseFilters(other.isUseFilters());
        this.setUnevaluatedFields(other.getUnevaluatedFields());
        this.defaultType = other.getDefaultType();
        this.setFullTableScanEnabled(other.isFullTableScanEnabled());
        this.setRealmSuffixExclusionPatterns(other.getRealmSuffixExclusionPatterns());
        this.setNonEventKeyColFams(other.getNonEventKeyColFams());
        this.setMinimumSelectivity(other.getMinimumSelectivity());
        this.setFilterMaskedValues(other.getFilterMaskedValues());
        this.setIncludeDataTypeAsField(other.getIncludeDataTypeAsField());
        this.setIncludeRecordId(other.getIncludeRecordId());
        this.setIncludeHierarchyFields(other.getIncludeHierarchyFields());
        this.setIncludeGroupingContext(other.getIncludeGroupingContext());
        this.setReducedResponse(other.isReducedResponse());
        this.setBeginDateCap(other.getBeginDateCap());
        this.setFailOutsideValidDateRange(other.isFailOutsideValidDateRange());
        this.setLimitAnyFieldLookups(other.getLimitAnyFieldLookups());
        this.setDisableEvaluation(other.isDisableEvaluation());
        this.setDisableIndexOnlyDocuments(other.disableIndexOnlyDocuments());
        this.setHitList(other.isHitList());
        this.setTypeMetadataInHdfs(other.isTypeMetadataInHdfs());
        this.setBlacklistedFields(other.getBlacklistedFields());
        this.setCacheModel(other.getCacheModel());
        this.setLimitFields(other.getLimitFields());
        this.setGroupFields(other.getGroupFields());
        this.setCompressServerSideResults(other.isCompressServerSideResults());
        this.setQuerySyntaxParsers(other.getQuerySyntaxParsers());
        this.setMandatoryQuerySyntax(other.getMandatoryQuerySyntax());
        this.setQueryPlanner(other.getQueryPlanner().clone());
        this.setCreateUidsIteratorClass(other.getCreateUidsIteratorClass());
        this.setUidIntersector(other.getUidIntersector());
        this.setCollapseUids(other.collapseUids);
        this.queries = other.queries;
        this.setMaxIndexScanTimeMillis(other.getMaxIndexScanTimeMillis());
        
        this.setEventPerDayThreshold(other.getEventPerDayThreshold());
        this.setShardsPerDayThreshold(other.getShardsPerDayThreshold());
        this.setMaxTermThreshold(other.getMaxTermThreshold());
        this.setMaxDepthThreshold(other.getMaxDepthThreshold());
        this.setMaxUnfieldedExpansionThreshold(other.getMaxUnfieldedExpansionThreshold());
        this.setMaxValueExpansionThreshold(other.getMaxValueExpansionThreshold());
        this.setMaxOrExpansionThreshold(other.getMaxOrExpansionThreshold());
        this.setMaxOrExpansionFstThreshold(other.getMaxOrExpansionFstThreshold());
        this.setMaxScannerBatchSize(other.getMaxScannerBatchSize());
        this.setMaxIndexBatchSize(other.getMaxIndexBatchSize());
        
        this.setYieldThresholdMs(other.getYieldThresholdMs());
        
        this.setHdfsSiteConfigURLs(other.getHdfsSiteConfigURLs());
        this.setHdfsFileCompressionCodec(other.getHdfsFileCompressionCodec());
        this.setZookeeperConfig(other.getZookeeperConfig());
        
        this.setIvaratorCacheBaseURIs(other.getIvaratorCacheBaseURIs());
        this.setIvaratorFstHdfsBaseURIs(other.getIvaratorFstHdfsBaseURIs());
        this.setIvaratorCacheBufferSize(other.getIvaratorCacheBufferSize());
        this.setIvaratorCacheScanPersistThreshold(other.getIvaratorCacheScanPersistThreshold());
        this.setIvaratorCacheScanTimeout(other.getIvaratorCacheScanTimeout());
        
        this.setMaxFieldIndexRangeSplit(other.getMaxFieldIndexRangeSplit());
        this.setIvaratorMaxOpenFiles(other.getIvaratorMaxOpenFiles());
        this.setMaxIvaratorSources(other.getMaxIvaratorSources());
        this.setMaxEvaluationPipelines(other.getMaxEvaluationPipelines());
        this.setMaxPipelineCachedResults(other.getMaxPipelineCachedResults());
        
        this.setExpandAllTerms(other.isExpandAllTerms());
        this.setParser(other.getParser());
        this.setQueryModel(other.getQueryModel());
        
        this.setScannerFactory(other.getScannerFactory());
        this.setScheduler(other.getScheduler());
        this.config = other.config;
        this.settings = other.settings;
        this.setAccumuloPassword(other.getAccumuloPassword());
        this.setSequentialScheduler(other.getSequentialScheduler());
        this.setContentFieldNames(Lists.newArrayList(other.getContentFieldNames()));
        this.setAccrueStats(other.getAccrueStats());
        this.setCollectTimingDetails(other.getCollectTimingDetails());
        this.setLogTimingDetails(other.getLogTimingDetails());
        this.setSendTimingToStatsd(other.getSendTimingToStatsd());
        this.setStatsdHost(other.getStatsdHost());
        this.setStatsdPort(other.getStatsdPort());
        this.setStatsdMaxQueueSize(other.getStatsdMaxQueueSize());
        this.setCardinalityConfiguration(other.getCardinalityConfiguration());
        this.setAllowShortcutEvaluation(other.getAllowShortcutEvaluation());
        this.setAllowFieldIndexEvaluation(other.isAllowFieldIndexEvaluation());
        this.setAllowTermFrequencyLookup(other.isAllowTermFrequencyLookup());
        this.setSpeculativeScanning(other.speculativeScanning);
        this.setBackoffEnabled(other.getBackoffEnabled());
        this.setUnsortedUIDsEnabled(other.getUnsortedUIDsEnabled());
        this.setDebugMultithreadedSources(other.isDebugMultithreadedSources());
        this.setDataQueryExpressionFilterEnabled(other.isDataQueryExpressionFilterEnabled());
        this.setSortGeoWaveQueryRanges(other.isSortGeoWaveQueryRanges());
        this.setNumRangesToBuffer(other.getNumRangesToBuffer());
        this.setRangeBufferTimeoutMillis(other.getRangeBufferTimeoutMillis());
        this.setRangeBufferPollMillis(other.getRangeBufferPollMillis());
        this.setGeoWaveMaxExpansion(other.getGeoWaveMaxExpansion());
        this.setGeoWaveMaxEnvelopes(other.getGeoWaveMaxEnvelopes());
        this.setMaxDocScanTimeout(other.maxDocScanTimeout);
        this.setConfiguredProfiles(other.configuredProfiles);
        if (other.eventQueryDataDecoratorTransformer != null) {
            this.eventQueryDataDecoratorTransformer = new EventQueryDataDecoratorTransformer(other.eventQueryDataDecoratorTransformer);
        }
    }
    
    @Override
    public GenericQueryConfiguration initialize(Connector connection, Query settings, Set<Authorizations> auths) throws Exception {
        ShardQueryConfiguration config = ShardQueryConfigurationFactory.createShardQueryConfigurationFromConfiguredLogic(this, settings);
        
        this.config = config;
        this.settings = settings;
        if (log.isTraceEnabled())
            log.trace("Initializing ShardQueryLogic: " + System.identityHashCode(this) + '(' + (this.settings == null ? "empty" : this.settings.getId()) + ')');
        initialize(config, connection, settings, auths);
        return config;
    }
    
    public String getLimitFieldsString(Query settings) throws ParseException {
        Parameter parameter = settings.findParameter(QueryParameters.LIMIT_FIELDS);
        String value = parameter.getParameterValue();
        return value;
    }
    
    public String getGroupFieldsString(Query settings) throws ParseException {
        Parameter parameter = settings.findParameter(QueryParameters.GROUP_FIELDS);
        String value = parameter.getParameterValue();
        return value;
    }
    
    public String getHitListString(Query settings) throws ParseException {
        Parameter parameter = settings.findParameter(QueryParameters.HIT_LIST);
        String value = parameter.getParameterValue();
        return value;
    }
    
    protected String expandQueryMacros(String query) throws ParseException {
        log.trace("query macros are :" + this.queryMacroFunction);
        if (this.queryMacroFunction != null) {
            query = this.queryMacroFunction.apply(query);
        }
        return query;
    }
    
    public String getJexlQueryString(Query settings) throws ParseException {
        // queryString should be JEXl after all query parsers are applied
        String queryString;
        String originalQuery = settings.getQuery();
        
        originalQuery = this.expandQueryMacros(originalQuery);
        
        // Determine query syntax (i.e. JEXL, LUCENE, etc.)
        String querySyntax = settings.findParameter(QueryParameters.QUERY_SYNTAX).getParameterValue();
        
        // enforce mandatoryQuerySyntax if set
        if (null != this.mandatoryQuerySyntax) {
            if (org.apache.commons.lang.StringUtils.isEmpty(querySyntax)) {
                throw new IllegalStateException("Must specify one of the following syntax options: " + this.mandatoryQuerySyntax);
            } else {
                if (!this.mandatoryQuerySyntax.contains(querySyntax)) {
                    throw new IllegalStateException("Syntax not supported, must be one of the following: " + this.mandatoryQuerySyntax + ", submitted: "
                                    + querySyntax);
                }
            }
        }
        
        QueryParser querySyntaxParser = getParser();
        
        if (org.apache.commons.lang.StringUtils.isBlank(querySyntax)) {
            // Default to the class's query parser when one is not provided
            // Falling back to Jexl when one is not set on this class
            if (null == querySyntaxParser) {
                querySyntax = "JEXL";
            }
        } else if (!"JEXL".equals(querySyntax)) {
            if (null == querySyntaxParsers) {
                throw new IllegalStateException("Query syntax parsers not configured");
            }
            
            querySyntaxParser = querySyntaxParsers.get(querySyntax);
            
            if (null == querySyntaxParser) {
                // No parser was specified, try to default to the parser on the
                // class
                querySyntaxParser = getParser();
                
                if (null == querySyntaxParser) {
                    throw new IllegalArgumentException("QueryParser not configured for syntax: " + querySyntax);
                }
            }
        }
        
        if (null == originalQuery) {
            throw new IllegalArgumentException("Query cannot be null");
        } else {
            if ("JEXL".equals(querySyntax)) {
                queryString = originalQuery;
            } else {
                QueryNode node = querySyntaxParser.parse(originalQuery);
                queryString = node.getOriginalQuery();
                if (log.isTraceEnabled()) {
                    log.trace("luceneQueryString: " + originalQuery + " --> jexlQueryString: " + queryString);
                }
            }
        }
        return queryString;
    }
    
    public void initialize(ShardQueryConfiguration config, Connector connection, Query settings, Set<Authorizations> auths) throws Exception {
        // Set the connector and the authorizations into the config object
        config.setConnector(connection);
        config.setAuthorizations(auths);
        config.setMaxScannerBatchSize(getMaxScannerBatchSize());
        config.setMaxIndexBatchSize(getMaxIndexBatchSize());
        
        setScannerFactory(new ScannerFactory(config));
        
        String jexlQueryString = getJexlQueryString(settings);
        
        if (null == jexlQueryString) {
            throw new IllegalArgumentException("Query cannot be null");
        } else {
            config.setQueryString(jexlQueryString);
        }
        
        final Date beginDate = settings.getBeginDate();
        if (null == beginDate) {
            throw new IllegalArgumentException("Begin date cannot be null");
        } else {
            config.setBeginDate(beginDate);
        }
        
        final Date endDate = settings.getEndDate();
        if (null == endDate) {
            throw new IllegalArgumentException("End date cannot be null");
        } else {
            config.setEndDate(endDate);
        }
        
        loadQueryParameters(config, settings);
        
        MetadataHelper metadataHelper = prepareMetadataHelper(connection, this.getMetadataTableName(), auths, config.isRawTypes());
        
        DateIndexHelper dateIndexHelper = prepareDateIndexHelper(connection, this.getDateIndexTableName(), auths);
        if (config.isDateIndexTimeTravel()) {
            dateIndexHelper.setTimeTravel(config.isDateIndexTimeTravel());
        }
        
        QueryPlanner queryPlanner = getQueryPlanner();
        if (queryPlanner instanceof DefaultQueryPlanner) {
            DefaultQueryPlanner currentQueryPlanner = (DefaultQueryPlanner) queryPlanner;
            
            currentQueryPlanner.setMetadataHelper(metadataHelper);
            currentQueryPlanner.setDateIndexHelper(dateIndexHelper);
            
            QueryModelProvider queryModelProvider = currentQueryPlanner.getQueryModelProviderFactory().createQueryModelProvider();
            if (queryModelProvider instanceof MetadataHelperQueryModelProvider) {
                ((MetadataHelperQueryModelProvider) queryModelProvider).setMetadataHelper(metadataHelper);
                ((MetadataHelperQueryModelProvider) queryModelProvider).setConfig(config);
            }
            
            if (null != queryModelProvider.getQueryModel()) {
                queryModel = queryModelProvider.getQueryModel();
                
            }
        }
        
        if (this.queryModel == null)
            loadQueryModel(metadataHelper, config);
        
        getQueryPlanner().setCreateUidsIteratorClass(createUidsIteratorClass);
        getQueryPlanner().setUidIntersector(uidIntersector);
        
        if (cardinalityConfiguration != null && (config.getBlacklistedFields().size() > 0 || config.getProjectFields().size() > 0)) {
            // Ensure that fields used for resultCardinalities are returned. They will be removed in the DocumentTransformer.
            // Modify the projectFields and blacklistFields only for this stage, then return to the original values.
            // Not advisable to create a copy of the config object due to the embedded timers.
            Set<String> originalBlacklistedFields = new HashSet<>(config.getBlacklistedFields());
            Set<String> originalProjectFields = new HashSet<>(config.getProjectFields());
            
            // either projectFields or blacklistedFields can be used, but not both
            // this will be caught when loadQueryParameters is called
            if (config.getBlacklistedFields().size() > 0) {
                config.setBlacklistedFields(cardinalityConfiguration.getRevisedBlacklistFields(queryModel, originalBlacklistedFields));
            }
            if (config.getProjectFields().size() > 0) {
                config.setProjectFields(cardinalityConfiguration.getRevisedProjectFields(queryModel, originalProjectFields));
            }
            
            this.queries = getQueryPlanner().process(config, jexlQueryString, settings, this.scannerFactory);
            
            config.setBlacklistedFields(originalBlacklistedFields);
            config.setProjectFields(originalProjectFields);
        } else {
            this.queries = getQueryPlanner().process(config, jexlQueryString, settings, this.scannerFactory);
        }
        
        TraceStopwatch stopwatch = config.getTimers().newStartedStopwatch("ShardQueryLogic - Get iterator of queries");
        
        config.setQueries(this.queries.iterator());
        
        config.setQueryString(getQueryPlanner().getPlannedScript());
        
        stopwatch.stop();
    }
    
    protected MetadataHelper prepareMetadataHelper(Connector connection, String metadataTableName, Set<Authorizations> auths) {
        return prepareMetadataHelper(connection, metadataTableName, auths, false);
    }
    
    protected MetadataHelper prepareMetadataHelper(Connector connection, String metadataTableName, Set<Authorizations> auths, boolean rawTypes) {
        if (log.isTraceEnabled())
            log.trace("prepareMetadataHelper with " + connection);
        MetadataHelper metadataHelper = this.metadataHelperFactory.createMetadataHelper();
        // check to see if i need to initialize a new one
        if (metadataHelper.getMetadataTableName() != null && metadataTableName != null && !metadataTableName.equals(metadataHelper.getMetadataTableName())) {
            // initialize it
            metadataHelper.initialize(connection, metadataTableName, auths, rawTypes);
        } else if (metadataHelper.getAuths() == null || metadataHelper.getAuths().isEmpty()) {
            return metadataHelper.initialize(connection, metadataTableName, auths, rawTypes);
            // assumption is that it is already initialized. we shall see.....
        } else {
            if (log.isTraceEnabled())
                log.trace("the MetadataHelper did not need to be initialized:" + metadataHelper + " and " + metadataTableName + " and " + auths);
        }
        return metadataHelper.initialize(connection, metadataTableName, auths, rawTypes);
    }
    
    public MetadataHelperFactory getMetadataHelperFactory() {
        return metadataHelperFactory;
    }
    
    public void setMetadataHelperFactory(MetadataHelperFactory metadataHelperFactory) {
        if (log.isTraceEnabled())
            log.trace("setting MetadataHelperFactory on " + this + " - " + this.getClass() + " to " + metadataHelperFactory + " - "
                            + metadataHelperFactory.getClass());
        this.metadataHelperFactory = metadataHelperFactory;
    }
    
    public DateIndexHelperFactory getDateIndexHelperFactory() {
        return dateIndexHelperFactory;
    }
    
    public void setDateIndexHelperFactory(DateIndexHelperFactory dateIndexHelperFactory) {
        this.dateIndexHelperFactory = dateIndexHelperFactory;
    }
    
    private DateIndexHelper prepareDateIndexHelper(Connector connection, String dateIndexTableName, Set<Authorizations> auths) {
        DateIndexHelper dateIndexHelper = this.dateIndexHelperFactory.createDateIndexHelper();
        return dateIndexHelper.initialize(connection, dateIndexTableName, auths, dateIndexThreads, collapseDatePercentThreshold);
    }
    
    @Override
    public void setupQuery(GenericQueryConfiguration genericConfig) throws Exception {
        if (!ShardQueryConfiguration.class.isAssignableFrom(genericConfig.getClass())) {
            throw new QueryException("Did not receive a ShardQueryConfiguration instance!!");
        }
        
        ShardQueryConfiguration config = (ShardQueryConfiguration) genericConfig;
        
        final QueryStopwatch timers = config.getTimers();
        TraceStopwatch stopwatch = timers.newStartedStopwatch("ShardQueryLogic - Setup Query");
        
        // Ensure we have all of the information needed to run a query
        if (!config.canRunQuery()) {
            log.warn("The given query '" + config + "' could not be run, most likely due to not matching any records in the global index.");
            
            // Stub out an iterator to correctly present "no results"
            this.iterator = new Iterator<Map.Entry<Key,Value>>() {
                @Override
                public boolean hasNext() {
                    return false;
                }
                
                @Override
                public Map.Entry<Key,Value> next() {
                    return null;
                }
                
                @Override
                public void remove() {
                    return;
                }
            };
            
            this.scanner = null;
            
            stopwatch.stop();
            
            log.info(getStopwatchHeader(config));
            List<String> timings = timers.summarizeAsList();
            for (String timing : timings) {
                log.info(timing);
            }
            
            return;
        }
        
        // Instantiate the scheduler for the queries
        this.scheduler = getScheduler(config, scannerFactory);
        
        this.scanner = null;
        this.iterator = this.scheduler.iterator();
        
        if (!config.isSortedUIDs()) {
            this.iterator = new DedupingIterator(this.iterator);
        }
        
        stopwatch.stop();
        
        log.info(getStopwatchHeader(config));
        List<String> timings = timers.summarizeAsList();
        for (String timing : timings) {
            log.info(timing);
        }
    }
    
    protected String getStopwatchHeader(ShardQueryConfiguration config) {
        return "ShardQueryLogic: " + config.getQueryString() + ", [" + config.getBeginDate() + ", " + config.getEndDate() + "]";
    }
    
    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        MarkingFunctions markingFunctions = this.getMarkingFunctions();
        ResponseObjectFactory responseObjectFactory = this.getResponseObjectFactory();
        
        boolean reducedInSettings = false;
        String reducedResponseStr = settings.findParameter(QueryOptions.REDUCED_RESPONSE).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(reducedResponseStr)) {
            reducedInSettings = Boolean.parseBoolean(reducedResponseStr);
        }
        boolean reduced = (this.isReducedResponse() || reducedInSettings);
        DocumentTransformer transformer = null;
        if (settings.findParameter(QueryOptions.GROUP_FIELDS).getParameterValue().length() > 0) {
            transformer = new GroupingDocumentTransformer(this, settings, markingFunctions, responseObjectFactory, config.getGroupFields(), reduced);
        } else {
            transformer = new DocumentTransformer(this, settings, markingFunctions, responseObjectFactory, reduced);
        }
        transformer.setEventQueryDataDecoratorTransformer(eventQueryDataDecoratorTransformer);
        transformer.setContentFieldNames(contentFieldNames);
        transformer.setLogTimingDetails(this.logTimingDetails);
        transformer.setCardinalityConfiguration(cardinalityConfiguration);
        transformer.setQm(queryModel);
        if (config != null) {
            transformer.setProjectFields(config.getProjectFields());
            transformer.setBlacklistedFields(config.getBlacklistedFields());
        }
        
        return transformer;
    }
    
    protected void loadQueryParameters(ShardQueryConfiguration config, Query settings) throws QueryException {
        TraceStopwatch stopwatch = config.getTimers().newStartedStopwatch("ShardQueryLogic - Parse query parameters");
        
        boolean rawDataOnly = false;
        String rawDataOnlyStr = settings.findParameter(QueryParameters.RAW_DATA_ONLY).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(rawDataOnlyStr)) {
            rawDataOnly = Boolean.valueOf(rawDataOnlyStr);
            // if the master option raw.data.only is set, then set all of the transforming options appropriately.
            // note that if any of these other options are set, then it overrides the settings here
            if (rawDataOnly) {
                // set the grouping context to trye to ensure we get the full field names
                this.setIncludeGroupingContext(true);
                config.setIncludeGroupingContext(true);
                // set the hierarchy fields to false as they are generated fields
                this.setIncludeHierarchyFields(false);
                config.setIncludeHierarchyFields(false);
                // set the datatype field to false as it is a generated field
                this.setIncludeDataTypeAsField(false);
                config.setIncludeDataTypeAsField(false);
                // do not include the record id
                this.setIncludeRecordId(false);
                config.setIncludeRecordId(false);
                // set the hit list to false as it is a generated field
                this.setHitList(false);
                config.setHitList(false);
                // set the raw types to true to avoid any type transformations of the values
                config.setRawTypes(true);
                // do not filter masked values
                this.setFilterMaskedValues(false);
                config.setFilterMaskedValues(false);
                // do not reduce the response
                this.setReducedResponse(false);
                config.setReducedResponse(false);
                // clear the content field names to prevent content field transformations (see DocumentTransformer)
                this.setContentFieldNames(Collections.EMPTY_LIST);
                // clear the model name to avoid field name translations
                this.setModelName(null);
                config.setModelName(null);
            }
        }
        
        // Get the datatype set if specified
        String typeList = settings.findParameter(DATATYPE_FILTER_SET).getParameterValue().trim();
        
        if (org.apache.commons.lang.StringUtils.isNotBlank(typeList)) {
            HashSet<String> typeFilter = new HashSet<>();
            typeFilter.addAll(Arrays.asList(StringUtils.split(typeList, Constants.PARAM_VALUE_SEP)));
            
            if (log.isDebugEnabled()) {
                log.debug("Type Filter: " + typeFilter.toString());
            }
            
            config.setDatatypeFilter(typeFilter);
        }
        
        // Get the list of fields to project up the stack. May be null.
        String projectFields = settings.findParameter(QueryParameters.RETURN_FIELDS).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(projectFields)) {
            List<String> projectFieldsList = Arrays.asList(StringUtils.split(projectFields, Constants.PARAM_VALUE_SEP));
            
            // Only set the projection fields if we were actually given some
            if (!projectFieldsList.isEmpty()) {
                config.setProjectFields(new HashSet<>(projectFieldsList));
                
                if (log.isDebugEnabled()) {
                    final int maxLen = 100;
                    // Trim down the projection if it's stupid long
                    projectFields = maxLen < projectFields.length() ? projectFields.substring(0, maxLen) + "[TRUNCATED]" : projectFields;
                    log.debug("Projection fields: " + projectFields);
                }
            }
        }
        
        // if the TRANFORM_CONTENT_TO_UID is false, then unset the list of content field names preventing the DocumentTransformer from
        // transforming them.
        String transformContentStr = settings.findParameter(QueryParameters.TRANFORM_CONTENT_TO_UID).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(transformContentStr)) {
            if (!Boolean.valueOf(transformContentStr)) {
                setContentFieldNames(Collections.EMPTY_LIST);
            }
        }
        
        QueryLogicTransformer transformer = getTransformer(settings);
        if (transformer instanceof WritesQueryMetrics) {
            String logTimingDetailsStr = settings.findParameter(QueryOptions.LOG_TIMING_DETAILS).getParameterValue().trim();
            if (org.apache.commons.lang.StringUtils.isNotBlank(logTimingDetailsStr)) {
                this.logTimingDetails = Boolean.valueOf(logTimingDetailsStr);
            }
            if (this.logTimingDetails == true) {
                // we have to collect the timing details on the iterator stack
                // in order to log them
                this.collectTimingDetails = true;
            } else {
                
                String collectTimingDetailsStr = settings.findParameter(QueryOptions.COLLECT_TIMING_DETAILS).getParameterValue().trim();
                if (org.apache.commons.lang.StringUtils.isNotBlank(collectTimingDetailsStr)) {
                    this.collectTimingDetails = Boolean.valueOf(collectTimingDetailsStr);
                }
            }
        } else {
            // if the transformer can not process the timing metrics, then turn
            // them off
            this.logTimingDetails = false;
            this.collectTimingDetails = false;
        }
        
        config.setLogTimingDetails(this.logTimingDetails);
        config.setCollectTimingDetails(this.collectTimingDetails);
        
        // Get the list of blacklisted fields. May be null.
        String tBlacklistedFields = settings.findParameter(QueryParameters.BLACKLISTED_FIELDS).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(tBlacklistedFields)) {
            List<String> blacklistedFieldsList = Arrays.asList(StringUtils.split(tBlacklistedFields, Constants.PARAM_VALUE_SEP));
            
            // Only set the blacklisted fields if we were actually given some
            if (!blacklistedFieldsList.isEmpty()) {
                if (!config.getProjectFields().isEmpty()) {
                    throw new QueryException("Whitelist and blacklist projection options are mutually exclusive");
                }
                
                config.setBlacklistedFields(new HashSet<>(blacklistedFieldsList));
                
                if (log.isDebugEnabled()) {
                    log.debug("Blacklisted fields: " + tBlacklistedFields);
                }
            }
        }
        
        // Get the MAX_RESULTS_OVERRIDE parameter if given
        String maxResultsOverrideStr = settings.findParameter(MAX_RESULTS_OVERRIDE).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(maxResultsOverrideStr)) {
            try {
                long override = Long.parseLong(maxResultsOverrideStr);
                
                if (override < config.getMaxQueryResults()) {
                    config.setMaxQueryResults(override);
                    // this.maxresults is initially set to the value in the
                    // config, we are overriding it here for this instance
                    // of the query.
                    this.setMaxResults(override);
                }
            } catch (NumberFormatException nfe) {
                log.error(MAX_RESULTS_OVERRIDE + " query parameter is not a valid number: " + maxResultsOverrideStr + ", using default value");
            }
        }
        
        if (log.isDebugEnabled()) {
            log.debug("Max Results: " + config.getMaxQueryResults());
        }
        
        // Get the LIMIT_FIELDS parameter if given
        String limitFieldsString = settings.findParameter(QueryOptions.LIMIT_FIELDS).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(limitFieldsString)) {
            String limitFields = settings.findParameter(QueryParameters.LIMIT_FIELDS).getParameterValue().trim();
            if (org.apache.commons.lang.StringUtils.isNotBlank(limitFields)) {
                List<String> limitFieldsList = Arrays.asList(StringUtils.split(limitFields, Constants.PARAM_VALUE_SEP));
                
                // Only set the limit fields if we were actually given some
                if (!limitFieldsList.isEmpty()) {
                    config.setLimitFields(new HashSet<>(limitFieldsList));
                }
            }
        }
        
        // Get the GROUP_FIELDS parameter if given
        String groupFieldsString = settings.findParameter(QueryOptions.GROUP_FIELDS).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(groupFieldsString)) {
            String groupFields = settings.findParameter(QueryOptions.GROUP_FIELDS).getParameterValue().trim();
            if (org.apache.commons.lang.StringUtils.isNotBlank(groupFields)) {
                List<String> groupFieldsList = Arrays.asList(StringUtils.split(groupFields, Constants.PARAM_VALUE_SEP));
                
                // Only set the group fields if we were actually given some
                if (!groupFieldsList.isEmpty()) {
                    config.setGroupFields(new HashSet<>(groupFieldsList));
                    config.setProjectFields(new HashSet<>(groupFieldsList));
                }
            }
        }
        
        // Get the HIT_LIST parameter if given
        String hitListString = settings.findParameter(HIT_LIST).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(hitListString)) {
            Boolean hitListBool = Boolean.parseBoolean(hitListString);
            config.setHitList(hitListBool);
        }
        
        // Get the TYPE_METADATA_IN_HDFS parameter if given
        String typeMetadataInHdfsString = settings.findParameter(TYPE_METADATA_IN_HDFS).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(typeMetadataInHdfsString)) {
            Boolean typeMetadataInHdfsBool = Boolean.parseBoolean(typeMetadataInHdfsString);
            config.setTypeMetadataInHdfs(typeMetadataInHdfsBool);
        }
        
        // Get the BYPASS_ACCUMULO parameter if given
        String bypassAccumuloString = settings.findParameter(BYPASS_ACCUMULO).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(bypassAccumuloString)) {
            Boolean bypassAccumuloBool = Boolean.parseBoolean(bypassAccumuloString);
            config.setBypassAccumulo(bypassAccumuloBool);
        }
        
        // Get the DATE_INDEX_TIME_TRAVEL parameter if given
        String dateIndexTimeTravelString = settings.findParameter(QueryOptions.DATE_INDEX_TIME_TRAVEL).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(dateIndexTimeTravelString)) {
            Boolean dateIndexTimeTravel = Boolean.parseBoolean(dateIndexTimeTravelString);
            config.setDateIndexTimeTravel(dateIndexTimeTravel);
        }
        
        // get the RAW_TYPES parameter if given
        String rawTypesString = settings.findParameter(RAW_TYPES).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(rawTypesString)) {
            Boolean rawTypesBool = Boolean.parseBoolean(rawTypesString);
            config.setRawTypes(rawTypesBool);
            // if raw types are going to be replaced in the type metadata, we cannot use the hdfs-cached typemetadata
            // these properties are mutually exclusive
            if (rawTypesBool) {
                config.setTypeMetadataInHdfs(false);
            }
        }
        
        // Get the FILTER_MASKED_VALUES spring setting
        String filterMaskedValuesStr = settings.findParameter(QueryParameters.FILTER_MASKED_VALUES).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(filterMaskedValuesStr)) {
            Boolean filterMaskedValuesBool = Boolean.parseBoolean(filterMaskedValuesStr);
            this.setFilterMaskedValues(filterMaskedValuesBool);
            config.setFilterMaskedValues(filterMaskedValuesBool);
        }
        
        // Get the INCLUDE_DATATYPE_AS_FIELD spring setting
        String includeDatatypeAsFieldStr = settings.findParameter(QueryParameters.INCLUDE_DATATYPE_AS_FIELD).getParameterValue().trim();
        if (((org.apache.commons.lang.StringUtils.isNotBlank(includeDatatypeAsFieldStr) && Boolean.valueOf(includeDatatypeAsFieldStr)))
                        || (this.getIncludeDataTypeAsField() && !rawDataOnly)) {
            config.setIncludeDataTypeAsField(true);
        }
        
        // Get the INCLUDE_RECORD_ID spring setting
        String includeRecordIdStr = settings.findParameter(QueryParameters.INCLUDE_RECORD_ID).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(includeRecordIdStr)) {
            boolean includeRecordIdBool = Boolean.parseBoolean(includeRecordIdStr) && !rawDataOnly;
            this.setIncludeRecordId(includeRecordIdBool);
            config.setIncludeRecordId(includeRecordIdBool);
        }
        
        // Get the INCLUDE_HIERARCHY_FIELDS spring setting
        String includeHierarchyFieldsStr = settings.findParameter(QueryParameters.INCLUDE_HIERARCHY_FIELDS).getParameterValue().trim();
        if (((org.apache.commons.lang.StringUtils.isNotBlank(includeHierarchyFieldsStr) && Boolean.valueOf(includeHierarchyFieldsStr)))
                        || (this.getIncludeHierarchyFields() && !rawDataOnly)) {
            config.setIncludeHierarchyFields(true);
            
            final Map<String,String> options = this.getHierarchyFieldOptions();
            config.setHierarchyFieldOptions(options);
        }
        
        // Get the query profile to allow us to select the tune profile of the
        // query
        String queryProfile = settings.findParameter(QueryParameters.QUERY_PROFILE).getParameterValue().trim();
        if ((org.apache.commons.lang.StringUtils.isNotBlank(queryProfile))) {
            
            selectedProfile = configuredProfiles.get(queryProfile);
            
            if (null == selectedProfile) {
                throw new QueryException(QueryParameters.QUERY_PROFILE + " has been specified but " + queryProfile + " is not a selectable profile");
            }
            
        }
        
        // Get the include.grouping.context = true/false spring setting
        String includeGroupingContextStr = settings.findParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT).getParameterValue().trim();
        if (((org.apache.commons.lang.StringUtils.isNotBlank(includeGroupingContextStr) && Boolean.valueOf(includeGroupingContextStr)))
                        || (this.getIncludeGroupingContext() && !rawDataOnly)) {
            config.setIncludeGroupingContext(true);
        }
        
        // Check if the default modelName and modelTableNames have been
        // overriden by custom parameters.
        String parameterModelName = settings.findParameter(QueryParameters.PARAMETER_MODEL_NAME).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(parameterModelName)) {
            this.modelName = parameterModelName;
        }
        
        config.setModelName(this.modelName);
        
        String parameterModelTableName = settings.findParameter(QueryParameters.PARAMETER_MODEL_TABLE_NAME).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(parameterModelTableName)) {
            this.modelTableName = parameterModelTableName;
        }
        
        config.setModelTableName(this.modelTableName);
        
        if (null != config.getModelName() && null == config.getModelTableName()) {
            throw new IllegalArgumentException(QueryParameters.PARAMETER_MODEL_NAME + " has been specified but " + QueryParameters.PARAMETER_MODEL_TABLE_NAME
                            + " is missing. Both are required to use a model");
        }
        
        configureDocumentAggregation(settings);
        
        config.setLimitTermExpansionToModel(this.isExpansionLimitedToModelContents());
        
        String reducedResponseStr = settings.findParameter(QueryOptions.REDUCED_RESPONSE).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(reducedResponseStr)) {
            Boolean reducedResponseValue = Boolean.parseBoolean(reducedResponseStr);
            this.setReducedResponse(reducedResponseValue);
            config.setReducedResponse(reducedResponseValue);
        }
        
        final String postProcessingClasses = settings.findParameter(QueryOptions.POSTPROCESSING_CLASSES).getParameterValue().trim();
        
        final String postProcessingOptions = settings.findParameter(QueryOptions.POSTPROCESSING_OPTIONS).getParameterValue().trim();
        
        // build the post p
        if (org.apache.commons.lang.StringUtils.isNotBlank(postProcessingClasses)) {
            
            List<String> filterClasses = config.getFilterClassNames();
            if (null == filterClasses) {
                filterClasses = new ArrayList<>();
            }
            
            for (String fClassName : StringUtils.splitIterable(postProcessingClasses, ',', true)) {
                filterClasses.add(fClassName);
            }
            config.setFilterClassNames(filterClasses);
            
            final Map<String,String> options = this.filterOptions;
            if (null != options) {
                config.putFilterOptions(options);
            }
            
            if (org.apache.commons.lang.StringUtils.isNotBlank(postProcessingOptions)) {
                for (String filterOptionStr : StringUtils.splitIterable(postProcessingOptions, ',', true)) {
                    if (org.apache.commons.lang.StringUtils.isNotBlank(filterOptionStr)) {
                        final String filterValueString = settings.findParameter(filterOptionStr).getParameterValue().trim();
                        if (org.apache.commons.lang.StringUtils.isNotBlank(filterValueString)) {
                            config.putFilterOptions(filterOptionStr, filterValueString);
                        }
                    }
                }
            }
        }
        
        String tCompressServerSideResults = settings.findParameter(QueryOptions.COMPRESS_SERVER_SIDE_RESULTS).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(tCompressServerSideResults)) {
            boolean compress = Boolean.parseBoolean(tCompressServerSideResults);
            config.setCompressServerSideResults(compress);
        }
        
        // Configure index-only filter functions to be enabled if not already
        // set to such a state
        config.setIndexOnlyFilterFunctionsEnabled(this.isIndexOnlyFilterFunctionsEnabled());
        
        // Set the ReturnType for Documents coming out of the iterator stack
        config.setReturnType(DocumentSerialization.getReturnType(settings));
        stopwatch.stop();
        
        if (null != selectedProfile) {
            selectedProfile.configure(this);
            selectedProfile.configure(config);
            selectedProfile.configure(planner);
        }
    }
    
    void configureDocumentAggregation(Query settings) {
        Parameter disabledIndexOnlyDocument = settings.findParameter(QueryOptions.DISABLE_DOCUMENTS_WITHOUT_EVENTS);
        if (null != disabledIndexOnlyDocument) {
            final String disabledIndexOnlyDocumentStr = disabledIndexOnlyDocument.getParameterValue().trim();
            if (org.apache.commons.lang.StringUtils.isNotBlank(disabledIndexOnlyDocumentStr)) {
                Boolean disabledIndexOnlyDocuments = Boolean.parseBoolean(disabledIndexOnlyDocumentStr);
                this.setDisableIndexOnlyDocuments(disabledIndexOnlyDocuments);
                config.setDisableIndexOnlyDocuments(disableIndexOnlyDocuments);
            }
        }
    }
    
    /**
     * Loads a query Model
     * 
     * @param helper
     * @param config
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws TableNotFoundException
     * @throws ExecutionException
     */
    protected void loadQueryModel(MetadataHelper helper, ShardQueryConfiguration config) throws InstantiationException, IllegalAccessException,
                    TableNotFoundException, ExecutionException {
        TraceStopwatch modelWatch = config.getTimers().newStartedStopwatch("ShardQueryLogic - Loading the query model");
        
        int cacheKeyCode = new HashCodeBuilder().append(config.getDatatypeFilter()).append(config.getModelName()).hashCode();
        
        if (cacheModel) {
            queryModel = queryModelMap.getIfPresent(String.valueOf(cacheKeyCode));
        }
        if (null == queryModel && (null != config.getModelName() && null != config.getModelTableName())) {
            
            queryModel = helper.getQueryModel(config.getModelTableName(), config.getModelName(), helper.getIndexOnlyFields(config.getDatatypeFilter()));
            
            if (cacheModel) {
                
                queryModelMap.put(String.valueOf(cacheKeyCode), queryModel);
            }
            
        }
        config.setQueryModel(queryModel);
        
        modelWatch.stop();
        
    }
    
    protected Scheduler getScheduler(ShardQueryConfiguration config, ScannerFactory scannerFactory) {
        if (config.getSequentialScheduler()) {
            return new SequentialScheduler(config, scannerFactory);
        } else {
            return new PushdownScheduler(config, scannerFactory, this.metadataHelperFactory);
        }
    }
    
    public EventQueryDataDecoratorTransformer getEventQueryDataDecoratorTransformer() {
        return eventQueryDataDecoratorTransformer;
    }
    
    public void setEventQueryDataDecoratorTransformer(EventQueryDataDecoratorTransformer eventQueryDataDecoratorTransformer) {
        this.eventQueryDataDecoratorTransformer = eventQueryDataDecoratorTransformer;
    }
    
    @Override
    public ShardQueryLogic clone() {
        return new ShardQueryLogic(this);
    }
    
    @Override
    public void close() {
        
        super.close();
        
        log.debug("Closing ShardQueryLogic: " + System.identityHashCode(this));
        
        if (null == scannerFactory) {
            log.debug("ScannerFactory was never initialized because, therefore there are no connections to close: " + System.identityHashCode(this));
        } else {
            log.debug("Closing ShardQueryLogic scannerFactory: " + System.identityHashCode(this));
            try {
                int nClosed = 0;
                scannerFactory.lockdown();
                for (ScannerBase bs : Lists.newArrayList(scannerFactory.currentScanners())) {
                    scannerFactory.close(bs);
                    ++nClosed;
                }
                if (log.isDebugEnabled()) {
                    log.debug("Cleaned up " + nClosed + " batch scanners associated with this query logic.");
                }
                
                nClosed = 0;
                
                for (ScannerSession bs : Lists.newArrayList(scannerFactory.currentSessions())) {
                    scannerFactory.close(bs);
                    ++nClosed;
                }
                
                if (log.isDebugEnabled()) {
                    log.debug("Cleaned up " + nClosed + " scanner sessions.");
                }
                
            } catch (Exception e) {
                log.error("Caught exception trying to close scannerFactory", e);
            }
            
        }
        
        if (null != this.planner) {
            try {
                log.debug("Closing ShardQueryLogic planner: " + System.identityHashCode(this) + '(' + (this.settings == null ? "empty" : this.settings.getId())
                                + ')');
                this.planner.close(this.config, this.settings);
            } catch (Exception e) {
                log.error("Caught exception trying to close QueryPlanner", e);
            }
        }
        
        if (null != this.queries) {
            try {
                log.debug("Closing ShardQueryLogic queries: " + System.identityHashCode(this));
                this.queries.close();
            } catch (IOException e) {
                log.error("Caught exception trying to close CloseableIterable of queries", e);
            }
        }
        
        if (null != this.scheduler) {
            try {
                log.debug("Closing ShardQueryLogic scheduler: " + System.identityHashCode(this));
                this.scheduler.close();
                
                ScanSessionStats stats = this.scheduler.getSchedulerStats();
                
                if (null != stats) {
                    stats.logSummary(log);
                }
                
            } catch (IOException e) {
                log.error("Caught exception trying to close Scheduler", e);
            }
        }
        
    }
    
    public static BatchScanner createBatchScanner(ShardQueryConfiguration config, ScannerFactory scannerFactory, QueryData qd) throws TableNotFoundException {
        final BatchScanner bs = scannerFactory.newScanner(config.getShardTableName(), config.getAuthorizations(), config.getNumQueryThreads(),
                        config.getQuery());
        
        if (log.isTraceEnabled()) {
            log.trace("Running with " + config.getAuthorizations() + " and " + config.getNumQueryThreads() + " threads: " + qd);
        }
        
        bs.setRanges(qd.getRanges());
        
        for (IteratorSetting cfg : qd.getSettings()) {
            bs.addScanIterator(cfg);
        }
        
        return bs;
    }
    
    public String getNonEventKeyColFams() {
        return nonEventKeyColFams;
    }
    
    public void setNonEventKeyColFams(String nonEventKeyColFams) {
        this.nonEventKeyColFams = nonEventKeyColFams;
    }
    
    @Override
    public AccumuloConnectionFactory.Priority getConnectionPriority() {
        return AccumuloConnectionFactory.Priority.NORMAL;
    }
    
    public boolean getFilterMaskedValues() {
        return filterMaskedValues;
    }
    
    public void setFilterMaskedValues(boolean filterMaskedValues) {
        this.filterMaskedValues = filterMaskedValues;
    }
    
    public boolean getIncludeDataTypeAsField() {
        return includeDataTypeAsField;
    }
    
    public void setIncludeDataTypeAsField(boolean includeDataTypeAsField) {
        this.includeDataTypeAsField = includeDataTypeAsField;
    }
    
    public boolean getIncludeRecordId() {
        return includeRecordId;
    }
    
    public void setIncludeRecordId(boolean includeRecordId) {
        this.includeRecordId = includeRecordId;
    }
    
    public boolean getIncludeHierarchyFields() {
        return includeHierarchyFields;
    }
    
    public void setIncludeHierarchyFields(boolean includeHierarchyFields) {
        this.includeHierarchyFields = includeHierarchyFields;
    }
    
    public Map<String,String> getHierarchyFieldOptions() {
        return this.hierarchyFieldOptions;
    }
    
    public void setHierarchyFieldOptions(final Map<String,String> options) {
        final Map<String,String> emptyOptions = Collections.emptyMap();
        this.hierarchyFieldOptions = (null != options) ? options : emptyOptions;
    }
    
    public void setBlacklistedFields(Set<String> blacklistedFields) {
        this.blacklistedFields = new HashSet<>(blacklistedFields);
    }
    
    public Set<String> getBlacklistedFields() {
        return this.blacklistedFields;
    }
    
    public void setLimitFields(Set<String> limitFields) {
        this.limitFields = new HashSet<>(limitFields);
    }
    
    public Set<String> getLimitFields() {
        return this.limitFields;
    }
    
    public void setGroupFields(Set<String> groupFields) {
        this.groupFields = new HashSet<>(groupFields);
    }
    
    public Set<String> getGroupFields() {
        return this.groupFields;
    }
    
    public String getBlacklistedFieldsString() {
        return org.apache.commons.lang.StringUtils.join(this.blacklistedFields, '/');
    }
    
    public boolean getIncludeGroupingContext() {
        return this.includeGroupingContext;
    }
    
    public void setIncludeGroupingContext(boolean opt) {
        this.includeGroupingContext = opt;
    }
    
    public boolean isReducedResponse() {
        return reducedResponse;
    }
    
    public void setReducedResponse(boolean reducedResponse) {
        this.reducedResponse = reducedResponse;
    }
    
    public boolean isDisableEvaluation() {
        return disableEvaluation;
    }
    
    public void setDisableEvaluation(boolean disableEvaluation) {
        this.disableEvaluation = disableEvaluation;
    }
    
    public boolean disableIndexOnlyDocuments() {
        return disableIndexOnlyDocuments;
    }
    
    public void setDisableIndexOnlyDocuments(boolean disableIndexOnlyDocuments) {
        this.disableIndexOnlyDocuments = disableIndexOnlyDocuments;
    }
    
    public boolean isHitList() {
        return this.hitList;
    }
    
    public void setHitList(boolean hitList) {
        this.hitList = hitList;
    }
    
    public boolean isTypeMetadataInHdfs() {
        return typeMetadataInHdfs;
    }
    
    public void setTypeMetadataInHdfs(boolean typeMetadataInHdfs) {
        this.typeMetadataInHdfs = typeMetadataInHdfs;
    }
    
    public int getEventPerDayThreshold() {
        return eventPerDayThreshold;
    }
    
    public int getShardsPerDayThreshold() {
        return shardsPerDayThreshold;
    }
    
    public int getMaxTermThreshold() {
        return maxTermThreshold;
    }
    
    public int getMaxDepthThreshold() {
        return maxDepthThreshold;
    }
    
    public int getMaxUnfieldedExpansionThreshold() {
        return maxUnfieldedExpansionThreshold;
    }
    
    public int getMaxValueExpansionThreshold() {
        return maxValueExpansionThreshold;
    }
    
    public int getMaxOrExpansionThreshold() {
        return maxOrExpansionThreshold;
    }
    
    public void setMaxOrExpansionThreshold(int maxOrExpansionThreshold) {
        this.maxOrExpansionThreshold = maxOrExpansionThreshold;
    }
    
    public int getMaxOrExpansionFstThreshold() {
        return maxOrExpansionFstThreshold;
    }
    
    public void setMaxOrExpansionFstThreshold(int maxOrExpansionFstThreshold) {
        this.maxOrExpansionFstThreshold = maxOrExpansionFstThreshold;
    }
    
    public void setEventPerDayThreshold(int eventPerDayThreshold) {
        this.eventPerDayThreshold = eventPerDayThreshold;
    }
    
    public void setShardsPerDayThreshold(int shardsPerDayThreshold) {
        this.shardsPerDayThreshold = shardsPerDayThreshold;
    }
    
    public void setMaxTermThreshold(int maxTermThreshold) {
        this.maxTermThreshold = maxTermThreshold;
    }
    
    public void setMaxDepthThreshold(int maxDepthThreshold) {
        this.maxDepthThreshold = maxDepthThreshold;
    }
    
    public void setMaxUnfieldedExpansionThreshold(int maxUnfieldedExpansionThreshold) {
        this.maxUnfieldedExpansionThreshold = maxUnfieldedExpansionThreshold;
    }
    
    public void setMaxValueExpansionThreshold(int maxValueExpansionThreshold) {
        this.maxValueExpansionThreshold = maxValueExpansionThreshold;
    }
    
    public boolean isCleanupShardsAndDaysQueryHints() {
        return cleanupShardsAndDaysQueryHints;
    }
    
    public void setCleanupShardsAndDaysQueryHints(boolean cleanupShardsAndDaysQueryHints) {
        this.cleanupShardsAndDaysQueryHints = cleanupShardsAndDaysQueryHints;
    }
    
    public String getDateIndexTableName() {
        return dateIndexTableName;
    }
    
    public String getMetadataTableName() {
        return metadataTableName;
    }
    
    public String getIndexTableName() {
        return indexTableName;
    }
    
    @Override
    public String getTableName() {
        return tableName;
    }
    
    public void setDateIndexTableName(String dateIndexTableName) {
        this.dateIndexTableName = dateIndexTableName;
    }
    
    public void setMetadataTableName(String metadataTableName) {
        this.metadataTableName = metadataTableName;
    }
    
    public void setIndexTableName(String indexTableName) {
        this.indexTableName = indexTableName;
    }
    
    @Override
    public void setTableName(String tableName) {
        this.tableName = tableName;
        super.setTableName(tableName);
    }
    
    public String getModelTableName() {
        return modelTableName;
    }
    
    public void setModelTableName(String modelTableName) {
        this.modelTableName = modelTableName;
    }
    
    public String getModelName() {
        return modelName;
    }
    
    public void setModelName(String modelName) {
        this.modelName = modelName;
    }
    
    public int getQueryThreads() {
        return queryThreads;
    }
    
    public void setQueryThreads(int queryThreads) {
        this.queryThreads = queryThreads;
    }
    
    public int getIndexLookupThreads() {
        return indexLookupThreads;
    }
    
    public void setIndexLookupThreads(int indexLookupThreads) {
        this.indexLookupThreads = indexLookupThreads;
    }
    
    public void setDateIndexThreads(int indexThreads) {
        this.dateIndexThreads = indexThreads;
    }
    
    public int getDateIndexThreads() {
        return dateIndexThreads;
    }
    
    public void setMaxDocScanTimeout(int maxDocScanTimeout) {
        this.maxDocScanTimeout = maxDocScanTimeout;
    }
    
    public int getMaxDocScanTimeout() {
        return maxDocScanTimeout;
    }
    
    public float getCollapseDatePercentThreshold() {
        return collapseDatePercentThreshold;
    }
    
    public void setCollapseDatePercentThreshold(float collapseDatePercentThreshold) {
        this.collapseDatePercentThreshold = collapseDatePercentThreshold;
    }
    
    public String getReadAheadQueueSize() {
        return readAheadQueueSize;
    }
    
    public String getReadAheadTimeOut() {
        return readAheadTimeOut;
    }
    
    public void setReadAheadQueueSize(String readAheadQueueSize) {
        this.readAheadQueueSize = readAheadQueueSize;
    }
    
    public void setReadAheadTimeOut(String readAheadTimeOut) {
        this.readAheadTimeOut = readAheadTimeOut;
    }
    
    public List<String> getEnricherClassNames() {
        return this.enricherClassNames;
    }
    
    public void setEnricherClassNames(List<String> enricherClassNames) {
        this.enricherClassNames = enricherClassNames;
    }
    
    public boolean isUseEnrichers() {
        return this.useEnrichers;
    }
    
    public void setUseEnrichers(boolean useEnrichers) {
        this.useEnrichers = useEnrichers;
    }
    
    public boolean isTldQuery() {
        return false;
    }
    
    public boolean isExpandAllTerms() {
        return expandAllTerms;
    }
    
    public void setExpandAllTerms(boolean expandAllTerms) {
        this.expandAllTerms = expandAllTerms;
    }
    
    public List<String> getFilterClassNames() {
        return filterClassNames;
    }
    
    public List<String> getIndexFilteringClassNames() {
        return indexFilteringClassNames;
    }
    
    public Map<String,String> getFilterOptions() {
        return filterOptions;
    }
    
    public void setFilterClassNames(List<String> filterClassNames) {
        this.filterClassNames = filterClassNames;
    }
    
    public void setIndexFilteringClassNames(List<String> classNames) {
        this.indexFilteringClassNames = classNames;
    }
    
    public void setFilterOptions(final Map<String,String> options) {
        if (null != options) {
            filterOptions = new HashMap<String,String>(options);
        } else {
            filterOptions = null;
        }
    }
    
    public boolean isUseFilters() {
        return this.useFilters;
    }
    
    public void setUseFilters(boolean useFilters) {
        this.useFilters = useFilters;
    }
    
    public String getReverseIndexTableName() {
        return reverseIndexTableName;
    }
    
    public void setReverseIndexTableName(String reverseIndexTableName) {
        this.reverseIndexTableName = reverseIndexTableName;
    }
    
    public List<String> getUnevaluatedFields() {
        return unevaluatedFields;
    }
    
    public void setUnevaluatedFields(List<String> unevaluatedFields) {
        this.unevaluatedFields = unevaluatedFields;
    }
    
    public void setUnevaluatedFields(String unevaluatedFieldList) {
        this.unevaluatedFields = Arrays.asList(unevaluatedFieldList.split(ShardQueryConfiguration.PARAM_VALUE_SEP_STR));
    }
    
    public Class<? extends Type<?>> getDefaultType() {
        return defaultType;
    }
    
    @SuppressWarnings("unchecked")
    public void setDefaultType(String className) {
        try {
            defaultType = (Class<? extends Type<?>>) Class.forName(className);
        } catch (ClassNotFoundException ex) {
            log.warn("Class name: " + className + " not found, defaulting to NoOpNormalizer.class");
            defaultType = NoOpType.class;
        }
    }
    
    public boolean isFullTableScanEnabled() {
        return fullTableScanEnabled;
    }
    
    public void setFullTableScanEnabled(boolean fullTableScanEnabled) {
        this.fullTableScanEnabled = fullTableScanEnabled;
    }
    
    public List<String> getIvaratorCacheBaseURIsAsList() {
        return ivaratorCacheBaseURIs;
    }
    
    public String getIvaratorCacheBaseURIs() {
        if (ivaratorCacheBaseURIs == null) {
            return null;
        } else {
            StringBuilder builder = new StringBuilder();
            for (String hdfsCacheBaseURI : ivaratorCacheBaseURIs) {
                if (builder.length() > 0) {
                    builder.append(',');
                }
                builder.append(hdfsCacheBaseURI);
            }
            return builder.toString();
        }
    }
    
    public void setIvaratorCacheBaseURIs(String ivaratorCacheBaseURIs) {
        if (ivaratorCacheBaseURIs == null || ivaratorCacheBaseURIs.isEmpty()) {
            this.ivaratorCacheBaseURIs = null;
        } else {
            this.ivaratorCacheBaseURIs = Arrays.asList(StringUtils.split(ivaratorCacheBaseURIs, ','));
        }
    }
    
    public String getIvaratorFstHdfsBaseURIs() {
        return ivaratorFstHdfsBaseURIs;
    }
    
    public void setIvaratorFstHdfsBaseURIs(String ivaratorFstHdfsBaseURIs) {
        this.ivaratorFstHdfsBaseURIs = ivaratorFstHdfsBaseURIs;
    }
    
    public int getIvaratorCacheBufferSize() {
        return ivaratorCacheBufferSize;
    }
    
    public void setIvaratorCacheBufferSize(int ivaratorCacheBufferSize) {
        this.ivaratorCacheBufferSize = ivaratorCacheBufferSize;
    }
    
    public long getIvaratorCacheScanPersistThreshold() {
        return ivaratorCacheScanPersistThreshold;
    }
    
    public void setIvaratorCacheScanPersistThreshold(long ivaratorCacheScanPersistThreshold) {
        this.ivaratorCacheScanPersistThreshold = ivaratorCacheScanPersistThreshold;
    }
    
    public long getIvaratorCacheScanTimeout() {
        return ivaratorCacheScanTimeout;
    }
    
    public void setIvaratorCacheScanTimeout(long ivaratorCacheScanTimeout) {
        this.ivaratorCacheScanTimeout = ivaratorCacheScanTimeout;
    }
    
    public void setIvaratorCacheScanTimeoutMinutes(long hdfsCacheScanTimeoutMinutes) {
        this.ivaratorCacheScanTimeout = hdfsCacheScanTimeoutMinutes * 1000 * 60;
    }
    
    public String getHdfsSiteConfigURLs() {
        return hdfsSiteConfigURLs;
    }
    
    public void setHdfsSiteConfigURLs(String hadoopConfigURLs) {
        this.hdfsSiteConfigURLs = hadoopConfigURLs;
    }
    
    public String getHdfsFileCompressionCodec() {
        return hdfsFileCompressionCodec;
    }
    
    public void setHdfsFileCompressionCodec(String hdfsFileCompressionCodec) {
        this.hdfsFileCompressionCodec = hdfsFileCompressionCodec;
    }
    
    public String getZookeeperConfig() {
        return zookeeperConfig;
    }
    
    public void setZookeeperConfig(String zookeeperConfig) {
        this.zookeeperConfig = zookeeperConfig;
    }
    
    public int getMaxFieldIndexRangeSplit() {
        return maxFieldIndexRangeSplit;
    }
    
    public void setMaxFieldIndexRangeSplit(int maxFieldIndexRangeSplit) {
        this.maxFieldIndexRangeSplit = maxFieldIndexRangeSplit;
    }
    
    public int getIvaratorMaxOpenFiles() {
        return ivaratorMaxOpenFiles;
    }
    
    public void setIvaratorMaxOpenFiles(int ivaratorMaxOpenFiles) {
        this.ivaratorMaxOpenFiles = ivaratorMaxOpenFiles;
    }
    
    public int getMaxIvaratorSources() {
        return maxIvaratorSources;
    }
    
    public void setMaxIvaratorSources(int maxIvaratorSources) {
        this.maxIvaratorSources = maxIvaratorSources;
    }
    
    public int getMaxEvaluationPipelines() {
        return maxEvaluationPipelines;
    }
    
    public void setMaxEvaluationPipelines(int maxEvaluationPipelines) {
        this.maxEvaluationPipelines = maxEvaluationPipelines;
    }
    
    public int getMaxPipelineCachedResults() {
        return maxPipelineCachedResults;
    }
    
    public void setMaxPipelineCachedResults(int maxCachedResults) {
        this.maxPipelineCachedResults = maxCachedResults;
    }
    
    public double getMinimumSelectivity() {
        return this.minSelectivity;
    }
    
    public void setMinimumSelectivity(double d) {
        this.minSelectivity = d;
    }
    
    public String getIndexStatsTableName() {
        return this.indexStatsTableName;
    }
    
    public void setIndexStatsTableName(String t) {
        this.indexStatsTableName = t;
    }
    
    public Map<String,QueryParser> getQuerySyntaxParsers() {
        return querySyntaxParsers;
    }
    
    public void setQuerySyntaxParsers(Map<String,QueryParser> querySyntaxParsers) {
        this.querySyntaxParsers = querySyntaxParsers;
    }
    
    public QueryParser getParser() {
        return parser;
    }
    
    public void setParser(QueryParser parser) {
        this.parser = parser;
    }
    
    public QueryPlanner getQueryPlanner() {
        if (null == planner) {
            planner = new DefaultQueryPlanner();
        }
        
        return planner;
    }
    
    public void setQueryPlanner(QueryPlanner planner) {
        this.planner = planner;
    }
    
    public Class<? extends SortedKeyValueIterator<Key,Value>> getCreateUidsIteratorClass() {
        return createUidsIteratorClass;
    }
    
    public void setCreateUidsIteratorClass(Class<? extends SortedKeyValueIterator<Key,Value>> createUidsIteratorClass) {
        this.createUidsIteratorClass = createUidsIteratorClass;
    }
    
    public UidIntersector getUidIntersector() {
        return uidIntersector;
    }
    
    public void setUidIntersector(UidIntersector uidIntersector) {
        this.uidIntersector = uidIntersector;
    }
    
    public List<String> getContentFieldNames() {
        return contentFieldNames;
    }
    
    public void setContentFieldNames(List<String> contentFieldNames) {
        this.contentFieldNames = contentFieldNames;
    }
    
    public CloseableIterable<QueryData> getQueries() {
        return queries;
    }
    
    public QueryModel getQueryModel() {
        return queryModel;
    }
    
    public void setQueryModel(QueryModel queryModel) {
        log.debug("Setting a cached query model");
        this.queryModel = queryModel;
    }
    
    public ScannerFactory getScannerFactory() {
        return scannerFactory;
    }
    
    public void setScannerFactory(ScannerFactory scannerFactory) {
        log.debug("Setting scanner factory on ShardQueryLogic: " + System.identityHashCode(this) + ".setScannerFactory("
                        + System.identityHashCode(scannerFactory) + ')');
        this.scannerFactory = scannerFactory;
    }
    
    public Scheduler getScheduler() {
        return scheduler;
    }
    
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }
    
    public void setMaxScannerBatchSize(final int size) {
        this.maxScannerBatchSize = size;
    }
    
    public int getMaxScannerBatchSize() {
        return this.maxScannerBatchSize;
    }
    
    public void setMaxIndexBatchSize(final int size) {
        this.maxIndexBatchSize = size;
    }
    
    public int getMaxIndexBatchSize() {
        return this.maxIndexBatchSize;
    }
    
    public boolean getCompressServerSideResults() {
        return compressServerSideResults;
    }
    
    public boolean isCompressServerSideResults() {
        return compressServerSideResults;
    }
    
    public void setCompressServerSideResults(boolean compressServerSideResults) {
        this.compressServerSideResults = compressServerSideResults;
    }
    
    /**
     * Returns a value indicating whether index-only filter functions (e.g., #INCLUDE, #EXCLUDE) should be enabled. If true, the use of such filters can
     * potentially consume a LOT of memory.
     * 
     * @return true, if index-only filter functions should be enabled.
     */
    public boolean isIndexOnlyFilterFunctionsEnabled() {
        return this.indexOnlyFilterFunctionsEnabled;
    }
    
    /**
     * Sets a value indicating whether index-only filter functions (e.g., #INCLUDE and #EXCLUDE) should be enabled. If true, the use of such filters can
     * potentially consume a LOT of memory.
     * 
     * @param enabled
     *            indicates whether index-only filter functions (e.g., <i>filter:includeRegex()</i> and <i>not(filter:includeRegex())</i>) should be enabled
     */
    public void setIndexOnlyFilterFunctionsEnabled(boolean enabled) {
        this.indexOnlyFilterFunctionsEnabled = enabled;
    }
    
    @Override
    public Set<String> getOptionalQueryParameters() {
        Set<String> params = new TreeSet<>();
        params.add(datawave.webservice.query.QueryParameters.QUERY_BEGIN);
        params.add(datawave.webservice.query.QueryParameters.QUERY_END);
        params.add(QueryParameters.QUERY_SYNTAX);
        params.add(QueryParameters.PARAMETER_MODEL_NAME);
        params.add(QueryParameters.PARAMETER_MODEL_TABLE_NAME);
        params.add(QueryParameters.DATATYPE_FILTER_SET);
        params.add(QueryParameters.RETURN_FIELDS);
        params.add(QueryParameters.BLACKLISTED_FIELDS);
        params.add(QueryParameters.MAX_RESULTS_OVERRIDE);
        params.add(QueryParameters.FILTER_MASKED_VALUES);
        params.add(QueryParameters.INCLUDE_DATATYPE_AS_FIELD);
        params.add(QueryParameters.INCLUDE_GROUPING_CONTEXT);
        params.add(QueryParameters.RAW_DATA_ONLY);
        params.add(QueryParameters.TRANFORM_CONTENT_TO_UID);
        params.add(QueryOptions.REDUCED_RESPONSE);
        params.add(QueryOptions.POSTPROCESSING_CLASSES);
        params.add(QueryOptions.COMPRESS_SERVER_SIDE_RESULTS);
        params.add(QueryOptions.HIT_LIST);
        params.add(QueryOptions.TYPE_METADATA_IN_HDFS);
        params.add(QueryOptions.DATE_INDEX_TIME_TRAVEL);
        params.add(QueryOptions.LIMIT_FIELDS);
        params.add(QueryOptions.GROUP_FIELDS);
        params.add(QueryOptions.LOG_TIMING_DETAILS);
        return params;
    }
    
    @Override
    public Set<String> getRequiredQueryParameters() {
        return Collections.emptySet();
    }
    
    @Override
    public Set<String> getExampleQueries() {
        return Collections.emptySet();
    }
    
    public Set<String> getMandatoryQuerySyntax() {
        return mandatoryQuerySyntax;
    }
    
    public void setMandatoryQuerySyntax(Set<String> mandatoryQuerySyntax) {
        this.mandatoryQuerySyntax = mandatoryQuerySyntax;
    }
    
    public List<String> getRealmSuffixExclusionPatterns() {
        return realmSuffixExclusionPatterns;
    }
    
    public void setRealmSuffixExclusionPatterns(List<String> realmSuffixExclusionPatterns) {
        this.realmSuffixExclusionPatterns = realmSuffixExclusionPatterns;
    }
    
    public void setAccumuloPassword(String password) {
        this.password = password;
    }
    
    /**
     * @return
     */
    public String getAccumuloPassword() {
        return new String(password);
    }
    
    public void setLimitTermExpansionToModel(boolean shouldLimitTermExpansionToModel) {
        this.shouldLimitTermExpansionToModel = shouldLimitTermExpansionToModel;
    }
    
    public boolean isExpansionLimitedToModelContents() {
        return shouldLimitTermExpansionToModel;
    }
    
    public boolean getSequentialScheduler() {
        return sequentialScheduler;
    }
    
    public void setSequentialScheduler(boolean sequentialScheduler) {
        this.sequentialScheduler = sequentialScheduler;
    }
    
    public boolean getCollapseUids() {
        return collapseUids;
    }
    
    public void setCollapseUids(boolean collapseUids) {
        this.collapseUids = collapseUids;
    }
    
    public void setMaxIndexScanTimeMillis(long maxTime) {
        this.maxIndexScanTimeMillis = maxTime;
    }
    
    public long getMaxIndexScanTimeMillis() {
        return maxIndexScanTimeMillis;
    }
    
    public Function getQueryMacroFunction() {
        return queryMacroFunction;
    }
    
    public void setQueryMacroFunction(Function queryMacroFunction) {
        this.queryMacroFunction = queryMacroFunction;
    }
    
    public boolean getLimitAnyFieldLookups() {
        return limitAnyFieldLookups;
    }
    
    public void setLimitAnyFieldLookups(boolean limitAnyFieldLookups) {
        this.limitAnyFieldLookups = limitAnyFieldLookups;
    }
    
    public boolean getSpeculativeScanning() {
        return speculativeScanning;
    }
    
    public void setSpeculativeScanning(boolean speculativeScanning) {
        this.speculativeScanning = speculativeScanning;
    }
    
    public boolean getAllowShortcutEvaluation() {
        return allowShortcutEvaluation;
    }
    
    public void setAllowShortcutEvaluation(boolean allowShortcutEvaluation) {
        this.allowShortcutEvaluation = allowShortcutEvaluation;
    }
    
    public boolean isAllowFieldIndexEvaluation() {
        return allowFieldIndexEvaluation;
    }
    
    public void setAllowFieldIndexEvaluation(boolean allowFieldIndexEvaluation) {
        this.allowFieldIndexEvaluation = allowFieldIndexEvaluation;
    }
    
    public boolean isAllowTermFrequencyLookup() {
        return allowTermFrequencyLookup;
    }
    
    public void setAllowTermFrequencyLookup(boolean allowTermFrequencyLookup) {
        this.allowTermFrequencyLookup = allowTermFrequencyLookup;
    }
    
    public boolean getAccrueStats() {
        return accrueStats;
    }
    
    public void setAccrueStats(final boolean accrueStats) {
        this.accrueStats = accrueStats;
    }
    
    public void setCollectTimingDetails(Boolean collectTimingDetails) {
        this.collectTimingDetails = collectTimingDetails;
        
    }
    
    public Boolean getCollectTimingDetails() {
        return collectTimingDetails;
    }
    
    public Boolean getLogTimingDetails() {
        return logTimingDetails;
    }
    
    public void setLogTimingDetails(Boolean logTimingDetails) {
        this.logTimingDetails = logTimingDetails;
    }
    
    public String getStatsdHost() {
        return statsdHost;
    }
    
    public void setStatsdHost(String statsdHost) {
        this.statsdHost = statsdHost;
    }
    
    public int getStatsdPort() {
        return statsdPort;
    }
    
    public void setStatsdPort(int statsdPort) {
        this.statsdPort = statsdPort;
    }
    
    public int getStatsdMaxQueueSize() {
        return statsdMaxQueueSize;
    }
    
    public void setStatsdMaxQueueSize(int statsdMaxQueueSize) {
        this.statsdMaxQueueSize = statsdMaxQueueSize;
    }
    
    public boolean getSendTimingToStatsd() {
        return sendTimingToStatsd;
    }
    
    public void setSendTimingToStatsd(boolean sendTimingToStatsd) {
        this.sendTimingToStatsd = sendTimingToStatsd;
    }
    
    public void setCacheModel(boolean cacheModel) {
        this.cacheModel = cacheModel;
    }
    
    public boolean getCacheModel() {
        return this.cacheModel;
    }
    
    public List<IndexHole> getIndexHoles() {
        return indexHoles;
    }
    
    public void setIndexHoles(List<IndexHole> indexHoles) {
        this.indexHoles = indexHoles;
    }
    
    public CardinalityConfiguration getCardinalityConfiguration() {
        return cardinalityConfiguration;
    }
    
    public void setCardinalityConfiguration(CardinalityConfiguration cardinalityConfiguration) {
        this.cardinalityConfiguration = cardinalityConfiguration;
    }
    
    public void setConfiguredProfiles(Map<String,Profile> configuredProfiles) {
        this.configuredProfiles.putAll(configuredProfiles);
    }
    
    public void setBackoffEnabled(boolean backoffEnabled) {
        this.backoffEnabled = backoffEnabled;
    }
    
    public boolean getBackoffEnabled() {
        return backoffEnabled;
    }
    
    public boolean getUnsortedUIDsEnabled() {
        return unsortedUIDsEnabled;
    }
    
    public void setUnsortedUIDsEnabled(boolean unsortedUIDsEnabled) {
        this.unsortedUIDsEnabled = unsortedUIDsEnabled;
    }
    
    public boolean isDebugMultithreadedSources() {
        return debugMultithreadedSources;
    }
    
    public void setDebugMultithreadedSources(boolean debugMultithreadedSources) {
        this.debugMultithreadedSources = debugMultithreadedSources;
    }
    
    public boolean isDataQueryExpressionFilterEnabled() {
        return dataQueryExpressionFilterEnabled;
    }
    
    public void setDataQueryExpressionFilterEnabled(boolean dataQueryExpressionFilterEnabled) {
        this.dataQueryExpressionFilterEnabled = dataQueryExpressionFilterEnabled;
    }
    
    public boolean isSortGeoWaveQueryRanges() {
        return sortGeoWaveQueryRanges;
    }
    
    public void setSortGeoWaveQueryRanges(boolean sortGeoWaveQueryRanges) {
        this.sortGeoWaveQueryRanges = sortGeoWaveQueryRanges;
    }
    
    public int getNumRangesToBuffer() {
        return numRangesToBuffer;
    }
    
    public void setNumRangesToBuffer(int numRangesToBuffer) {
        this.numRangesToBuffer = numRangesToBuffer;
    }
    
    public long getRangeBufferTimeoutMillis() {
        return rangeBufferTimeoutMillis;
    }
    
    public void setRangeBufferTimeoutMillis(long rangeBufferTimeoutMillis) {
        this.rangeBufferTimeoutMillis = rangeBufferTimeoutMillis;
    }
    
    public long getRangeBufferPollMillis() {
        return rangeBufferPollMillis;
    }
    
    public void setRangeBufferPollMillis(long rangeBufferPollMillis) {
        this.rangeBufferPollMillis = rangeBufferPollMillis;
    }
    
    public int getGeoWaveMaxExpansion() {
        return geoWaveMaxExpansion;
    }
    
    public void setGeoWaveMaxExpansion(int geoWaveMaxExpansion) {
        this.geoWaveMaxExpansion = geoWaveMaxExpansion;
    }
    
    public int getGeoWaveMaxEnvelopes() {
        return geoWaveMaxEnvelopes;
    }
    
    public void setGeoWaveMaxEnvelopes(int geoWaveMaxEnvelopes) {
        this.geoWaveMaxEnvelopes = geoWaveMaxEnvelopes;
    }
    
    public long getBeginDateCap() {
        return beginDateCap;
    }
    
    public void setBeginDateCap(long beginDateCap) {
        this.beginDateCap = beginDateCap;
    }
    
    public boolean isFailOutsideValidDateRange() {
        return failOutsideValidDateRange;
    }
    
    public void setFailOutsideValidDateRange(boolean failOutsideValidDateRange) {
        this.failOutsideValidDateRange = failOutsideValidDateRange;
    }
    
    public long getYieldThresholdMs() {
        return yieldThresholdMs;
    }
    
    public void setYieldThresholdMs(long yieldThresholdMs) {
        this.yieldThresholdMs = yieldThresholdMs;
    }
    
}
