package datawave.query.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.data.type.DiscreteIndexType;
import datawave.data.type.NoOpType;
import datawave.data.type.Type;
import datawave.query.Constants;
import datawave.query.DocumentSerialization;
import datawave.query.DocumentSerialization.ReturnType;
import datawave.query.QueryParameters;
import datawave.query.UnindexType;
import datawave.query.function.DocumentPermutation;
import datawave.query.iterator.QueryIterator;
import datawave.query.model.QueryModel;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tld.TLDQueryIterator;
import datawave.query.util.QueryStopwatch;
import datawave.util.UniversalSet;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 * A GenericQueryConfiguration implementation that provides the additional logic on top of the traditional query that is needed to run a DATAWAVE sharded
 * boolean-logic query
 *
 * <p>
 * Provides support for normalizers, enricher classes, filter classes, projection, and datatype filters, in addition to additional parameters also exposed in
 * the Webservice QueryTable interface
 *
 * <p>
 * This class can be initialized with an instance of a ShardQueryLogic or ShardQueryTable which will grab the already configured parameters from the Accumulo
 * Webservice QueryTable and apply them to this configuration object
 */
public class ShardQueryConfiguration extends GenericQueryConfiguration implements Serializable {
    public static final String PARAM_VALUE_SEP_STR = new String(new char[] {Constants.PARAM_VALUE_SEP});
    @SuppressWarnings("unused")
    private static final long serialVersionUID = -4354990715046146110L;
    private static final Logger log = Logger.getLogger(ShardQueryConfiguration.class);
    // is this a tld query, explicitly default to false
    private boolean tldQuery = false;
    private Map<String,String> filterOptions = new HashMap<>();
    private boolean disableIndexOnlyDocuments = false;
    @JsonIgnore
    private QueryStopwatch timers = new QueryStopwatch();
    private int maxScannerBatchSize = 1000;
    /**
     * Index batch size is the size of results use for each index lookup
     */
    private int maxIndexBatchSize = 1000;
    private boolean allTermsIndexOnly;
    private String accumuloPassword = "";
    private long maxIndexScanTimeMillis = Long.MAX_VALUE;
    private boolean collapseUids = false;
    private boolean sequentialScheduler = false;
    private boolean collectTimingDetails = false;
    private boolean logTimingDetails = false;
    private boolean sendTimingToStatsd = true;
    private String statsdHost = "localhost";
    private int statsdPort = 8125;
    private int statsdMaxQueueSize = 500;
    private boolean limitAnyFieldLookups = false;
    private boolean bypassExecutabilityCheck = false;
    /**
     * Allows for back off of scanners.
     */
    private boolean backoffEnabled = false;
    /**
     * Allows for the unsorted UIDs feature (see SortedUIDsRequiredVisitor)
     */
    private boolean unsortedUIDsEnabled = true;
    /**
     * Allows us to serialize query iterator
     */
    private boolean serializeQueryIterator = false;
    /**
     * Used to enable the SourceThreadTrackingIterator on the tservers
     */
    private boolean debugMultithreadedSources = false;
    /**
     * Used to enable Event Field Value filtering in the TLD based on Query Expressions
     */
    private boolean dataQueryExpressionFilterEnabled = false;
    /**
     * Used to enable sorting query ranges from most to least granular for queries which contain geowave fields in ThreadedRangeBundler
     */
    private boolean sortGeoWaveQueryRanges = false;
    /**
     * Used to determine how many ranges the ThreadedRangeBundler should buffer before returning a range to the caller
     */
    private int numRangesToBuffer = 0;
    /**
     * Used to determine how long to allow the ThreadedRangeBundler to buffer ranges before returning a range to the caller
     */
    private long rangeBufferTimeoutMillis = 0;
    /**
     * Used to determine the poll interval when buffering ranges in ThreadedRangeBundler
     */
    private long rangeBufferPollMillis = 100;
    /**
     * Used to determine the maximum number of query ranges to generate per tier when performing a geowave query.
     */
    private int geoWaveMaxExpansion = 800;
    /**
     * Used to determine the maximum number of envelopes which can be used when generating ranges for a geowave query.
     */
    private int geoWaveMaxEnvelopes = 4;
    private String shardTableName = "shard";
    private String indexTableName = "shardIndex";
    private String reverseIndexTableName = "shardReverseIndex";
    private String metadataTableName = "DatawaveMetadata";
    private String dateIndexTableName = "DateIndex";
    private String indexStatsTableName = "shardIndexStats";
    private String defaultDateTypeName = "EVENT";
    // should we cleanup the shards and days hints that are sent to the tservers?
    private boolean cleanupShardsAndDaysQueryHints = true;
    // BatchScanner and query results options
    private Integer numQueryThreads = 8;
    private Integer numLookupThreads = 8;
    private Integer numDateIndexThreads = 8;
    private Integer maxDocScanTimeout = -1;
    // A counter used to uniquely identify FSTs generated in the
    // PushdownLargeFieldedListsVisitor
    private AtomicInteger fstCount = new AtomicInteger(0);
    // the percent shards marked when querying the date index after which the
    // shards are collapsed down to the entire day.
    private float collapseDatePercentThreshold = 0.99f;
    // Do we drop to a full-table scan if the query is not "optimized"
    private Boolean fullTableScanEnabled = true;
    private List<String> realmSuffixExclusionPatterns = null;
    // A default normalizer to use
    private Class<? extends Type<?>> defaultType = NoOpType.class;
    private SimpleDateFormat shardDateFormatter = new SimpleDateFormat("yyyyMMdd");
    // Enrichment properties
    private Boolean useEnrichers = false;
    private List<String> enricherClassNames = null;
    // Filter properties
    private Boolean useFilters = false;
    private List<String> filterClassNames = null;
    private List<String> indexFilteringClassNames = new ArrayList<>();
    // Used for ignoring 'd' and 'tf' column family in `shard`
    private Set<String> nonEventKeyPrefixes = Sets.newHashSet("d", "tf");
    // Default to having no unevaluatedFields
    private Set<String> unevaluatedFields = Collections.emptySet();
    // Filter results on datatypes. Default to having no filters
    private Set<String> datatypeFilter = UniversalSet.instance();
    // A set of sorted index holes
    private List<IndexHole> indexHoles = new ArrayList<>();
    // Limit fields returned per event
    private Set<String> projectFields = Collections.emptySet();
    private Set<String> blacklistedFields = new HashSet<>(0);
    private Set<String> indexedFields = Sets.newHashSet();
    private Set<String> normalizedFields = Sets.newHashSet();
    private Multimap<String,Type<?>> dataTypes = HashMultimap.create();
    private Multimap<String,Type<?>> queryFieldsDatatypes = HashMultimap.create();
    private Multimap<String,Type<?>> normalizedFieldsDatatypes = HashMultimap.create();
    private Map<String,DiscreteIndexType<?>> fieldToDiscreteIndexTypes = new HashMap<>();
    private Multimap<String,String> compositeToFieldMap = ArrayListMultimap.create();
    private Map<String,Date> compositeTransitionDates = new HashMap<>();
    private Map<String,String> compositeFieldSeparators = new HashMap<>();
    
    private boolean sortedUIDs = true;
    // The fields in the the query that are tf fields
    private Set<String> queryTermFrequencyFields = Collections.emptySet();
    // Are we required to get term frequencies (i.e. does the query contain content functions)
    private boolean termFrequenciesRequired = false;
    // Limit count of returned values for arbitrary fields.
    private Set<String> limitFields = Collections.emptySet();
    /**
     * should limit fields be applied early
     */
    private boolean limitFieldsPreQueryEvaluation = false;
    /**
     * when <code>limitFieldsPreQueryEvaluation = true</code> this field will be used to record which fields were limited
     */
    private String limitFieldsField = null;
    private boolean hitList = false;
    private boolean typeMetadataInHdfs = false;
    private boolean dateIndexTimeTravel = false;
    // Cap (or fail if failOutsideValidDateRange) the begin date with this value (subtracted from Now). 0 or less disables this feature.
    private long beginDateCap = -1;
    private boolean failOutsideValidDateRange = true;
    private boolean rawTypes = false;
    // Used to choose how "selective" a term is (indexStats)
    private double minSelectivity = -1.0;
    // Used to add the event datatype to the event as an event field.
    private boolean includeDataTypeAsField = false;
    // Used to add the event RECORD_ID to the event as an event field
    private boolean includeRecordId = true;
    // Used to add the CHILD_COUNT, DESCENDANT_COUNT, HAS_CHILDREN and/or
    // PARENT_UID as event fields,
    // plus various options for output and optimization
    private boolean includeHierarchyFields = false;
    private Map<String,String> hierarchyFieldOptions = Collections.emptyMap();
    // Used to set the ShardEventEvaluating iterator INCLUDE_GROUPING_CONTEXT
    private boolean includeGroupingContext = false;
    // Used to create arbitrary document permutations prior to evaluation and/or returning documents.
    private List<String> documentPermutations = Collections.emptyList();
    // Used to filter out masked values when the unmasked value is available
    private boolean filterMaskedValues = true;
    private boolean reducedResponse = false;
    /**
     * By default enable shortcut evaluation
     */
    private volatile boolean allowShortcutEvaluation = true;
    private boolean bypassAccumulo = false;
    /**
     * By default don't use speculative scanning.
     */
    private boolean speculativeScanning = false;
    private boolean disableEvaluation = false;
    private boolean containsIndexOnlyTerms = false;
    private boolean containsCompositeTerms = false;
    /**
     * By default enable field index only evaluation (aggregation of document post evaluation)
     */
    private boolean allowFieldIndexEvaluation = true;
    /**
     * By default enable using term frequency instead of field index when possible for value lookup
     */
    private boolean allowTermFrequencyLookup = true;
    private ReturnType returnType = DocumentSerialization.DEFAULT_RETURN_TYPE;
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
    // Adding the ability to pre-cache the query model for performance sake. If this is null
    // then the query model will be pulled from the MetadataHelper
    private QueryModel queryModel = null;
    private String modelName = null;
    private String modelTableName = "DatawaveMetadata";
    // limit expanded terms to only those fields that are defined in the chosen
    // model. drop others
    private boolean shouldLimitTermExpansionToModel = false;
    private Query query = null;
    private boolean compressServerSideResults = false;
    private boolean indexOnlyFilterFunctionsEnabled = false;
    private boolean compositeFilterFunctionsEnabled = false;
    
    private int groupFieldsBatchSize;
    private boolean accrueStats = false;
    private Set<String> groupFields = new HashSet<>(0);
    private Set<String> uniqueFields = new HashSet<>(0);
    private boolean cacheModel = false;
    /**
     * should the sizes of documents be tracked for this query
     */
    private boolean trackSizes = true;
    
    private List<String> contentFieldNames = Collections.emptyList();
    
    /**
     * Default constructor
     */
    public ShardQueryConfiguration() {
        super();
        query = new QueryImpl();
    }
    
    /**
     * Performs a deep copy of the provided ShardQueryConfiguration into a new instance
     * 
     * @param other
     *            - another ShardQueryConfiguration instance
     */
    public ShardQueryConfiguration(ShardQueryConfiguration other) {
        
        this.setTldQuery(other.isTldQuery());
        this.putFilterOptions(other.getFilterOptions());
        this.setDisableIndexOnlyDocuments(other.isDisableIndexOnlyDocuments());
        this.setMaxScannerBatchSize(other.getMaxScannerBatchSize());
        this.setMaxIndexBatchSize(other.getMaxIndexBatchSize());
        this.setAllTermsIndexOnly(other.isAllTermsIndexOnly());
        this.setAccumuloPassword(other.getAccumuloPassword());
        this.setMaxIndexScanTimeMillis(other.getMaxIndexScanTimeMillis());
        this.setCollapseUids(other.getCollapseUids());
        this.setSequentialScheduler(other.getSequentialScheduler());
        this.setCollectTimingDetails(other.getCollectTimingDetails());
        this.setLogTimingDetails(other.getLogTimingDetails());
        this.setSendTimingToStatsd(other.getSendTimingToStatsd());
        this.setStatsdHost(other.getStatsdHost());
        this.setStatsdPort(other.getStatsdPort());
        this.setStatsdMaxQueueSize(other.getStatsdMaxQueueSize());
        this.setLimitAnyFieldLookups(other.getLimitAnyFieldLookups());
        this.setBypassExecutabilityCheck(other.isBypassExecutabilityCheck());
        this.setBackoffEnabled(other.getBackoffEnabled());
        this.setUnsortedUIDsEnabled(other.getUnsortedUIDsEnabled());
        this.setSerializeQueryIterator(other.getSerializeQueryIterator());
        this.setDebugMultithreadedSources(other.isDebugMultithreadedSources());
        this.setDataQueryExpressionFilterEnabled(other.isDataQueryExpressionFilterEnabled());
        this.setSortGeoWaveQueryRanges(other.isSortGeoWaveQueryRanges());
        this.setNumRangesToBuffer(other.getNumRangesToBuffer());
        this.setRangeBufferTimeoutMillis(other.getRangeBufferTimeoutMillis());
        this.setRangeBufferPollMillis(other.getRangeBufferPollMillis());
        this.setGeoWaveMaxExpansion(other.getGeoWaveMaxExpansion());
        this.setGeoWaveMaxEnvelopes(other.getGeoWaveMaxEnvelopes());
        this.setShardTableName(other.getShardTableName());
        this.setIndexTableName(other.getIndexTableName());
        this.setReverseIndexTableName(other.getReverseIndexTableName());
        this.setMetadataTableName(other.getMetadataTableName());
        this.setDateIndexTableName(other.getDateIndexTableName());
        this.setIndexStatsTableName(other.getIndexStatsTableName());
        this.setDefaultDateTypeName(other.getDefaultDateTypeName());
        this.setCleanupShardsAndDaysQueryHints(other.isCleanupShardsAndDaysQueryHints());
        this.setNumQueryThreads(other.getNumQueryThreads());
        this.setNumIndexLookupThreads(other.getNumIndexLookupThreads());
        this.setNumDateIndexThreads(other.getNumDateIndexThreads());
        this.setMaxDocScanTimeout(other.getMaxDocScanTimeout());
        this.setFstCount(other.getFstCount());
        this.setCollapseDatePercentThreshold(other.getCollapseDatePercentThreshold());
        this.setFullTableScanEnabled(other.getFullTableScanEnabled());
        this.setRealmSuffixExclusionPatterns(null == other.getRealmSuffixExclusionPatterns() ? null : Lists.newArrayList(other
                        .getRealmSuffixExclusionPatterns()));
        this.setDefaultType(other.getDefaultType());
        this.setShardDateFormatter(null == other.getShardDateFormatter() ? null : new SimpleDateFormat(other.getShardDateFormatter().toPattern())); // TODO --
                                                                                                                                                    // deep copy
        this.setUseEnrichers(other.getUseEnrichers());
        this.setEnricherClassNames(null == other.getEnricherClassNames() ? null : Lists.newArrayList(other.getEnricherClassNames()));
        this.setUseFilters(other.getUseFilters());
        this.setFilterClassNames(null == other.getFilterClassNames() ? null : Lists.newArrayList(other.getFilterClassNames()));
        this.setIndexFilteringClassNames(null == other.getIndexFilteringClassNames() ? null : Lists.newArrayList(other.getIndexFilteringClassNames()));
        this.setNonEventKeyPrefixes(null == other.getNonEventKeyPrefixes() ? null : Sets.newHashSet(other.getNonEventKeyPrefixes()));
        this.setUnevaluatedFields(null == other.getUnevaluatedFields() ? null : Sets.newHashSet(other.getUnevaluatedFields()));
        this.setDatatypeFilter(null == other.getDatatypeFilter() ? null : Sets.newHashSet(other.getDatatypeFilter()));
        this.setIndexHoles(null == other.getIndexHoles() ? null : Lists.newArrayList(other.getIndexHoles()));
        this.setProjectFields(null == other.getProjectFields() ? null : Sets.newHashSet(other.getProjectFields()));
        this.setBlacklistedFields(null == other.getBlacklistedFields() ? null : Sets.newHashSet(other.getBlacklistedFields()));
        this.setIndexedFields(null == other.getIndexedFields() ? null : Sets.newHashSet(other.getIndexedFields()));
        this.setNormalizedFields(null == other.getNormalizedFields() ? null : Sets.newHashSet(other.getNormalizedFields()));
        this.setDataTypes(null == other.getDataTypes() ? null : HashMultimap.create(other.getDataTypes()));
        this.setQueryFieldsDatatypes(null == other.getQueryFieldsDatatypes() ? null : HashMultimap.create(other.getQueryFieldsDatatypes()));
        this.setNormalizedFieldsDatatypes(null == other.getNormalizedFieldsDatatypes() ? null : HashMultimap.create(other.getNormalizedFieldsDatatypes()));
        this.setFieldToDiscreteIndexTypes(null == other.getFieldToDiscreteIndexTypes() ? null : Maps.newHashMap(other.getFieldToDiscreteIndexTypes()));
        this.setCompositeToFieldMap(null == other.getCompositeToFieldMap() ? null : ArrayListMultimap.create(other.getCompositeToFieldMap()));
        this.setCompositeTransitionDates(null == other.getCompositeTransitionDates() ? null : Maps.newHashMap(other.getCompositeTransitionDates()));
        this.setCompositeFieldSeparators(null == other.getCompositeFieldSeparators() ? null : Maps.newHashMap(other.getCompositeFieldSeparators()));
        this.setSortedUIDs(other.isSortedUIDs());
        this.setQueryTermFrequencyFields(null == other.getQueryTermFrequencyFields() ? null : Sets.newHashSet(other.getQueryTermFrequencyFields()));
        this.setTermFrequenciesRequired(other.isTermFrequenciesRequired());
        this.setLimitFields(null == other.getLimitFields() ? null : Sets.newHashSet(other.getLimitFields()));
        this.setLimitFieldsPreQueryEvaluation(other.isLimitFieldsPreQueryEvaluation());
        this.setLimitFieldsField(other.getLimitFieldsField());
        this.setHitList(other.isHitList());
        this.setTypeMetadataInHdfs(other.isTypeMetadataInHdfs());
        this.setDateIndexTimeTravel(other.isDateIndexTimeTravel());
        this.setBeginDateCap(other.getBeginDateCap());
        this.setFailOutsideValidDateRange(other.isFailOutsideValidDateRange());
        this.setRawTypes(other.isRawTypes());
        this.setMinSelectivity(other.getMinSelectivity());
        this.setIncludeDataTypeAsField(other.getIncludeDataTypeAsField());
        this.setIncludeRecordId(other.getIncludeRecordId());
        this.setIncludeHierarchyFields(other.getIncludeHierarchyFields());
        this.setHierarchyFieldOptions(null == other.getHierarchyFieldOptions() ? null : Maps.newHashMap(other.getHierarchyFieldOptions()));
        this.setIncludeGroupingContext(other.getIncludeGroupingContext());
        this.setDocumentPermutations(null == other.getDocumentPermutations() ? null : Lists.newArrayList(other.getDocumentPermutations()));
        this.setFilterMaskedValues(other.getFilterMaskedValues());
        this.setReducedResponse(other.isReducedResponse());
        this.setAllowShortcutEvaluation(other.getAllowShortcutEvaluation());
        this.setBypassAccumulo(other.getBypassAccumulo());
        this.setSpeculativeScanning(other.getSpeculativeScanning());
        this.setDisableEvaluation(other.isDisableEvaluation());
        this.setContainsIndexOnlyTerms(other.isContainsIndexOnlyTerms());
        this.setContainsCompositeTerms(other.isContainsCompositeTerms());
        this.setAllowFieldIndexEvaluation(other.isAllowFieldIndexEvaluation());
        this.setAllowTermFrequencyLookup(other.isAllowTermFrequencyLookup());
        this.setReturnType(other.getReturnType());
        this.setEventPerDayThreshold(other.getEventPerDayThreshold());
        this.setShardsPerDayThreshold(other.getShardsPerDayThreshold());
        this.setMaxTermThreshold(other.getMaxTermThreshold());
        this.setMaxDepthThreshold(other.getMaxDepthThreshold());
        this.setMaxUnfieldedExpansionThreshold(other.getMaxUnfieldedExpansionThreshold());
        this.setMaxValueExpansionThreshold(other.getMaxValueExpansionThreshold());
        this.setMaxOrExpansionThreshold(other.getMaxOrExpansionThreshold());
        this.setMaxOrExpansionFstThreshold(other.getMaxOrExpansionFstThreshold());
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
        this.setQueryModel(null == other.getQueryModel() ? null : new QueryModel(other.getQueryModel()));
        this.setModelName(other.getModelName());
        this.setModelTableName(other.getModelTableName());
        this.setLimitTermExpansionToModel(other.isExpansionLimitedToModelContents());
        this.setQuery(null == other.getQuery() ? null : other.getQuery().duplicate(other.getQuery().getQueryName()));
        this.setCompressServerSideResults(other.isCompressServerSideResults());
        this.setIndexOnlyFilterFunctionsEnabled(other.isIndexOnlyFilterFunctionsEnabled());
        this.setCompositeFilterFunctionsEnabled(other.isCompositeFilterFunctionsEnabled());
        this.setGroupFieldsBatchSize(other.getGroupFieldsBatchSize());
        this.setAccrueStats(other.getAccrueStats());
        this.setGroupFields(null == other.getGroupFields() ? null : Sets.newHashSet(other.getGroupFields()));
        this.setUniqueFields(null == other.getUniqueFields() ? null : Sets.newHashSet(other.getUniqueFields()));
        this.setCacheModel(other.getCacheModel());
        this.setTrackSizes(other.isTrackSizes());
        this.setContentFieldNames(null == other.getContentFieldNames() ? null : Lists.newArrayList(other.getContentFieldNames()));
    }
    
    /**
     * Delegates deep copy work to appropriate constructor, sets additional values specific to the provided ShardQueryLogic
     *
     * @param logic
     *            - a ShardQueryLogic instance or subclass
     */
    public ShardQueryConfiguration(ShardQueryLogic logic) {
        this(logic.getConfig());
        
        // Setters that would have been picked up in a super(logic) call
        this.setTableName(logic.getTableName());
        this.setMaxRowsToScan(logic.getMaxRowsToScan());
        this.setUndisplayedVisibilities(logic.getUndisplayedVisibilities());
        this.setBaseIteratorPriority(logic.getBaseIteratorPriority());
    }
    
    /**
     * Factory method that instantiates an fresh ShardQueryConfiguration
     *
     * @return - a clean ShardQueryConfiguration
     */
    public static ShardQueryConfiguration create() {
        return new ShardQueryConfiguration();
    }
    
    /**
     * Factory method that returns a deep copy of the provided ShardQueryConfiguration
     *
     * @param other
     *            - another instance of a ShardQueryConfiguration
     * @return - copy of provided ShardQueryConfiguration
     */
    public static ShardQueryConfiguration create(ShardQueryConfiguration other) {
        return new ShardQueryConfiguration(other);
    }
    
    /**
     * Factory method that creates a ShardQueryConfiguration deep copy from a ShardQueryLogic
     *
     * @param shardQueryLogic
     *            - a configured ShardQueryLogic
     * @return - a ShardQueryConfiguration
     */
    public static ShardQueryConfiguration create(ShardQueryLogic shardQueryLogic) {
        
        ShardQueryConfiguration config = create(shardQueryLogic.getConfig());
        
        // Lastly, honor overrides passed in via query parameters
        Set<QueryImpl.Parameter> parameterSet = config.getQuery().getParameters();
        for (QueryImpl.Parameter parameter : parameterSet) {
            String name = parameter.getParameterName();
            String value = parameter.getParameterValue();
            if (name.equals(QueryParameters.HIT_LIST)) {
                config.setHitList(Boolean.parseBoolean(value));
            }
            if (name.equals(QueryParameters.TYPE_METADATA_IN_HDFS)) {
                config.setTypeMetadataInHdfs(Boolean.parseBoolean(value));
            }
            if (name.equals(QueryParameters.DATE_INDEX_TIME_TRAVEL)) {
                config.setDateIndexTimeTravel(Boolean.parseBoolean(value));
            }
            if (name.equals(QueryParameters.PARAMETER_MODEL_NAME)) {
                config.setMetadataTableName(value);
            }
        }
        
        return config;
    }
    
    /**
     * Factory method that creates a ShardQueryConfiguration from a ShardQueryLogic and a Query
     *
     * @param shardQueryLogic
     *            - a configured ShardQueryLogic
     * @param query
     *            - a configured Query object
     * @return - a ShardQueryConfiguration
     */
    public static ShardQueryConfiguration create(ShardQueryLogic shardQueryLogic, Query query) {
        ShardQueryConfiguration config = create(shardQueryLogic);
        config.setQuery(query);
        return config;
    }
    
    /**
     * @return - the accumulo password
     */
    public String getAccumuloPassword() {
        return this.accumuloPassword;
    }
    
    /**
     * Sets configured password for accumulo access
     *
     * @param password
     */
    public void setAccumuloPassword(String password) {
        this.accumuloPassword = password;
        
    }
    
    /**
     * A convenience method that determines whether we can handle when we have exceeded the value threshold on some node. We can handle this if the Ivarators
     * can be used which required a hadoop config and a base hdfs cache directory.
     *
     * @return
     */
    public boolean canHandleExceededValueThreshold() {
        return this.hdfsSiteConfigURLs != null && (null != this.ivaratorCacheBaseURIs && !this.ivaratorCacheBaseURIs.isEmpty());
    }
    
    /**
     * A convenience method that determines whether we can handle when we have exceeded the term threshold on some node. Currently we cannot.
     *
     * @return
     */
    public boolean canHandleExceededTermThreshold() {
        return false;
    }
    
    public String getShardTableName() {
        return shardTableName;
    }
    
    public void setShardTableName(String shardTableName) {
        setTableName(shardTableName);
    }
    
    @Override
    public void setTableName(String tableName) {
        super.setTableName(tableName);
        this.shardTableName = tableName;
    }
    
    public String getMetadataTableName() {
        return metadataTableName;
    }
    
    public void setMetadataTableName(String metadataTableName) {
        this.metadataTableName = metadataTableName;
    }
    
    public String getDateIndexTableName() {
        return dateIndexTableName;
    }
    
    public void setDateIndexTableName(String dateIndexTableName) {
        this.dateIndexTableName = dateIndexTableName;
    }
    
    public String getDefaultDateTypeName() {
        return defaultDateTypeName;
    }
    
    public void setDefaultDateTypeName(String defaultDateTypeName) {
        this.defaultDateTypeName = defaultDateTypeName;
    }
    
    public String getIndexTableName() {
        return indexTableName;
    }
    
    public void setIndexTableName(String indexTableName) {
        this.indexTableName = indexTableName;
    }
    
    public String getReverseIndexTableName() {
        return reverseIndexTableName;
    }
    
    public void setReverseIndexTableName(String reverseIndexTableName) {
        this.reverseIndexTableName = reverseIndexTableName;
    }
    
    public String getIndexStatsTableName() {
        return indexStatsTableName;
    }
    
    public void setIndexStatsTableName(String statsTableName) {
        this.indexStatsTableName = statsTableName;
    }
    
    public Integer getNumQueryThreads() {
        return numQueryThreads;
    }
    
    public void setNumQueryThreads(Integer numQueryThreads) {
        this.numQueryThreads = numQueryThreads;
    }
    
    public Integer getNumIndexLookupThreads() {
        return numLookupThreads;
    }
    
    public void setNumIndexLookupThreads(Integer numIndexLookupThreads) {
        this.numLookupThreads = numIndexLookupThreads;
    }
    
    public Integer getNumDateIndexThreads() {
        return numDateIndexThreads;
    }
    
    public void setNumDateIndexThreads(Integer numDateIndexThreads) {
        this.numDateIndexThreads = numDateIndexThreads;
    }
    
    public Integer getMaxDocScanTimeout() {
        return maxDocScanTimeout;
    }
    
    public void setMaxDocScanTimeout(Integer maxDocScanTimeout) {
        this.maxDocScanTimeout = maxDocScanTimeout;
    }
    
    public float getCollapseDatePercentThreshold() {
        return collapseDatePercentThreshold;
    }
    
    public void setCollapseDatePercentThreshold(float collapseDatePercentThreshold) {
        this.collapseDatePercentThreshold = collapseDatePercentThreshold;
    }
    
    public Boolean getFullTableScanEnabled() {
        return fullTableScanEnabled;
    }
    
    public void setFullTableScanEnabled(Boolean fullTableScanEnabled) {
        this.fullTableScanEnabled = fullTableScanEnabled;
    }
    
    public SimpleDateFormat getShardDateFormatter() {
        return shardDateFormatter;
    }
    
    public void setShardDateFormatter(SimpleDateFormat shardDateFormatter) {
        this.shardDateFormatter = shardDateFormatter;
    }
    
    public Set<String> getDatatypeFilter() {
        return datatypeFilter;
    }
    
    public void setDatatypeFilter(Set<String> typeFilter) {
        this.datatypeFilter = typeFilter;
    }
    
    public String getDatatypeFilterAsString() {
        return StringUtils.join(this.getDatatypeFilter(), Constants.PARAM_VALUE_SEP);
    }
    
    public Set<String> getProjectFields() {
        return projectFields;
    }
    
    public void setProjectFields(Set<String> projectFields) {
        this.projectFields = projectFields;
    }
    
    public String getProjectFieldsAsString() {
        return StringUtils.join(this.getProjectFields(), Constants.PARAM_VALUE_SEP);
    }
    
    public Set<String> getBlacklistedFields() {
        return blacklistedFields;
    }
    
    public void setBlacklistedFields(Set<String> blacklistedFields) {
        this.blacklistedFields = blacklistedFields;
    }
    
    public String getBlacklistedFieldsAsString() {
        return StringUtils.join(this.getBlacklistedFields(), Constants.PARAM_VALUE_SEP);
    }
    
    public Boolean getUseEnrichers() {
        return useEnrichers;
    }
    
    public void setUseEnrichers(Boolean useEnrichers) {
        this.useEnrichers = useEnrichers;
    }
    
    public List<String> getEnricherClassNames() {
        return enricherClassNames;
    }
    
    public void setEnricherClassNames(List<String> enricherClassNames) {
        this.enricherClassNames = enricherClassNames;
    }
    
    public String getEnricherClassNamesAsString() {
        return StringUtils.join(this.getEnricherClassNames(), Constants.PARAM_VALUE_SEP);
    }
    
    public boolean isTldQuery() {
        return tldQuery;
    }
    
    public void setTldQuery(boolean tldQuery) {
        this.tldQuery = tldQuery;
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
    
    public Boolean getUseFilters() {
        return useFilters;
    }
    
    public void setUseFilters(Boolean useFilters) {
        this.useFilters = useFilters;
    }
    
    /**
     * Add a filter option
     *
     * @param option
     *            filter option
     * @param value
     *            filter value
     */
    public void putFilterOptions(final String option, final String value) {
        if (StringUtils.isNotBlank(option) && StringUtils.isNotBlank(value))
            filterOptions.put(option, value);
    }
    
    /**
     * Add filter options
     *
     * @param options
     *            filter options
     */
    public void putFilterOptions(final Map<String,String> options) {
        if (null != options) {
            for (final Entry<String,String> entry : options.entrySet()) {
                putFilterOptions(entry.getKey(), entry.getValue());
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    public Map<String,String> getFilterOptions() {
        return Collections.unmodifiableMap(filterOptions);
    }
    
    public List<String> getFilterClassNames() {
        return filterClassNames;
    }
    
    @SuppressWarnings("unchecked")
    public void setFilterClassNames(List<String> filterClassNames) {
        this.filterClassNames = new ArrayList<>((filterClassNames != null ? filterClassNames : Collections.EMPTY_LIST));
    }
    
    /**
     * Gets any predicate-based filters to apply when iterating through the field index. These filters will be "anded" with the default data type filter, if
     * any, used to construct the IndexIterator, particularly via the TLDQueryIterator.
     *
     * @return list of predicate-implemented classnames
     *
     * @see QueryIterator
     * @see TLDQueryIterator
     */
    public List<String> getIndexFilteringClassNames() {
        return indexFilteringClassNames;
    }
    
    /**
     * Gets any predicate-based filters to apply when scanning through the field index. These filters will be "anded" with the default data type filter, if any,
     * used to construct the IndexIterator (particularly via the TLDQueryIterator).
     *
     * @param classNames
     *            the names of predicate-implemented classes to use when scanning the field index
     */
    @SuppressWarnings("unchecked")
    public void setIndexFilteringClassNames(List<String> classNames) {
        this.indexFilteringClassNames = new ArrayList<>((classNames != null ? classNames : Collections.EMPTY_LIST));
    }
    
    public String getFilterClassNamesAsString() {
        return StringUtils.join(this.getFilterClassNames(), Constants.PARAM_VALUE_SEP);
    }
    
    public Class<? extends Type<?>> getDefaultType() {
        return defaultType;
    }
    
    public void setDefaultType(Class<? extends Type<?>> defaultType) {
        this.defaultType = defaultType;
    }
    
    public void setDefaultType(String className) {
        try {
            setDefaultType((Class<? extends Type<?>>) Class.forName(className));
        } catch (ClassNotFoundException ex) {
            log.warn("Class name: " + className + " not found, defaulting to NoOpNormalizer.class");
            setDefaultType(NoOpType.class);
        }
    }
    
    public Set<String> getNonEventKeyPrefixes() {
        return nonEventKeyPrefixes;
    }
    
    public void setNonEventKeyPrefixes(Collection<String> nonEventKeyPrefixes) {
        if (null == nonEventKeyPrefixes) {
            this.nonEventKeyPrefixes = new HashSet<>();
        } else {
            this.nonEventKeyPrefixes = new HashSet<>(nonEventKeyPrefixes);
        }
    }
    
    public String getNonEventKeyPrefixesAsString() {
        return StringUtils.join(this.getNonEventKeyPrefixes(), Constants.PARAM_VALUE_SEP);
    }
    
    public Set<String> getUnevaluatedFields() {
        return unevaluatedFields;
    }
    
    public void setUnevaluatedFields(Collection<String> unevaluatedFields) {
        if (null == unevaluatedFields) {
            this.unevaluatedFields = new HashSet<>();
        } else {
            this.unevaluatedFields = new HashSet<>(unevaluatedFields);
        }
    }
    
    /**
     * Join unevaluated fields together on comma
     *
     * @return
     */
    public String getUnevaluatedFieldsAsString() {
        return StringUtils.join(this.unevaluatedFields, Constants.PARAM_VALUE_SEP);
    }
    
    public void setUnevaluatedFields(String unevaluatedFieldList) {
        this.setUnevaluatedFields(Arrays.asList(unevaluatedFieldList.split(PARAM_VALUE_SEP_STR)));
    }
    
    public int getEventPerDayThreshold() {
        return eventPerDayThreshold;
    }
    
    public void setEventPerDayThreshold(int eventPerDayThreshold) {
        this.eventPerDayThreshold = eventPerDayThreshold;
    }
    
    public int getShardsPerDayThreshold() {
        return shardsPerDayThreshold;
    }
    
    public void setShardsPerDayThreshold(int shardsPerDayThreshold) {
        this.shardsPerDayThreshold = shardsPerDayThreshold;
    }
    
    public int getMaxTermThreshold() {
        return maxTermThreshold;
    }
    
    public void setMaxTermThreshold(int maxTermThreshold) {
        this.maxTermThreshold = maxTermThreshold;
    }
    
    public int getMaxDepthThreshold() {
        return maxDepthThreshold;
    }
    
    public void setMaxDepthThreshold(int maxDepthThreshold) {
        this.maxDepthThreshold = maxDepthThreshold;
    }
    
    public int getMaxUnfieldedExpansionThreshold() {
        return maxUnfieldedExpansionThreshold;
    }
    
    public void setMaxUnfieldedExpansionThreshold(int maxUnfieldedExpansionThreshold) {
        this.maxUnfieldedExpansionThreshold = maxUnfieldedExpansionThreshold;
    }
    
    public int getMaxValueExpansionThreshold() {
        return maxValueExpansionThreshold;
    }
    
    public void setMaxValueExpansionThreshold(int maxValueExpansionThreshold) {
        this.maxValueExpansionThreshold = maxValueExpansionThreshold;
    }
    
    public int getMaxScannerBatchSize() {
        return this.maxScannerBatchSize;
    }
    
    public void setMaxScannerBatchSize(final int size) {
        this.maxScannerBatchSize = size;
    }
    
    public int getMaxIndexBatchSize() {
        return this.maxIndexBatchSize;
    }
    
    public void setMaxIndexBatchSize(final int size) {
        if (size >= 1)
            this.maxIndexBatchSize = size;
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
    
    public boolean isExpandAllTerms() {
        return expandAllTerms;
    }
    
    public void setExpandAllTerms(boolean expandAllTerms) {
        this.expandAllTerms = expandAllTerms;
    }
    
    /**
     * Creates string, mapping the normalizers used for a field to pass to the QueryEvaluator through the options map.
     *
     * @return FIELDNAME1:normalizer.class;FIELDNAME2:normalizer.class;
     */
    public String getIndexedFieldDataTypesAsString() {
        
        if (null == this.getIndexedFields() || this.getIndexedFields().isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        Multimap<String,Type<?>> mmap = getQueryFieldsDatatypes();
        for (String fieldName : mmap.keySet()) {
            sb.append(fieldName);
            for (Type<?> tn : mmap.get(fieldName)) {
                sb.append(":");
                sb.append(tn.getClass().getName());
            }
            sb.append(";");
        }
        return sb.toString();
    }
    
    public String getNormalizedFieldNormalizersAsString() {
        
        if (null == this.getNormalizedFields() || this.getNormalizedFields().isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        Multimap<String,Type<?>> mmap = getNormalizedFieldsDatatypes();
        for (String fieldName : mmap.keySet()) {
            sb.append(fieldName);
            for (Type<?> tn : mmap.get(fieldName)) {
                sb.append(":");
                sb.append(tn.getClass().getName());
            }
            sb.append(";");
        }
        return sb.toString();
    }
    
    public Set<String> getIndexedFields() {
        return indexedFields;
    }
    
    public void setIndexedFields(Multimap<String,Type<?>> indexedFieldsAndTypes) {
        this.indexedFields = Sets.newHashSet(indexedFieldsAndTypes.keySet());
        for (Entry<String,Type<?>> entry : indexedFieldsAndTypes.entries()) {
            if (entry.getValue() instanceof UnindexType) {
                this.indexedFields.remove(entry.getKey());
            }
        }
    }
    
    public void setIndexedFields(Set<String> indexedFields) {
        this.indexedFields = Sets.newHashSet(indexedFields);
    }
    
    public Set<String> getNormalizedFields() {
        return normalizedFields;
    }
    
    public void setNormalizedFields(Set<String> normalizedFields) {
        this.normalizedFields = normalizedFields;
    }
    
    public Multimap<String,Type<?>> getDataTypes() {
        if (dataTypes == null) {
            dataTypes = HashMultimap.create();
        }
        if (dataTypes.isEmpty()) {
            dataTypes.putAll(getQueryFieldsDatatypes());
            dataTypes.putAll(getNormalizedFieldsDatatypes());
        }
        return dataTypes;
    }
    
    public void setDataTypes(Multimap<String,Type<?>> dataTypes) {
        log.warn("setDataTypes to " + dataTypes);
        this.dataTypes = dataTypes;
    }
    
    public Multimap<String,Type<?>> getQueryFieldsDatatypes() {
        return queryFieldsDatatypes;
    }
    
    public void setQueryFieldsDatatypes(Multimap<String,Type<?>> queryFieldsDatatypes) {
        this.queryFieldsDatatypes = queryFieldsDatatypes;
    }
    
    public Map<String,DiscreteIndexType<?>> getFieldToDiscreteIndexTypes() {
        return fieldToDiscreteIndexTypes;
    }
    
    public void setFieldToDiscreteIndexTypes(Map<String,DiscreteIndexType<?>> fieldToDiscreteIndexTypes) {
        this.fieldToDiscreteIndexTypes = fieldToDiscreteIndexTypes;
    }
    
    public Multimap<String,String> getCompositeToFieldMap() {
        return compositeToFieldMap;
    }
    
    public void setCompositeToFieldMap(Multimap<String,String> compositeToFieldMap) {
        this.compositeToFieldMap = compositeToFieldMap;
    }
    
    public Map<String,Date> getCompositeTransitionDates() {
        return compositeTransitionDates;
    }
    
    public void setCompositeTransitionDates(Map<String,Date> compositeTransitionDates) {
        this.compositeTransitionDates = compositeTransitionDates;
    }
    
    public Map<String,String> getCompositeFieldSeparators() {
        return compositeFieldSeparators;
    }
    
    public void setCompositeFieldSeparators(Map<String,String> compositeFieldSeparators) {
        this.compositeFieldSeparators = compositeFieldSeparators;
    }
    
    public Multimap<String,Type<?>> getNormalizedFieldsDatatypes() {
        return normalizedFieldsDatatypes;
    }
    
    public void setNormalizedFieldsDatatypes(Multimap<String,Type<?>> normalizedFieldsDatatypes) {
        this.normalizedFieldsDatatypes = normalizedFieldsDatatypes;
        this.normalizedFields = Sets.newHashSet(normalizedFieldsDatatypes.keySet());
    }
    
    public Set<String> getLimitFields() {
        return limitFields;
    }
    
    public void setLimitFields(Set<String> limitFields) {
        this.limitFields = limitFields;
    }
    
    public String getLimitFieldsAsString() {
        return StringUtils.join(this.getLimitFields(), Constants.PARAM_VALUE_SEP);
    }
    
    public boolean isLimitFieldsPreQueryEvaluation() {
        return limitFieldsPreQueryEvaluation;
    }
    
    public void setLimitFieldsPreQueryEvaluation(boolean limitFieldsPreQueryEvaluation) {
        this.limitFieldsPreQueryEvaluation = limitFieldsPreQueryEvaluation;
    }
    
    public String getLimitFieldsField() {
        return limitFieldsField;
    }
    
    public void setLimitFieldsField(String limitFieldsField) {
        this.limitFieldsField = limitFieldsField;
    }
    
    public boolean isDateIndexTimeTravel() {
        return dateIndexTimeTravel;
    }
    
    public void setDateIndexTimeTravel(boolean dateIndexTimeTravel) {
        this.dateIndexTimeTravel = dateIndexTimeTravel;
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
    
    public Set<String> getGroupFields() {
        return groupFields;
    }
    
    public void setGroupFields(Set<String> groupFields) {
        this.groupFields = groupFields;
    }
    
    public String getGroupFieldsAsString() {
        return StringUtils.join(this.getGroupFields(), Constants.PARAM_VALUE_SEP);
    }
    
    public int getGroupFieldsBatchSize() {
        return groupFieldsBatchSize;
    }
    
    public void setGroupFieldsBatchSize(int groupFieldsBatchSize) {
        this.groupFieldsBatchSize = groupFieldsBatchSize;
    }
    
    public String getGroupFieldsBatchSizeAsString() {
        return "" + groupFieldsBatchSize;
    }
    
    public Set<String> getUniqueFields() {
        return uniqueFields;
    }
    
    public void setUniqueFields(Set<String> uniqueFields) {
        this.uniqueFields = uniqueFields;
    }
    
    public String getUniqueFieldsAsString() {
        return StringUtils.join(this.getUniqueFields(), Constants.PARAM_VALUE_SEP);
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
    
    public boolean isRawTypes() {
        return this.rawTypes;
    }
    
    public void setRawTypes(boolean rawTypes) {
        this.rawTypes = rawTypes;
    }
    
    public double getMinSelectivity() {
        return minSelectivity;
    }
    
    public void setMinSelectivity(double minSelectivity) {
        this.minSelectivity = minSelectivity;
    }
    
    /**
     * Checks for non-null, sane values for the configured values
     *
     * @return True if all of the encapsulated values have legitimate values, otherwise false
     */
    @Override
    public boolean canRunQuery() {
        if (!super.canRunQuery()) {
            log.warn("GenericQueryConfiguration.canRunQuery() returned false. Cannot run query!");
            return false;
        }
        
        return true;
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
    
    public boolean getIncludeGroupingContext() {
        return includeGroupingContext;
    }
    
    public void setIncludeGroupingContext(boolean withContextOption) {
        this.includeGroupingContext = withContextOption;
    }
    
    public List<String> getDocumentPermutations() {
        return documentPermutations;
    }
    
    public void setDocumentPermutations(List<String> documentPermutations) {
        // validate we have instances of DocumentPermutation
        for (String perm : documentPermutations) {
            try {
                Class<?> clazz = Class.forName(perm);
                if (!DocumentPermutation.class.isAssignableFrom(clazz)) {
                    throw new IllegalArgumentException("Unable to load " + perm + " as a DocumentPermutation");
                }
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Unable to load " + perm + " as a DocumentPermutation");
            }
        }
        this.documentPermutations = documentPermutations;
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
    
    public boolean isDisableIndexOnlyDocuments() {
        return disableIndexOnlyDocuments;
    }
    
    public void setDisableIndexOnlyDocuments(boolean disableIndexOnlyDocuments) {
        this.disableIndexOnlyDocuments = disableIndexOnlyDocuments;
    }
    
    public boolean isContainsIndexOnlyTerms() {
        return containsIndexOnlyTerms;
    }
    
    public void setContainsIndexOnlyTerms(boolean containsIndexOnlyTerms) {
        this.containsIndexOnlyTerms = containsIndexOnlyTerms;
    }
    
    public boolean isContainsCompositeTerms() {
        return containsCompositeTerms;
    }
    
    public void setContainsCompositeTerms(boolean containsCompositeTerms) {
        this.containsCompositeTerms = containsCompositeTerms;
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
    
    public boolean isAllTermsIndexOnly() {
        return allTermsIndexOnly;
    }
    
    public void setAllTermsIndexOnly(boolean allTermsIndexOnly) {
        this.allTermsIndexOnly = allTermsIndexOnly;
    }
    
    public QueryModel getQueryModel() {
        return queryModel;
    }
    
    public void setQueryModel(QueryModel queryModel) {
        this.queryModel = queryModel;
    }
    
    public String getModelName() {
        return modelName;
    }
    
    public void setModelName(String modelName) {
        this.modelName = modelName;
    }
    
    public String getModelTableName() {
        return modelTableName;
    }
    
    public void setModelTableName(String modelTableName) {
        this.modelTableName = modelTableName;
    }
    
    public ReturnType getReturnType() {
        return returnType;
    }
    
    public void setReturnType(ReturnType returnType) {
        this.returnType = returnType;
    }
    
    public QueryStopwatch getTimers() {
        return timers;
    }
    
    public Query getQuery() {
        return query;
    }
    
    public void setQuery(Query query) {
        this.query = query;
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
     * Sets a value indicating whether index-only filter functions (e.g., #INCLUDE, #EXCLUDE) should be enabled. If true, the use of such filters can
     * potentially consume a LOT of memory.
     *
     * @param enabled
     *            flags whether or not to enable index-only filter functions
     */
    public void setIndexOnlyFilterFunctionsEnabled(boolean enabled) {
        this.indexOnlyFilterFunctionsEnabled = enabled;
    }
    
    public boolean isCompositeFilterFunctionsEnabled() {
        return compositeFilterFunctionsEnabled;
    }
    
    public void setCompositeFilterFunctionsEnabled(boolean compositeFilterFunctionsEnabled) {
        this.compositeFilterFunctionsEnabled = compositeFilterFunctionsEnabled;
    }
    
    public List<String> getRealmSuffixExclusionPatterns() {
        return realmSuffixExclusionPatterns;
    }
    
    public void setRealmSuffixExclusionPatterns(List<String> realmSuffixExclusionPatterns) {
        this.realmSuffixExclusionPatterns = realmSuffixExclusionPatterns;
    }
    
    public Set<String> getQueryTermFrequencyFields() {
        return queryTermFrequencyFields;
    }
    
    public void setQueryTermFrequencyFields(Set<String> queryTermFrequencyFields) {
        this.queryTermFrequencyFields = queryTermFrequencyFields;
    }
    
    public boolean isTermFrequenciesRequired() {
        return termFrequenciesRequired;
    }
    
    public void setTermFrequenciesRequired(boolean termFrequenciesRequired) {
        this.termFrequenciesRequired = termFrequenciesRequired;
    }
    
    public void setLimitTermExpansionToModel(boolean shouldLimitTermExpansionToModel) {
        this.shouldLimitTermExpansionToModel = shouldLimitTermExpansionToModel;
    }
    
    public boolean isExpansionLimitedToModelContents() {
        return shouldLimitTermExpansionToModel;
    }
    
    public long getMaxIndexScanTimeMillis() {
        return maxIndexScanTimeMillis;
    }
    
    public void setMaxIndexScanTimeMillis(long maxTime) {
        this.maxIndexScanTimeMillis = maxTime;
    }
    
    public boolean getCollapseUids() {
        return collapseUids;
    }
    
    public void setCollapseUids(boolean collapseUids) {
        this.collapseUids = collapseUids;
    }
    
    public boolean getSequentialScheduler() {
        return sequentialScheduler;
    }
    
    public void setSequentialScheduler(boolean sequentialScheduler) {
        this.sequentialScheduler = sequentialScheduler;
    }
    
    public boolean getLimitAnyFieldLookups() {
        return limitAnyFieldLookups;
    }
    
    public void setLimitAnyFieldLookups(boolean limitAnyFieldLookups) {
        this.limitAnyFieldLookups = limitAnyFieldLookups;
    }
    
    public boolean getAllowShortcutEvaluation() {
        return allowShortcutEvaluation;
    }
    
    public void setAllowShortcutEvaluation(boolean allowShortcutEvaluation) {
        this.allowShortcutEvaluation = allowShortcutEvaluation;
    }
    
    public boolean getBypassAccumulo() {
        return bypassAccumulo;
    }
    
    public void setBypassAccumulo(boolean bypassAccumulo) {
        this.bypassAccumulo = bypassAccumulo;
    }
    
    public boolean getAccrueStats() {
        return accrueStats;
    }
    
    public void setAccrueStats(boolean accrueStats) {
        this.accrueStats = accrueStats;
        
    }
    
    public List<IndexHole> getIndexHoles() {
        return indexHoles;
    }
    
    public void setIndexHoles(List<IndexHole> indexHoles) {
        this.indexHoles = indexHoles;
    }
    
    public boolean getCollectTimingDetails() {
        return collectTimingDetails;
    }
    
    public void setCollectTimingDetails(boolean collectTimingDetails) {
        this.collectTimingDetails = collectTimingDetails;
        
    }
    
    public boolean getLogTimingDetails() {
        return logTimingDetails;
    }
    
    public void setLogTimingDetails(boolean logTimingDetails) {
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
    
    public boolean isCleanupShardsAndDaysQueryHints() {
        return cleanupShardsAndDaysQueryHints;
    }
    
    public void setCleanupShardsAndDaysQueryHints(boolean cleanupShardsAndDaysQueryHints) {
        this.cleanupShardsAndDaysQueryHints = cleanupShardsAndDaysQueryHints;
    }
    
    public AtomicInteger getFstCount() {
        return fstCount;
    }
    
    public void setFstCount(AtomicInteger fstCount) {
        this.fstCount = fstCount;
    }
    
    public boolean getCacheModel() {
        return cacheModel;
    }
    
    public void setCacheModel(boolean cacheModel) {
        this.cacheModel = cacheModel;
    }
    
    public boolean isBypassExecutabilityCheck() {
        return bypassExecutabilityCheck;
    }
    
    public void setBypassExecutabilityCheck() {
        bypassExecutabilityCheck = true;
    }
    
    public void setBypassExecutabilityCheck(boolean bypassExecutabilityCheck) {
        this.bypassExecutabilityCheck = bypassExecutabilityCheck;
    }
    
    public boolean getBackoffEnabled() {
        return backoffEnabled;
    }
    
    public void setBackoffEnabled(boolean backoffEnabled) {
        this.backoffEnabled = backoffEnabled;
    }
    
    public boolean getUnsortedUIDsEnabled() {
        return unsortedUIDsEnabled;
    }
    
    public void setUnsortedUIDsEnabled(boolean unsortedUIDsEnabled) {
        this.unsortedUIDsEnabled = unsortedUIDsEnabled;
    }
    
    public boolean getSpeculativeScanning() {
        return speculativeScanning;
    }
    
    public void setSpeculativeScanning(boolean speculativeScanning) {
        this.speculativeScanning = speculativeScanning;
    }
    
    public boolean getSerializeQueryIterator() {
        return serializeQueryIterator;
    }
    
    public void setSerializeQueryIterator(boolean serializeQueryIterator) {
        this.serializeQueryIterator = serializeQueryIterator;
    }
    
    public boolean isSortedUIDs() {
        return sortedUIDs;
    }
    
    public void setSortedUIDs(boolean sortedUIDs) {
        this.sortedUIDs = sortedUIDs;
    }
    
    public long getYieldThresholdMs() {
        return yieldThresholdMs;
    }
    
    public void setYieldThresholdMs(long yieldThresholdMs) {
        this.yieldThresholdMs = yieldThresholdMs;
    }
    
    public boolean isTrackSizes() {
        return trackSizes;
    }
    
    public void setTrackSizes(boolean trackSizes) {
        this.trackSizes = trackSizes;
    }
    
    public List<String> getContentFieldNames() {
        return contentFieldNames;
    }
    
    public void setContentFieldNames(List<String> contentFieldNames) {
        this.contentFieldNames = contentFieldNames;
    }
}
