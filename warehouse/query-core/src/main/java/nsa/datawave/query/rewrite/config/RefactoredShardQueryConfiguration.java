package nsa.datawave.query.rewrite.config;

import com.google.common.collect.*;
import nsa.datawave.data.type.NoOpType;
import nsa.datawave.data.type.Type;
import nsa.datawave.query.QueryParameters;
import nsa.datawave.query.model.QueryModel;
import nsa.datawave.query.rewrite.Constants;
import nsa.datawave.query.rewrite.DocumentSerialization;
import nsa.datawave.query.rewrite.DocumentSerialization.ReturnType;
import nsa.datawave.query.rewrite.UnindexType;
import nsa.datawave.query.rewrite.iterator.PowerSet;
import nsa.datawave.query.rewrite.iterator.QueryIterator;
import nsa.datawave.query.rewrite.tld.TLDQueryIterator;
import nsa.datawave.query.rewrite.util.QueryStopwatch;
import nsa.datawave.query.util.CompositeNameAndIndex;
import nsa.datawave.webservice.query.Query;
import nsa.datawave.webservice.query.QueryImpl;
import nsa.datawave.webservice.query.configuration.GenericQueryConfiguration;
import org.apache.commons.collections.map.UnmodifiableMap;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 * A GenericQueryConfiguration implementation that provides the additional logic on top of the traditional query that is needed to run a DATAWAVE sharded
 * boolean-logic query
 * </p>
 * *
 * <p/>
 * <p>
 * Provides support for normalizers, enricher classes, filter classes, projection, and datatype filters, in addition to additional parameters also exposed in
 * the Webservice QueryTable interface
 * </p>
 * *
 * <p/>
 * <p>
 * This class can be initialized with an instance of a ShardQueryLogic or ShardQueryTable which will grab the already configured parameters from the Accumulo
 * Webservice QueryTable and apply them to this configuration object
 * </p>
 */
public class RefactoredShardQueryConfiguration extends GenericQueryConfiguration {
    @SuppressWarnings("unused")
    private static final long serialVersionUID = -4354990715046146110L;
    private static final Logger log = Logger.getLogger(RefactoredShardQueryConfiguration.class);
    
    public static final String PARAM_VALUE_SEP_STR = new String(new char[] {Constants.PARAM_VALUE_SEP});
    
    private String shardTableName = "shard";
    private String indexTableName = "shardIndex";
    private String reverseIndexTableName = "shardReverseIndex";
    private String metadataTableName = "DatawaveMetadata";
    private String dateIndexTableName = "DateIndex";
    private String indexStatsTableName = "indexStats";
    
    // should we cleanup the shards and days hints that are sent to the
    // tservers?
    private boolean cleanupShardsAndDaysQueryHints = true;
    
    // BatchScanner and query results options
    private Integer numQueryThreads = 100;
    private Integer numLookupThreads = 100;
    private Integer numDateIndexThreads = 100;
    private Integer maxDocScanTimeout = -1;
    
    // A counter used to uniquely identify FSTs generated in the
    // PushdownLargeFieldedListsVisitor
    private AtomicInteger fstCount = new AtomicInteger(0);
    
    // the percent shards marked when querying the date index after which the
    // shards are collapsed down to the entire day.
    private float collapseDatePercentThreshold = 0.99f;
    
    // Do we drop to a full-table scan if the query is not "optimized"
    private Boolean fullTableScanEnabled = false;
    
    private List<String> realmSuffixExclusionPatterns = null;
    
    // A default normalizer to use
    private Class<? extends Type<?>> defaultType = NoOpType.class;
    
    private SimpleDateFormat shardDateFormatter = new SimpleDateFormat("yyyyMMdd");
    
    // Enrichment properties
    private Boolean useEnrichers = true;
    private List<String> enricherClassNames = Collections.singletonList("nsa.datawave.query.enrich.DatawaveTermFrequencyEnricher");
    
    // is this a tld query
    protected boolean tldQuery = false;
    
    // Filter properties
    private Boolean useFilters = true;
    private List<String> filterClassNames = new ArrayList<>(Collections.singletonList("nsa.datawave.query.filter.DatawavePhraseFilter"));
    private List<String> indexFilteringClassNames = new ArrayList<>();
    protected Map<String,String> filterOptions = new HashMap<>();
    
    // Used for ignoring 'd' and 'tf' column family in `shard`
    private Set<String> nonEventKeyPrefixes = new HashSet<>(Arrays.asList("d", "tf"));
    
    // Default to having no unevaluatedFields
    private Set<String> unevaluatedFields = Collections.emptySet();
    
    // Filter results on datatypes
    private Set<String> datatypeFilter = PowerSet.instance();
    
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
    
    private Multimap<String,CompositeNameAndIndex> fieldToCompositeMap = ArrayListMultimap.create();
    private Multimap<String,String> compositeToFieldMap = ArrayListMultimap.create();
    private Multimap<String,String> currentCompositeToFieldMap = ArrayListMultimap.create();
    
    private boolean sortedUIDs = true;
    
    // The fields in the the query that are tf fields
    private Set<String> queryTermFrequencyFields = Collections.emptySet();
    
    // Limit count of returned values for arbitrary fields.
    private Set<String> limitFields = Collections.emptySet();
    
    private boolean hitList = false;
    
    private boolean typeMetadataInHdfs = false;
    
    private boolean dateIndexTimeTravel = false;
    
    // Cap (or fail if failOutsideValidDateRange) the begin date with this value (subtracted from Now). 0 or less disables this feature.
    private long beginDateCap = -1;
    
    private boolean failOutsideValidDateRange = false;
    
    private boolean rawTypes = false;
    
    // Used to choose how "selective" a term is (indexStats)
    private double minSelectivity = -1.0;
    // Used to add the event datatype to the event as an event field.
    private boolean includeDataTypeAsField = false;
    
    // Used to add the CHILD_COUNT, DESCENDANT_COUNT, HAS_CHILDREN and/or
    // PARENT_UID as event fields,
    // plus various options for output and optimization
    private boolean includeHierarchyFields = false;
    private Map<String,String> hierarchyFieldOptions = Collections.emptyMap();
    
    // Used to set the ShardEventEvaluating iterator INCLUDE_GROUPING_CONTEXT
    private boolean includeGroupingContext = false;
    
    // Used to filter out masked values when the unmasked value is available
    private boolean filterMaskedValues = true;
    
    private boolean reducedResponse = false;
    
    private volatile boolean allowShortcutEvaluation = true;
    
    private boolean bypassAccumulo = false;
    
    private boolean speculativeScanning = false;
    
    private boolean disableEvaluation = false;
    
    protected boolean disableIndexOnlyDocuments = false;
    
    private boolean containsIndexOnlyTerms = false;
    
    private boolean containsCompositeTerms = false;
    /**
     * By default enable field index only evaluation (aggregation of document post evaluation)
     */
    private boolean allowFieldIndexEvaluation = true;
    
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
    
    // Adding the ability to precache the query model for performance sake. If
    // this is null
    // then the query model will be pulled from the MetadataHelper
    private QueryModel queryModel = null;
    
    private String modelName = null;
    private String modelTableName = null;
    
    // limit expanded terms to only those fields that are defined in the chosen
    // model. drop others
    private boolean shouldLimitTermExpansionToModel = false;
    
    private Query query = null;
    
    protected QueryStopwatch timers = new QueryStopwatch();
    
    private boolean compressServerSideResults = false;
    
    private boolean indexOnlyFilterFunctionsEnabled = false;
    
    private boolean compositeFilterFunctionsEnabled = false;
    
    protected int maxScannerBatchSize = 2000;
    
    /**
     * Index batch size is the size of results use for each index lookup
     */
    protected int maxIndexBatchSize = 200;
    
    protected boolean allTermsIndexOnly;
    
    protected String password;
    
    protected long maxIndexScanTimeMillis = Long.MAX_VALUE;
    
    protected boolean collapseUids = false;
    
    protected boolean sequentialScheduler = false;
    
    protected boolean collectTimingDetails = false;
    
    protected boolean logTimingDetails = false;
    
    protected boolean sendTimingToStatsd = true;
    protected String statsdHost = "localhost";
    protected int statsdPort = 8125;
    protected long statsdLatencyMs = 5000;
    protected int statsdMaxQueueSize = 500;
    protected long statsdKeepAliveMs = 5000;
    
    protected boolean limitAnyFieldLookups = true;
    
    protected Collection<String> groupFields;
    
    private boolean accrueStats = false;
    
    private boolean cacheModel = false;
    
    protected boolean bypassExecutabilityCheck = false;
    
    /**
     * Allows for back off of scanners.
     */
    protected boolean backoffEnabled = false;
    
    /**
     * Allows for the unsorted UIDs feature (see SortedUIDsRequiredVisitor)
     */
    protected boolean unsortedUIDsEnabled = true;
    
    /**
     * Allows us to serialize query iterator
     */
    protected boolean serializeQueryIterator = false;
    
    /**
     * Used to enable the SourceThreadTrackingIterator on the tservers
     */
    protected boolean debugMultithreadedSources = false;
    
    public RefactoredShardQueryConfiguration() {
        query = new QueryImpl();
    }
    
    /**
     * Sets configured password
     *
     * @param password
     */
    public void setAccumuloPassword(String password) {
        this.password = password;
        
    }
    
    /**
     * @return
     */
    public String getAccumuloPassword() {
        return new String(this.password);
    }
    
    /**
     * A convenience method that determines whether we can handle when we have exceeded the value threshold on some node. We can handle this if the Ivarators
     * can be used which required a hadoop config and a base hdfs cache directory.
     *
     * @return
     */
    public boolean canHandleExceededValueThreshold() {
        return this.hdfsSiteConfigURLs != null && (null != this.ivaratorCacheBaseURIs && this.ivaratorCacheBaseURIs.size() > 0);
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
        this.shardTableName = shardTableName;
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
    
    public String getDatatypeFilterAsString() {
        return org.apache.commons.lang.StringUtils.join(this.getDatatypeFilter(), Constants.PARAM_VALUE_SEP);
    }
    
    public void setDatatypeFilter(Set<String> typeFilter) {
        this.datatypeFilter = typeFilter;
    }
    
    public Set<String> getProjectFields() {
        return projectFields;
    }
    
    public String getProjectFieldsAsString() {
        return org.apache.commons.lang.StringUtils.join(this.getProjectFields(), Constants.PARAM_VALUE_SEP);
    }
    
    public void setProjectFields(Set<String> projectFields) {
        this.projectFields = projectFields;
    }
    
    public Set<String> getBlacklistedFields() {
        return blacklistedFields;
    }
    
    public String getBlacklistedFieldsAsString() {
        return StringUtils.join(this.getBlacklistedFields(), Constants.PARAM_VALUE_SEP);
    }
    
    public void setBlacklistedFields(Set<String> blacklistedFields) {
        this.blacklistedFields = blacklistedFields;
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
    
    public String getEnricherClassNamesAsString() {
        return StringUtils.join(this.getEnricherClassNames(), Constants.PARAM_VALUE_SEP);
    }
    
    public void setEnricherClassNames(List<String> enricherClassNames) {
        this.enricherClassNames = enricherClassNames;
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
        return UnmodifiableMap.decorate(filterOptions);
    }
    
    public List<String> getFilterClassNames() {
        return filterClassNames;
    }
    
    @SuppressWarnings("unchecked")
    public void setFilterClassNames(List<String> filterClassNames) {
        this.filterClassNames = new ArrayList<String>((filterClassNames != null ? filterClassNames : Collections.EMPTY_LIST));
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
        this.indexFilteringClassNames = new ArrayList<String>((classNames != null ? classNames : Collections.EMPTY_LIST));
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
    
    public Set<String> getNonEventKeyPrefixes() {
        return nonEventKeyPrefixes;
    }
    
    public String getNonEventKeyPrefixesAsString() {
        return StringUtils.join(this.getNonEventKeyPrefixes(), Constants.PARAM_VALUE_SEP);
    }
    
    public void setNonEventKeyPrefixes(Collection<String> nonEventKeyPrefixes) {
        if (null == nonEventKeyPrefixes) {
            this.nonEventKeyPrefixes = new HashSet<>();
        } else {
            this.nonEventKeyPrefixes = new HashSet<>(nonEventKeyPrefixes);
        }
    }
    
    public Set<String> getUnevaluatedFields() {
        return unevaluatedFields;
    }
    
    public String getUnevaluatedFieldsAsString() {
        return StringUtils.join(this.unevaluatedFields, Constants.PARAM_VALUE_SEP);
    }
    
    public void setUnevaluatedFields(Collection<String> unevaluatedFields) {
        if (null == unevaluatedFields) {
            this.unevaluatedFields = new HashSet<>();
        } else {
            this.unevaluatedFields = new HashSet<>(unevaluatedFields);
        }
    }
    
    public int getEventPerDayThreshold() {
        return eventPerDayThreshold;
    }
    
    public int getShardsPerDayThreshold() {
        return shardsPerDayThreshold;
    }
    
    public void setEventPerDayThreshold(int eventPerDayThreshold) {
        this.eventPerDayThreshold = eventPerDayThreshold;
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
    
    public void setMaxScannerBatchSize(final int size) {
        this.maxScannerBatchSize = size;
    }
    
    public int getMaxScannerBatchSize() {
        return this.maxScannerBatchSize;
    }
    
    public void setMaxIndexBatchSize(final int size) {
        if (size >= 1)
            this.maxIndexBatchSize = size;
    }
    
    public int getMaxIndexBatchSize() {
        return this.maxIndexBatchSize;
    }
    
    public void setMaxValueExpansionThreshold(int maxValueExpansionThreshold) {
        this.maxValueExpansionThreshold = maxValueExpansionThreshold;
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
        
        if (null == this.getNormalizedFields()) {
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
    
    public void setIndexedFields(Set<String> indexedFields) {
        this.indexedFields = Sets.newHashSet(indexedFields);
    }
    
    public void setIndexedFields(Multimap<String,Type<?>> indexedFieldsAndTypes) {
        this.indexedFields = Sets.newHashSet(indexedFieldsAndTypes.keySet());
        for (Entry<String,Type<?>> entry : indexedFieldsAndTypes.entries()) {
            if (entry.getValue() instanceof UnindexType) {
                this.indexedFields.remove(entry.getKey());
            }
        }
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
    
    public void setQueryFieldsDatatypes(Multimap<String,Type<?>> queryFieldsDatatypes) {
        this.queryFieldsDatatypes = queryFieldsDatatypes;
    }
    
    public Multimap<String,Type<?>> getQueryFieldsDatatypes() {
        return queryFieldsDatatypes;
    }
    
    public void setCompositeToFieldMap(Multimap<String,String> compositeToFieldMap) {
        this.compositeToFieldMap = compositeToFieldMap;
    }
    
    public Multimap<String,String> getCompositeToFieldMap() {
        return compositeToFieldMap;
    }
    
    public Multimap<String,CompositeNameAndIndex> getFieldToCompositeMap() {
        return fieldToCompositeMap;
    }
    
    public void setFieldToCompositeMap(Multimap<String,CompositeNameAndIndex> fieldToCompositeMap) {
        this.fieldToCompositeMap = fieldToCompositeMap;
    }
    
    public Multimap<String,String> getCurrentCompositeToFieldMap() {
        return currentCompositeToFieldMap;
    }
    
    public void setCurrentCompositeToFieldMap(Multimap<String,String> currentCompositeToFieldMap) {
        this.currentCompositeToFieldMap = currentCompositeToFieldMap;
    }
    
    public Multimap<String,Type<?>> getNormalizedFieldsDatatypes() {
        return normalizedFieldsDatatypes;
    }
    
    public void setNormalizedFieldsDatatypes(Multimap<String,Type<?>> normalizedFieldsDatatypes) {
        this.normalizedFieldsDatatypes = normalizedFieldsDatatypes;
        this.normalizedFields = Sets.newHashSet(normalizedFieldsDatatypes.keySet());
    }
    
    public Collection<String> getLimitFields() {
        return limitFields;
    }
    
    public void setLimitFields(Set<String> limitFields) {
        this.limitFields = limitFields;
    }
    
    public String getLimitFieldsAsString() {
        return org.apache.commons.lang.StringUtils.join(this.getLimitFields(), Constants.PARAM_VALUE_SEP);
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
    
    public Collection<String> getGroupFields() {
        return groupFields;
    }
    
    public void setGroupFields(Set<String> groupFields) {
        this.groupFields = groupFields;
    }
    
    public String getGroupFieldsAsString() {
        return org.apache.commons.lang.StringUtils.join(this.getGroupFields(), Constants.PARAM_VALUE_SEP);
    }
    
    public void setHitList(boolean hitList) {
        this.hitList = hitList;
    }
    
    public boolean isHitList() {
        return this.hitList;
    }
    
    public boolean isTypeMetadataInHdfs() {
        return typeMetadataInHdfs;
    }
    
    public void setTypeMetadataInHdfs(boolean typeMetadataInHdfs) {
        this.typeMetadataInHdfs = typeMetadataInHdfs;
    }
    
    public void setRawTypes(boolean rawTypes) {
        this.rawTypes = rawTypes;
    }
    
    public boolean isRawTypes() {
        return this.rawTypes;
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
    
    public boolean allTermsIndexOnly() {
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
    
    public void setLimitTermExpansionToModel(boolean shouldLimitTermExpansionToModel) {
        this.shouldLimitTermExpansionToModel = shouldLimitTermExpansionToModel;
    }
    
    public boolean isExpansionLimitedToModelContents() {
        return shouldLimitTermExpansionToModel;
    }
    
    public void setMaxIndexScanTimeMillis(long maxTime) {
        this.maxIndexScanTimeMillis = maxTime;
    }
    
    public long getMaxIndexScanTimeMillis() {
        return maxIndexScanTimeMillis;
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
    
    public void setAllowShortcutEvaluation(boolean allowShortcutEvaluation) {
        this.allowShortcutEvaluation = allowShortcutEvaluation;
    }
    
    public boolean getAllowShortcutEvaluation() {
        return allowShortcutEvaluation;
    }
    
    public void setBypassAccumulo(boolean bypassAccumulo) {
        this.bypassAccumulo = bypassAccumulo;
    }
    
    public boolean getBypassAccumulo() {
        return bypassAccumulo;
    }
    
    // copy constructor
    public RefactoredShardQueryConfiguration(RefactoredShardQueryConfiguration copy) {
        
        // normally set via super constructor
        this.setTableName(copy.getTableName());
        this.setMaxQueryResults(copy.getMaxQueryResults());
        this.setMaxRowsToScan(copy.getMaxRowsToScan());
        
        this.setUndisplayedVisibilities(null == copy.getUndisplayedVisibilities() ? null : Sets.newHashSet(copy.getUndisplayedVisibilities()));
        this.setBaseIteratorPriority(copy.getBaseIteratorPriority());
        
        this.setMaxQueryResults(copy.getMaxQueryResults());
        
        this.setMaxRowsToScan(copy.getMaxRowsToScan());
        this.setNumQueryThreads(copy.getNumQueryThreads());
        this.setDefaultType(copy.getDefaultType());
        
        // Table names
        this.setShardTableName(copy.getShardTableName());
        this.setIndexTableName(copy.getIndexTableName());
        this.setReverseIndexTableName(copy.getReverseIndexTableName());
        this.setMetadataTableName(copy.getMetadataTableName());
        this.setDateIndexTableName(copy.getDateIndexTableName());
        this.setQueryModel(copy.getQueryModel());
        this.setModelTableName(copy.getModelTableName());
        this.setIndexStatsTableName(copy.getIndexStatsTableName());
        
        this.setIndexHoles(copy.getIndexHoles());
        
        this.setSequentialScheduler(copy.getSequentialScheduler());
        // Enrichment properties
        this.setUseEnrichers(copy.getUseEnrichers());
        this.setEnricherClassNames(null == copy.getEnricherClassNames() ? null : Lists.newArrayList(copy.getEnricherClassNames()));
        
        // is config a tld query logic
        this.setTldQuery(copy.isTldQuery());
        
        // Filter properties
        this.setUseFilters(copy.getUseFilters());
        this.setFilterClassNames(copy.getFilterClassNames());
        this.setIndexFilteringClassNames(copy.getIndexFilteringClassNames());
        this.putFilterOptions(copy.getFilterOptions());
        
        this.setFullTableScanEnabled(copy.getFullTableScanEnabled());
        this.setRealmSuffixExclusionPatterns(copy.getRealmSuffixExclusionPatterns());
        this.setNonEventKeyPrefixes(null == copy.getNonEventKeyPrefixes() ? null : Sets.newHashSet(copy.getNonEventKeyPrefixes()));
        this.setUnevaluatedFields(null == copy.getUnevaluatedFields() ? null : Sets.newHashSet(copy.getUnevaluatedFields()));
        this.setMinSelectivity(copy.getMinSelectivity());
        
        this.setFilterMaskedValues(copy.getFilterMaskedValues());
        
        this.setIncludeDataTypeAsField(copy.getIncludeDataTypeAsField());
        this.setIncludeHierarchyFields(copy.getIncludeHierarchyFields());
        this.setHierarchyFieldOptions(null == copy.getHierarchyFieldOptions() ? null : Maps.newHashMap(copy.getHierarchyFieldOptions()));
        this.setBlacklistedFields(null == copy.getBlacklistedFields() ? null : Sets.newHashSet(copy.getBlacklistedFields()));
        
        // ShardEventEvaluatingIterator options
        this.setIncludeGroupingContext(copy.getIncludeGroupingContext());
        
        // Pass down the RangeCalculator options
        this.setEventPerDayThreshold(copy.getEventPerDayThreshold());
        this.setShardsPerDayThreshold(copy.getShardsPerDayThreshold());
        this.setMaxTermThreshold(copy.getMaxTermThreshold());
        this.setMaxDepthThreshold(copy.getMaxDepthThreshold());
        this.setMaxUnfieldedExpansionThreshold(copy.getMaxUnfieldedExpansionThreshold());
        this.setMaxValueExpansionThreshold(copy.getMaxValueExpansionThreshold());
        this.setMaxOrExpansionThreshold(copy.getMaxOrExpansionThreshold());
        this.setMaxOrExpansionFstThreshold(copy.getMaxOrExpansionFstThreshold());
        
        this.setExpandAllTerms(copy.isExpandAllTerms());
        
        this.setHdfsSiteConfigURLs(copy.getHdfsSiteConfigURLs());
        this.setHdfsFileCompressionCodec(copy.getHdfsFileCompressionCodec());
        this.setZookeeperConfig(copy.getZookeeperConfig());
        
        this.setIvaratorCacheBaseURIs(copy.getIvaratorCacheBaseURIs());
        this.setIvaratorFstHdfsBaseURIs(copy.getIvaratorFstHdfsBaseURIs());
        this.setIvaratorCacheBufferSize(copy.getIvaratorCacheBufferSize());
        this.setIvaratorCacheScanPersistThreshold(copy.getIvaratorCacheScanPersistThreshold());
        this.setIvaratorCacheScanTimeout(copy.getIvaratorCacheScanTimeout());
        
        this.setMaxFieldIndexRangeSplit(copy.getMaxFieldIndexRangeSplit());
        this.setIvaratorMaxOpenFiles(copy.getIvaratorMaxOpenFiles());
        this.setMaxIvaratorSources(copy.getMaxIvaratorSources());
        this.setMaxEvaluationPipelines(copy.getMaxEvaluationPipelines());
        this.setMaxPipelineCachedResults(copy.getMaxPipelineCachedResults());
        
        this.setReducedResponse(copy.isReducedResponse());
        this.setDisableEvaluation(copy.isDisableEvaluation());
        this.setDisableIndexOnlyDocuments(copy.disableIndexOnlyDocuments());
        this.setHitList(copy.isHitList());
        this.setTypeMetadataInHdfs(copy.isTypeMetadataInHdfs());
        this.setCompressServerSideResults(copy.isCompressServerSideResults());
        this.setMetadataTableName(copy.metadataTableName);
        
        // Allow index-only JEXL functions, which can potentially use a huge
        // amount of memory, to be turned on or off
        this.setIndexOnlyFilterFunctionsEnabled(copy.isIndexOnlyFilterFunctionsEnabled());
        
        this.setAccumuloPassword(copy.getAccumuloPassword());
        
        this.setCollapseUids(copy.getCollapseUids());
        
        this.setMaxIndexScanTimeMillis(copy.getMaxIndexScanTimeMillis());
        
        this.setAllowShortcutEvaluation(copy.getAllowShortcutEvaluation());
        
        this.setLimitFields(new HashSet<String>(copy.getLimitFields()));
        this.setQuery(copy.getQuery());
        Set<QueryImpl.Parameter> parameterSet = query.getParameters();
        for (QueryImpl.Parameter parameter : parameterSet) {
            String name = parameter.getParameterName();
            String value = parameter.getParameterValue();
            if (name.equals(QueryParameters.HIT_LIST)) {
                this.setHitList(Boolean.parseBoolean(value));
            }
            if (name.equals(QueryParameters.TYPE_METADATA_IN_HDFS)) {
                this.setTypeMetadataInHdfs(Boolean.parseBoolean(value));
            }
            if (name.equals(QueryParameters.DATE_INDEX_TIME_TRAVEL)) {
                this.setDateIndexTimeTravel(Boolean.parseBoolean(value));
            }
            if (name.equals(QueryParameters.PARAMETER_MODEL_NAME)) {
                this.setMetadataTableName(value);
            }
        }
        
        this.setBeginDateCap(copy.getBeginDateCap());
        this.setFailOutsideValidDateRange(copy.isFailOutsideValidDateRange());
        
        this.setLogTimingDetails(copy.getLogTimingDetails());
        this.setCollectTimingDetails(copy.getCollectTimingDetails());
        this.setSendTimingToStatsd(copy.getSendTimingToStatsd());
        this.setStatsdHost(copy.getStatsdHost());
        this.setStatsdPort(copy.getStatsdPort());
        this.setStatsdLatencyMs(copy.getStatsdLatencyMs());
        this.setStatsdMaxQueueSize(copy.getStatsdMaxQueueSize());
        this.setStatsdKeepAliveMs(copy.getStatsdKeepAliveMs());
        
        this.setQuery(query);
        this.setCollapseDatePercentThreshold(copy.getCollapseDatePercentThreshold());
        this.setSerializeQueryIterator(copy.getSerializeQueryIterator());
        this.fstCount = copy.fstCount;
        
        this.setDebugMultithreadedSources(copy.isDebugMultithreadedSources());
        
        this.setSortedUIDs(copy.isSortedUIDs());
    }
    
    public void setAccrueStats(boolean accrueStats) {
        this.accrueStats = accrueStats;
        
    }
    
    public boolean getAccrueStats() {
        return accrueStats;
    }
    
    public List<IndexHole> getIndexHoles() {
        return indexHoles;
    }
    
    public void setIndexHoles(List<IndexHole> indexHoles) {
        this.indexHoles = indexHoles;
    }
    
    public void setCollectTimingDetails(boolean collectTimingDetails) {
        this.collectTimingDetails = collectTimingDetails;
        
    }
    
    public boolean getCollectTimingDetails() {
        return collectTimingDetails;
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
    
    public long getStatsdLatencyMs() {
        return statsdLatencyMs;
    }
    
    public void setStatsdLatencyMs(long statsdLatencyMs) {
        this.statsdLatencyMs = statsdLatencyMs;
    }
    
    public int getStatsdMaxQueueSize() {
        return statsdMaxQueueSize;
    }
    
    public void setStatsdMaxQueueSize(int statsdMaxQueueSize) {
        this.statsdMaxQueueSize = statsdMaxQueueSize;
    }
    
    public long getStatsdKeepAliveMs() {
        return statsdKeepAliveMs;
    }
    
    public void setStatsdKeepAliveMs(long statsdKeepAliveMs) {
        this.statsdKeepAliveMs = statsdKeepAliveMs;
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
    
    public boolean getCacheModel() {
        return cacheModel;
    }
    
    public void setCacheModel(boolean cacheModel) {
        this.cacheModel = cacheModel;
    }
    
    public void setBypassExecutabilityCheck() {
        bypassExecutabilityCheck = true;
    }
    
    public boolean bypassExecutabilityCheck() {
        return bypassExecutabilityCheck;
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
    
    public void setSpeculativeScanning(boolean speculativeScanning) {
        this.speculativeScanning = speculativeScanning;
    }
    
    public boolean getSpeculativeScanning() {
        return speculativeScanning;
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
}
