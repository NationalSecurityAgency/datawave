package datawave.query.planner;

/**
 * Options that control how a query is planned.
 * <p>
 * The QueryPlannerOptions exist as an internal, injectable variable in the {@link DefaultQueryPlanner}.
 * <p>
 * Any QueryLogic that extends {@link datawave.query.tables.ShardQueryLogic} may reference its own QueryPlannerOptions that gets set on the QueryPlanner. In
 * this way multiple distinct query logics may reference distinct "planning profiles".
 */
public class QueryPlannerOptions {

    private boolean limitScanners = true;
    private boolean disableBoundedLookup;
    private boolean disableAnyFieldLookup;
    private boolean disableCompositeFields;
    private boolean disableTestNonExistentFields;
    private boolean disableWhindexFieldMappings;
    private boolean disableExpandIndexFunctions;

    // threaded range bundler options here
    private long maxRangesPerQueryPiece;
    private long maxRangeWaitMillis = 125L;
    // how many ranges should the threaded range bundler buffer before the first range is returned to the caller
    private int numRangesToBuffer = 0;
    // how long ranges are allowed to buffer before the first range is returned to the caller
    private long rangeBufferTimeoutMillis = 0L;
    // determines the poll interval when buffering ranges in the threaded range bundler
    private long rangeBufferPollMillis = 100L;

    // misc options
    private boolean cacheDataTypes;
    private boolean compressMappings;
    private boolean preloadOptions;
    // if the number of query terms exceeds this threshold then all document ranges are pushed into shard ranges
    private long pushdownThreshold = 500L;
    private long sourceLimit;
    private boolean executableExpansion = true;
    // automated logical reduction of the query
    private boolean reduceQuery = true;
    // print the pruned query with assignment nodes. could be costly from a performance perspective.
    private boolean showReducedQueryPrune = true;

    public QueryPlannerOptions() {
        // empty constructor
    }

    /**
     * Copy constructor
     *
     * @param other
     *            another instance of QueryPlannerOptions
     */
    public QueryPlannerOptions(QueryPlannerOptions other) {
        this.limitScanners = other.limitScanners;
        this.disableBoundedLookup = other.disableBoundedLookup;
        this.disableAnyFieldLookup = other.disableAnyFieldLookup;
        this.disableCompositeFields = other.disableCompositeFields;
        this.disableTestNonExistentFields = other.disableTestNonExistentFields;
        this.disableWhindexFieldMappings = other.disableWhindexFieldMappings;
        this.disableExpandIndexFunctions = other.disableExpandIndexFunctions;
        // range bundler options
        this.maxRangesPerQueryPiece = other.maxRangesPerQueryPiece;
        this.maxRangeWaitMillis = other.maxRangeWaitMillis;
        this.numRangesToBuffer = other.numRangesToBuffer;
        this.rangeBufferTimeoutMillis = other.rangeBufferTimeoutMillis;
        this.rangeBufferPollMillis = other.rangeBufferPollMillis;
        // misc options
        this.cacheDataTypes = other.cacheDataTypes;
        this.compressMappings = other.compressMappings;
        this.preloadOptions = other.preloadOptions;
        this.pushdownThreshold = other.pushdownThreshold;
        this.sourceLimit = other.sourceLimit;
        this.executableExpansion = other.executableExpansion;
        this.reduceQuery = other.reduceQuery;
        this.showReducedQueryPrune = other.showReducedQueryPrune;
    }

    public boolean isLimitScanners() {
        return limitScanners;
    }

    public void setLimitScanners(boolean limitScanners) {
        this.limitScanners = limitScanners;
    }

    public boolean isDisableBoundedLookup() {
        return disableBoundedLookup;
    }

    public void setDisableBoundedLookup(boolean disableBoundedLookup) {
        this.disableBoundedLookup = disableBoundedLookup;
    }

    public boolean isDisableAnyFieldLookup() {
        return disableAnyFieldLookup;
    }

    public void setDisableAnyFieldLookup(boolean disableAnyFieldLookup) {
        this.disableAnyFieldLookup = disableAnyFieldLookup;
    }

    public boolean isDisableCompositeFields() {
        return disableCompositeFields;
    }

    public void setDisableCompositeFields(boolean disableCompositeFields) {
        this.disableCompositeFields = disableCompositeFields;
    }

    public boolean isDisableTestNonExistentFields() {
        return disableTestNonExistentFields;
    }

    public void setDisableTestNonExistentFields(boolean disableTestNonExistentFields) {
        this.disableTestNonExistentFields = disableTestNonExistentFields;
    }

    public boolean isDisableWhindexFieldMappings() {
        return disableWhindexFieldMappings;
    }

    public void setDisableWhindexFieldMappings(boolean disableWhindexFieldMappings) {
        this.disableWhindexFieldMappings = disableWhindexFieldMappings;
    }

    public boolean isDisableExpandIndexFunctions() {
        return disableExpandIndexFunctions;
    }

    public void setDisableExpandIndexFunctions(boolean disableExpandIndexFunctions) {
        this.disableExpandIndexFunctions = disableExpandIndexFunctions;
    }

    public boolean isCacheDataTypes() {
        return cacheDataTypes;
    }

    public void setCacheDataTypes(boolean cacheDataTypes) {
        this.cacheDataTypes = cacheDataTypes;
    }

    public long getMaxRangesPerQueryPiece() {
        return maxRangesPerQueryPiece;
    }

    public void setMaxRangesPerQueryPiece(long maxRangesPerQueryPiece) {
        this.maxRangesPerQueryPiece = maxRangesPerQueryPiece;
    }

    public boolean isCompressMappings() {
        return compressMappings;
    }

    public void setCompressMappings(boolean compressMappings) {
        this.compressMappings = compressMappings;
    }

    public boolean isPreloadOptions() {
        return preloadOptions;
    }

    public void setPreloadOptions(boolean preloadOptions) {
        this.preloadOptions = preloadOptions;
    }

    public long getMaxRangeWaitMillis() {
        return maxRangeWaitMillis;
    }

    public void setMaxRangeWaitMillis(long maxRangeWaitMillis) {
        this.maxRangeWaitMillis = maxRangeWaitMillis;
    }

    public long getPushdownThreshold() {
        return pushdownThreshold;
    }

    public void setPushdownThreshold(long pushdownThreshold) {
        this.pushdownThreshold = pushdownThreshold;
    }

    public long getSourceLimit() {
        return sourceLimit;
    }

    public void setSourceLimit(long sourceLimit) {
        this.sourceLimit = sourceLimit;
    }

    public boolean isExecutableExpansion() {
        return executableExpansion;
    }

    public void setExecutableExpansion(boolean executableExpansion) {
        this.executableExpansion = executableExpansion;
    }

    public boolean isReduceQuery() {
        return reduceQuery;
    }

    public void setReduceQuery(boolean reduceQuery) {
        this.reduceQuery = reduceQuery;
    }

    public boolean isShowReducedQueryPrune() {
        return showReducedQueryPrune;
    }

    public void setShowReducedQueryPrune(boolean showReducedQueryPrune) {
        this.showReducedQueryPrune = showReducedQueryPrune;
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
}
