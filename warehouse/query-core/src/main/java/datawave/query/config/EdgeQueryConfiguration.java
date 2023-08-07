package datawave.query.config;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import datawave.core.query.configuration.CheckpointableQueryConfiguration;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.configuration.QueryData;
import datawave.data.type.Type;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.model.edge.EdgeQueryModel;
import datawave.query.tables.edge.EdgeQueryLogic;

/**
 * Created with IntelliJ IDEA. To change this template use File | Settings | File Templates.
 */
public class EdgeQueryConfiguration extends GenericQueryConfiguration implements Serializable, CheckpointableQueryConfiguration {
    private static final long serialVersionUID = -2795330785878662313L;
    public static final int DEFAULT_SKIP_LIMIT = 10;
    public static final long DEFAULT_SCAN_LIMIT = Long.MAX_VALUE;

    // We originally had the two choices:
    // EVENT => apply date range to edges generated using event date
    // LOAD => apply date range to load date in value of edges generated using event date
    // Then we added edges generated using activity date. So, we've added:
    // ACTIVITY => apply date range to edges generated using activity date
    // ACTIVITY_LOAD => apply date range to load date in value of edges generated using activity date
    // ANY => apply date range to regardless of date type
    // ANY_LOAD => apply date range to load date in value of edges generated using any date
    public enum dateType {
        EVENT, LOAD, ACTIVITY, ACTIVITY_LOAD, ANY, ANY_LOAD
    };

    public static final String INCLUDE_STATS = "stats";

    public static final String DATE_RANGE_TYPE = "date.type";

    public static final String LOAD = "LOAD", EVENT = "EVENT", ACTIVITY = "ACTIVITY", ACTIVITY_LOAD = "ACTIVITY_LOAD", ANY = "ANY", ANY_LOAD = "ANY_LOAD";

    public static final String SUMMARIZE = "summarize";

    // Query model defaults...
    private String modelName = "DATAWAVE_EDGE";
    private String modelTableName = "DatawaveMetadata";
    private String metadataTableName = "DatawaveMetadata";
    private EdgeQueryModel edgeQueryModel = null;

    private List<? extends Type<?>> dataTypes;
    private List<? extends Type<?>> regexDataTypes = null;

    // to be backwards compatible, by default we want to return
    protected boolean includeStats = true;
    private long maxQueryTerms = 10000;
    private long maxPrefilterValues = 100000;

    // default will be event date
    private dateType dateRangeType = dateType.EVENT;

    // Use to aggregate results will be false by default
    private boolean aggregateResults = false;

    protected int queryThreads = 8;

    protected int dateFilterSkipLimit = DEFAULT_SKIP_LIMIT;

    protected long dateFilterScanLimit = DEFAULT_SCAN_LIMIT;

    /**
     * Default constructor
     */
    public EdgeQueryConfiguration() {
        super();
    }

    /**
     * Performs a deep copy of the provided EdgeQueryConfiguration into a new instance
     *
     * @param other
     *            - another EdgeQueryConfiguration instance
     */
    public EdgeQueryConfiguration(EdgeQueryConfiguration other) {

        // GenericQueryConfiguration copy first
        super(other);

        // EdgeQueryConfiguration copy
        setModelName(other.getModelName());
        setModelTableName(other.getModelTableName());
        setMetadataTableName(other.getMetadataTableName());
        setEdgeQueryModel(other.getEdgeQueryModel());
        setDataTypes(other.getDataTypes());
        setRegexDataTypes(other.getRegexDataTypes());
        setQueryThreads(other.getQueryThreads());
        setIncludeStats(other.includeStats());
        setMaxQueryTerms(other.getMaxQueryTerms());
        setMaxPrefilterValues(other.getMaxPrefilterValues());
        setDateRangeType(other.getDateRangeType());
        setAggregateResults(other.isAggregateResults());
        setDateFilterScanLimit(other.getDateFilterScanLimit());
        setDateFilterSkipLimit(other.getDateFilterSkipLimit());
    }

    /**
     * This constructor is used when we are creating a checkpoint for a set of ranges (i.e. QueryData objects). All configuration required for post planning
     * needs to be copied over here.
     *
     * @param other
     * @param queries
     */
    public EdgeQueryConfiguration(EdgeQueryConfiguration other, Collection<QueryData> queries) {
        this(other);

        this.setQueries(queries);

        // do not preserve the original queries iter. getQueriesIter will create a new
        // iterator based off of the queries collection if queriesIter is null
        this.setQueriesIter(null);
    }

    @Override
    public EdgeQueryConfiguration checkpoint() {
        // Create a new config that only contains what is needed to execute the specified ranges
        return new EdgeQueryConfiguration(this, getQueries());
    }

    /**
     * Delegates deep copy work to appropriate constructor, sets additional values specific to the provided ShardQueryLogic
     *
     * @param logic
     *            - a EdgeQueryLogic instance or subclass
     */
    public EdgeQueryConfiguration(EdgeQueryLogic logic) {
        this(logic.getConfig());
    }

    /**
     * Factory method that instantiates an fresh EdgeQueryConfiguration
     *
     * @return - a clean EdgeQueryConfiguration
     */
    public static EdgeQueryConfiguration create() {
        return new EdgeQueryConfiguration();
    }

    /**
     * Factory method that returns a deep copy of the provided EdgeQueryConfiguration
     *
     * @param other
     *            - another instance of a EdgeQueryConfiguration
     * @return - copy of provided EdgeQueryConfiguration
     */
    public static EdgeQueryConfiguration create(EdgeQueryConfiguration other) {
        return new EdgeQueryConfiguration(other);
    }

    /**
     * Factory method that creates a EdgeQueryConfiguration deep copy from a EdgeQueryLogic
     *
     * @param logic
     *            - a configured EdgeQueryLogic
     * @return - a EdgeQueryConfiguration
     */
    public static EdgeQueryConfiguration create(EdgeQueryLogic logic) {

        EdgeQueryConfiguration config = create(logic.getConfig());

        // Lastly, honor overrides passed in via query parameters
        config.parseParameters(config.getQuery());

        return config;
    }

    /**
     * Factory method that creates a EdgeQueryConfiguration from a EdgeQueryLogic and a Query
     *
     * @param logic
     *            - a configured EdgeQueryLogic
     * @param query
     *            - a configured Query object
     * @return - a EdgeQueryConfiguration
     */
    public static EdgeQueryConfiguration create(EdgeQueryLogic logic, Query query) {
        EdgeQueryConfiguration config = create(logic);
        config.setQuery(query);
        return config;
    }

    public List<? extends Type<?>> getDataTypes() {
        return dataTypes;
    }

    public void setDataTypes(List<? extends Type<?>> dataTypes) {
        this.dataTypes = dataTypes;
    }

    public dateType getDateRangeType() {
        return dateRangeType;
    }

    public void setDateRangeType(dateType dateRangeType) {
        this.dateRangeType = dateRangeType;
    }

    /**
     * Fluent interface for parsing the parameters out of the Parameter set provided by the Query.
     *
     * @param settings
     *            the settings
     * @return an edge query configuration
     */
    public EdgeQueryConfiguration parseParameters(Query settings) {
        setQuery(settings);
        if (settings.getParameters() != null) {
            QueryImpl.Parameter p = settings.findParameter(INCLUDE_STATS);
            if (p != null && !p.getParameterValue().isEmpty()) {
                this.includeStats = Boolean.parseBoolean(p.getParameterValue());
            }

            p = settings.findParameter(DATE_RANGE_TYPE);
            if (p != null && !p.getParameterValue().isEmpty()) {
                String paramVal = p.getParameterValue();
                switch (paramVal) {
                    case LOAD:
                        this.dateRangeType = dateType.LOAD;
                        break;
                    case EVENT:
                        this.dateRangeType = dateType.EVENT;
                        break;
                    case ACTIVITY:
                        this.dateRangeType = dateType.ACTIVITY;
                        break;
                    case ACTIVITY_LOAD:
                        this.dateRangeType = dateType.ACTIVITY_LOAD;
                        break;
                    case ANY:
                        this.dateRangeType = dateType.ANY;
                        break;
                    case ANY_LOAD:
                        this.dateRangeType = dateType.ANY_LOAD;
                        break;
                    // @WARNING no default case, i.e. we are ignoring unexpected values for dateRangeType
                }
            }

            p = settings.findParameter(SUMMARIZE);
            if (p != null && !p.getParameterValue().isEmpty()) {
                this.aggregateResults = Boolean.parseBoolean(p.getParameterValue());
            }
        }
        return this;
    }

    public long getMaxQueryTerms() {
        return maxQueryTerms;
    }

    public void setMaxQueryTerms(long maxQueryTerms) {
        this.maxQueryTerms = maxQueryTerms;
    }

    public long getMaxPrefilterValues() {
        return maxPrefilterValues;
    }

    public void setMaxPrefilterValues(long maxPrefilterValues) {
        this.maxPrefilterValues = maxPrefilterValues;
    }

    public boolean isAggregateResults() {
        return aggregateResults;
    }

    public void setAggregateResults(boolean aggregateResults) {
        this.aggregateResults = aggregateResults;
    }

    public EdgeQueryModel getEdgeQueryModel() {
        return this.edgeQueryModel;
    }

    public void setEdgeQueryModel(EdgeQueryModel model) {
        this.edgeQueryModel = model;
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

    public String getMetadataTableName() {
        return metadataTableName;
    }

    public void setMetadataTableName(String metadataTableName) {
        this.metadataTableName = metadataTableName;
    }

    public List<? extends Type<?>> getRegexDataTypes() {
        return regexDataTypes;
    }

    public void setRegexDataTypes(List<? extends Type<?>> regexDataTypes) {
        this.regexDataTypes = regexDataTypes;
    }

    public int getQueryThreads() {
        return queryThreads;
    }

    public void setQueryThreads(int queryThreads) {
        this.queryThreads = queryThreads;
    }

    public boolean includeStats() {
        return includeStats;
    }

    public void setIncludeStats(boolean includeStats) {
        this.includeStats = includeStats;
    }

    public int getDateFilterSkipLimit() {
        return dateFilterSkipLimit;
    }

    public void setDateFilterSkipLimit(int dateFilterSkipLimit) {
        this.dateFilterSkipLimit = dateFilterSkipLimit;
    }

    public long getDateFilterScanLimit() {
        return dateFilterScanLimit;
    }

    public void setDateFilterScanLimit(long dateFilterScanLimit) {
        this.dateFilterScanLimit = dateFilterScanLimit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;
        EdgeQueryConfiguration that = (EdgeQueryConfiguration) o;
        return queryThreads == that.queryThreads && includeStats == that.includeStats && maxQueryTerms == that.maxQueryTerms
                        && maxPrefilterValues == that.maxPrefilterValues && aggregateResults == that.aggregateResults
                        && Objects.equals(modelName, that.modelName) && Objects.equals(modelTableName, that.modelTableName)
                        && Objects.equals(edgeQueryModel, that.edgeQueryModel) && Objects.equals(dataTypes, that.dataTypes)
                        && dateRangeType == that.dateRangeType && dateFilterScanLimit == that.dateFilterScanLimit
                        && dateFilterSkipLimit == that.dateFilterSkipLimit;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), modelName, modelTableName, edgeQueryModel, dataTypes, queryThreads, includeStats, maxQueryTerms,
                        maxPrefilterValues, dateRangeType, aggregateResults, dateFilterScanLimit, dateFilterSkipLimit);
    }
}
