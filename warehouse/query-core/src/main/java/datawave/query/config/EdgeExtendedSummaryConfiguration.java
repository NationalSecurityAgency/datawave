package datawave.query.config;

import java.io.Serializable;
import java.util.Collection;
import java.util.Objects;

import datawave.core.query.configuration.QueryData;
import datawave.query.edge.DefaultExtendedEdgeQueryLogic;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;

public class EdgeExtendedSummaryConfiguration extends EdgeQueryConfiguration implements Serializable {
    public static final String SUMMARIZE = "summarize";
    public static final String LIMITED_JEXL = "LIMITED_JEXL";
    public static final String LIST = "LIST";
    public static final String IGNORE_STATS_PARAM = "ignoreStats";
    public static final String IGNORE_RELATIONSHIPS_PARAM = "ignoreRelationships";
    public static final String QUERY_DELIMITER_PARAM = "delimiter";
    public static final String EDGE_TYPES_PARAM = "edgeTypes";

    public static final String querySyntax = "query.syntax";

    private char delimiter = '\0';

    private boolean includeRelationships = true;

    private String edgeTypes;

    private boolean summaryInputType = false;

    // Use to aggregate results will be false by default
    private boolean aggregateResults = false;

    private boolean overRideInput = false;
    private boolean overRideOutput = false;

    /**
     * Default constructor
     */
    public EdgeExtendedSummaryConfiguration() {
        super();
    }

    /**
     * Performs a deep copy of the provided EdgeExtendedSummaryConfiguration into a new instance
     *
     * @param other
     *            - another EdgeExtendedSummaryConfiguration instance
     */
    public EdgeExtendedSummaryConfiguration(EdgeExtendedSummaryConfiguration other) {

        // GenericQueryConfiguration copy first
        super(other);

        // EdgeExtendedSummaryConfiguration copy
        delimiter = other.delimiter;
        includeRelationships = other.includeRelationships;
        edgeTypes = other.edgeTypes;
        summaryInputType = other.summaryInputType;
        aggregateResults = other.aggregateResults;
        overRideInput = other.overRideInput;
        overRideOutput = other.overRideOutput;
    }

    /**
     * This constructor is used when we are creating a checkpoint for a set of ranges (i.e. QueryData objects). All configuration required for post planning
     * needs to be copied over here.
     *
     * @param other
     * @param queries
     */
    public EdgeExtendedSummaryConfiguration(EdgeExtendedSummaryConfiguration other, Collection<QueryData> queries) {
        this(other);

        this.setQueries(queries);

        // do not preserve the original queries iter. getQueriesIter will create a new
        // iterator based off of the queries collection if queriesIter is null
        this.setQueriesIter(null);
    }

    @Override
    public EdgeExtendedSummaryConfiguration checkpoint() {
        // Create a new config that only contains what is needed to execute the specified ranges
        return new EdgeExtendedSummaryConfiguration(this, getQueries());
    }

    /**
     * Delegates deep copy work to appropriate constructor, sets additional values specific to the provided ShardQueryLogic
     *
     * @param logic
     *            - a DefaultExtendedEdgeQueryLogic instance or subclass
     */
    public EdgeExtendedSummaryConfiguration(DefaultExtendedEdgeQueryLogic logic) {
        this(logic.getConfig());
    }

    /**
     * Factory method that instantiates an fresh EdgeExtendedSummaryConfiguration
     *
     * @return - a clean EdgeExtendedSummaryConfiguration
     */
    public static EdgeExtendedSummaryConfiguration create() {
        return new EdgeExtendedSummaryConfiguration();
    }

    /**
     * Factory method that returns a deep copy of the provided EdgeExtendedSummaryConfiguration
     *
     * @param other
     *            - another instance of a EdgeExtendedSummaryConfiguration
     * @return - copy of provided EdgeExtendedSummaryConfiguration
     */
    public static EdgeExtendedSummaryConfiguration create(EdgeExtendedSummaryConfiguration other) {
        return new EdgeExtendedSummaryConfiguration(other);
    }

    /**
     * Factory method that creates a EdgeExtendedSummaryConfiguration deep copy from a DefaultExtendedEdgeQueryLogic
     *
     * @param logic
     *            - a configured DefaultExtendedEdgeQueryLogic
     * @return - a EdgeExtendedSummaryConfiguration
     */
    public static EdgeExtendedSummaryConfiguration create(DefaultExtendedEdgeQueryLogic logic) {

        EdgeExtendedSummaryConfiguration config = create(logic.getConfig());

        // Lastly, honor overrides passed in via query parameters
        config.parseParameters(config.getQuery());

        return config;
    }

    /**
     * Factory method that creates a EdgeExtendedSummaryConfiguration from a DefaultExtendedEdgeQueryLogic and a Query
     *
     * @param logic
     *            - a configured DefaultExtendedEdgeQueryLogic
     * @param query
     *            - a configured Query object
     * @return - a EdgeExtendedSummaryConfiguration
     */
    public static EdgeExtendedSummaryConfiguration create(DefaultExtendedEdgeQueryLogic logic, Query query) {
        EdgeExtendedSummaryConfiguration config = create(logic);
        config.setQuery(query);
        return config;
    }

    @Override
    public EdgeExtendedSummaryConfiguration parseParameters(Query settings) {
        super.parseParameters(settings);
        if (settings.getParameters() != null) {

            QueryImpl.Parameter p = settings.findParameter(SUMMARIZE);
            if (p != null && !p.getParameterValue().isEmpty()) {
                this.aggregateResults = Boolean.parseBoolean(p.getParameterValue());
                overRideOutput = true;
            }

            p = settings.findParameter(querySyntax);
            if (p != null && !p.getParameterValue().isEmpty()) {
                String paramVal = p.getParameterValue();
                if (paramVal.equals(LIST)) {
                    summaryInputType = true;
                } else {
                    summaryInputType = false;
                }
                overRideInput = true;
            }

            p = settings.findParameter(IGNORE_STATS_PARAM);
            if (p != null && !p.getParameterValue().isEmpty()) {
                if (Boolean.parseBoolean(p.getParameterValue())) {
                    includeStats = false;
                }
            }

            p = settings.findParameter(IGNORE_RELATIONSHIPS_PARAM);
            if (p != null && !p.getParameterValue().isEmpty()) {
                if (Boolean.parseBoolean(p.getParameterValue())) {
                    includeRelationships = false;
                }
            }

            p = settings.findParameter(QUERY_DELIMITER_PARAM);
            if (p != null && !p.getParameterValue().isEmpty()) {
                if (p.getParameterValue().length() != 1) {
                    throw new UnsupportedOperationException("The query parameter " + QUERY_DELIMITER_PARAM + " only accepts a single character.");
                }
                delimiter = p.getParameterValue().charAt(0);
            }

            p = settings.findParameter(EDGE_TYPES_PARAM);
            if (p != null && !p.getParameterValue().isEmpty()) {
                edgeTypes = p.getParameterValue();
            }
        }
        return this;
    }

    public char getDelimiter() {
        return delimiter;
    }

    public boolean isIncludeRelationships() {
        return includeRelationships;
    }

    public boolean isAggregateResults() {
        return aggregateResults;
    }

    public boolean isSummaryInputType() {
        return summaryInputType;
    }

    public String getEdgeTypes() {
        return edgeTypes;
    }

    public boolean isOverRideOutput() {
        return overRideOutput;
    }

    public boolean isOverRideInput() {
        return overRideInput;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;
        EdgeExtendedSummaryConfiguration that = (EdgeExtendedSummaryConfiguration) o;
        return delimiter == that.delimiter && includeRelationships == that.includeRelationships && summaryInputType == that.summaryInputType
                        && aggregateResults == that.aggregateResults && overRideInput == that.overRideInput && overRideOutput == that.overRideOutput
                        && Objects.equals(edgeTypes, that.edgeTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), delimiter, includeRelationships, edgeTypes, summaryInputType, aggregateResults, overRideInput, overRideOutput);
    }
}
