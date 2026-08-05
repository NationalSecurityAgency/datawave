package datawave.webservice.query.limit;

import java.util.Objects;
import java.util.StringJoiner;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a custom query limit configuration that can be configured for query logics.
 */
public class QueryLogicGroupLimitConfiguration {
    
    /**
     * The name of the query logic group.
     */
    @JsonProperty
    private String groupName;
    
    /**
     * The query logic regex pattern.
     */
    @JsonProperty
    private String queryLogicPattern;
    
    /**
     * The default concurrency limit for users. This applies to the total concurrent queries a user may run that originate from a query logic in the group
     * across all systems.
     */
    @JsonProperty
    private int queryLimit;

    public QueryLogicGroupLimitConfiguration() {}

    public QueryLogicGroupLimitConfiguration(String groupName, String queryLogicPattern, int queryLimit) {
        this.groupName = groupName;
        this.queryLogicPattern = queryLogicPattern;
        this.queryLimit = queryLimit;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getQueryLogicPattern() {
        return queryLogicPattern;
    }

    public void setQueryLogicPattern(String queryLogicPattern) {
        this.queryLogicPattern = queryLogicPattern;
    }

    public int getQueryLimit() {
        return queryLimit;
    }

    public void setQueryLimit(int queryLimit) {
        this.queryLimit = queryLimit;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryLogicGroupLimitConfiguration that = (QueryLogicGroupLimitConfiguration) o;
        return queryLimit == that.queryLimit && Objects.equals(groupName, that.groupName) && Objects.equals(queryLogicPattern, that.queryLogicPattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupName, queryLogicPattern, queryLimit);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", QueryLogicGroupLimitConfiguration.class.getSimpleName() + "[", "]").add("groupName='" + groupName + "'")
                        .add("queryLogicPattern='" + queryLogicPattern + "'").add("queryLimit=" + queryLimit).toString();
    }
}
