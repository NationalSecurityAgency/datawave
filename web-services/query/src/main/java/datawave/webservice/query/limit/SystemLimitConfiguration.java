package datawave.webservice.query.limit;

import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * Represents a custom query limit configuration that can be configured for matching systems.
 */
public class SystemLimitConfiguration {

    // The system name regex pattern.
    private String systemPattern;

    // Whether queries submitted on matching systems should count against a user's query limit.
    private Boolean countsAgainstUserLimit;

    // The maximum number of queries that can run concurrently on matching systems.
    private Integer queryLimit;

    // Map of query logic group names to the maximum number of queries that can run concurrently on the system when originating from query logics that fall
    // within the query logic group. The names may be regex patterns.
    private Map<String,Integer> queryLogicGroupLimits;

    public SystemLimitConfiguration() {
        this(null, null, null, null);
    }

    public SystemLimitConfiguration(String systemPattern, Boolean countsAgainstUserLimit, Integer queryLimit, Map<String,Integer> queryLogicGroupLimits) {
        this.systemPattern = systemPattern;
        this.countsAgainstUserLimit = countsAgainstUserLimit;
        this.queryLimit = queryLimit;
        this.queryLogicGroupLimits = queryLogicGroupLimits == null ? Map.of() : Map.copyOf(queryLogicGroupLimits);
    }

    public String getSystemPattern() {
        return systemPattern;
    }

    public void setSystemPattern(String systemPattern) {
        this.systemPattern = systemPattern;
    }

    public Boolean getCountsAgainstUserLimit() {
        return countsAgainstUserLimit;
    }

    public void setCountsAgainstUserLimit(Boolean countsAgainstUserLimit) {
        this.countsAgainstUserLimit = countsAgainstUserLimit;
    }

    public Integer getQueryLimit() {
        return queryLimit;
    }

    public void setQueryLimit(Integer queryLimit) {
        this.queryLimit = queryLimit;
    }

    public Map<String,Integer> getQueryLogicGroupLimits() {
        return queryLogicGroupLimits;
    }

    public void setQueryLogicGroupLimits(Map<String,Integer> queryLogicGroupLimits) {
        this.queryLogicGroupLimits = queryLogicGroupLimits == null ? Map.of() : queryLogicGroupLimits;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SystemLimitConfiguration that = (SystemLimitConfiguration) o;
        return Objects.equals(systemPattern, that.systemPattern) && Objects.equals(countsAgainstUserLimit, that.countsAgainstUserLimit)
                        && Objects.equals(queryLimit, that.queryLimit) && Objects.equals(queryLogicGroupLimits, that.queryLogicGroupLimits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(systemPattern, countsAgainstUserLimit, queryLimit, queryLogicGroupLimits);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", SystemLimitConfiguration.class.getSimpleName() + "[", "]").add("systemPattern='" + systemPattern + "'")
                        .add("countsAgainstsUserLimit=" + countsAgainstUserLimit).add("queryLimit=" + queryLimit)
                        .add("queryLogicGroupLimits=" + queryLogicGroupLimits).toString();
    }
}
