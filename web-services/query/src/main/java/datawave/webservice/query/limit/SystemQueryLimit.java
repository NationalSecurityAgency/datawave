package datawave.webservice.query.limit;

import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * This class represents the query limits to be enforced for matching systems. Default limits will be configured, and custom limits can be specified for
 * individual users, systems, and query logics.
 */
public class SystemQueryLimit {
    
    // The system name regex pattern.
    private final String systemPattern;
    
    // The maximum number of queries that can run concurrently on matching systems.
    private final int queryLimit;
    
    // Whether queries submitted on matching systems should count against a user's query limit.
    private final boolean countsAgainstUserLimit;
    
    // Map of query logic group names to the maximum number of queries that can run concurrently on the system when originating from query logics that fall
    // within the query logic group.
    private final Map<String, Integer> queryLogicGroupLimits;
    
    // The source of the limits (configuration vs. default limits).
    private final LimitSource source;
    
    /**
     * Returns a new {@link SystemQueryLimit} with limits that originated from a matching system query limit configuration.
     * @param systemPattern the system name regex pattern
     * @param queryLimit the query limit
     * @param countsAgainstUserLimit whether queries submitted on matching systems should count against the user's query limit
     * @param queryLogicGroupLimits the overridden query logic group limits
     * @return the new {@link SystemQueryLimit}
     */
    public static SystemQueryLimit fromConfig(String systemPattern, int queryLimit, boolean countsAgainstUserLimit, Map<String, Integer> queryLogicGroupLimits) {
        return new SystemQueryLimit(systemPattern, queryLimit, countsAgainstUserLimit, queryLogicGroupLimits, LimitSource.CONFIG);
    }
    
    /**
     * Returns a new {@link SystemQueryLimit} with limits that originated from default limits
     * @param systemPattern the system name pattern
     * @param queryLimit the query limit
     * @return the new {@link SystemQueryLimit}
     */
    public static SystemQueryLimit fromDefaults(String systemPattern, int queryLimit) {
        return new SystemQueryLimit(systemPattern, queryLimit, true, null, LimitSource.DEFAULTS);
    }
    
    private SystemQueryLimit(String systemPattern, int queryLimit, boolean countsAgainstUserLimit, Map<String, Integer> queryLogicGroupLimits, LimitSource source) {
        this.systemPattern = systemPattern;
        this.queryLimit = queryLimit;
        this.countsAgainstUserLimit = countsAgainstUserLimit;
        this.queryLogicGroupLimits = queryLogicGroupLimits == null ? Map.of() : Map.copyOf(queryLogicGroupLimits);
        this.source = source;
    }
    
    public String getSystemPattern() {
        return systemPattern;
    }
    
    public int getQueryLimit() {
        return queryLimit;
    }
    
    public boolean countsAgainstUserLimit() {
        return countsAgainstUserLimit;
    }
    
    public Map<String,Integer> getQueryLogicGroupLimits() {
        return queryLogicGroupLimits;
    }
    
    public boolean overridesAnyQueryLogicLimits() {
        return !queryLogicGroupLimits.isEmpty();
    }
    
    public LimitSource getSource() {
        return source;
    }
    
    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SystemQueryLimit that = (SystemQueryLimit) o;
        return queryLimit == that.queryLimit && countsAgainstUserLimit == that.countsAgainstUserLimit && Objects.equals(systemPattern, that.systemPattern)
                        && Objects.equals(queryLogicGroupLimits, that.queryLogicGroupLimits) && source == that.source;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(systemPattern, queryLimit, countsAgainstUserLimit, queryLogicGroupLimits, source);
    }
    
    @Override
    public String toString() {
        return new StringJoiner(", ", SystemQueryLimit.class.getSimpleName() + "[", "]").add("systemPattern='" + systemPattern + "'")
                        .add("queryLimit=" + queryLimit).add("countsAgainstUserLimit=" + countsAgainstUserLimit)
                        .add("queryLogicGroupLimits=" + queryLogicGroupLimits).add("source=" + source).toString();
    }
}
