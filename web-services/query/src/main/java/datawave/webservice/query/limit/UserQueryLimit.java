package datawave.webservice.query.limit;

import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * This class represents the query limits to be enforced for an individual user.
 */
public class UserQueryLimit {
    
    // The user DN.
    private final String userDn;
    
    // The user's concurrent query limit. This applies to the total number of queries the user may run across all systems.
    private final int queryLimit;
    
    // Map of query logic group names to the user's concurrent query limit for the group.
    private final Map<String, Integer> queryLogicGroupLimits;
    
    // The source of the limits (configuration vs. default limits).
    private final LimitSource source;
    
    /**
     * Returns a new {@link UserQueryLimit} with limits that originated from a matching user query limit configuration.
     * @param userDn the user DN
     * @param queryLimit the query limit
     * @param queryLogicGroupLimits the overridden query logic group limits
     * @return the new {@link UserQueryLimit}
     */
    public static UserQueryLimit fromConfig(String userDn, int queryLimit, Map<String, Integer> queryLogicGroupLimits) {
        return new UserQueryLimit(userDn, queryLimit, queryLogicGroupLimits, LimitSource.CONFIG);
    }
    
    /**
     * Returns a new {@link UserQueryLimit} with limits that originated from the default limits.
     * @param userDn the user DN
     * @param queryLimit the query limit
     * @return the new {@link UserQueryLimit}
     */
    public static UserQueryLimit fromDefaults(String userDn, int queryLimit) {
        return new UserQueryLimit(userDn, queryLimit, null, LimitSource.DEFAULTS);
    }
    
    private UserQueryLimit(String userDn, int queryLimit, Map<String,Integer> queryLogicGroupLimits, LimitSource source) {
        this.userDn = userDn;
        this.queryLimit = queryLimit;
        this.queryLogicGroupLimits = queryLogicGroupLimits == null ? Map.of() : Map.copyOf(queryLogicGroupLimits);
        this.source = source;
    }
    
    public String getUserDn() {
        return userDn;
    }
    
    public int getQueryLimit() {
        return queryLimit;
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
        UserQueryLimit that = (UserQueryLimit) o;
        return queryLimit == that.queryLimit && Objects.equals(userDn, that.userDn) && Objects.equals(queryLogicGroupLimits, that.queryLogicGroupLimits)
                        && source == that.source;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(userDn, queryLimit, queryLogicGroupLimits, source);
    }
    
    @Override
    public String toString() {
        return new StringJoiner(", ", UserQueryLimit.class.getSimpleName() + "[", "]").add("userDn='" + userDn + "'").add("queryLimit=" + queryLimit)
                        .add("queryLogicGroupLimits=" + queryLogicGroupLimits).add("source=" + source).toString();
    }
}
