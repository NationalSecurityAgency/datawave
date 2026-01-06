package datawave.webservice.query.limit;

import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.StringJoiner;

/**
 * Represents custom query limits defined for a specific user.
 */
public class UserLimits {

    // The user DN.
    private final String userDn;

    // The maximum number of queries the user may have concurrently running across all systems.
    private final int queryLimit;

    // The custom limits the user has for query logic groups.
    private final GroupLimitCache groupLimitOverrides;

    public UserLimits(String userDn, int queryLimit, SortedSet<QueryLogicGroupLimit> groupLimitsOverrides, long maxCacheSize) {
        this.userDn = userDn;
        this.queryLimit = queryLimit;
        this.groupLimitOverrides = GroupLimitCache.of(groupLimitsOverrides, maxCacheSize);
    }
    
    public String getUserDn() {
        return userDn;
    }
    
    /**
     * Return the maximum number of queries the user associated with this {@link UserLimits} is allowed to have running concurrently across all systems.
     *
     * @return the user query limit
     */
    public int getQueryLimit() {
        return queryLimit;
    }

    /**
     * Return whether this {@link UserLimits} overrides any limits for query logic groups.
     *
     * @return true if any limits are overridden, or false otherwise.
     */
    public boolean overridesAnyGroupLimits() {
        return !groupLimitOverrides.isEmpty();
    }

    /**
     * Return the group limit overrides that best match the given query logic.
     *
     * @param queryLogic the query logic
     * @return a map of groups to their limit
     */
    public Map<String,Integer> getBestGroupLimits(String queryLogic) {
        return groupLimitOverrides.getBestGroupLimits(queryLogic);
    }
    
    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UserLimits that = (UserLimits) o;
        return Objects.equals(userDn, that.userDn) && Objects.equals(queryLimit, that.queryLimit)
                        && Objects.equals(groupLimitOverrides, that.groupLimitOverrides);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userDn, queryLimit, groupLimitOverrides);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", UserLimits.class.getSimpleName() + "[", "]").add("userDn='" + userDn + "'").add("queryLimit=" + queryLimit)
                        .add("groupLimitOverrides=" + groupLimitOverrides).toString();
    }
}
