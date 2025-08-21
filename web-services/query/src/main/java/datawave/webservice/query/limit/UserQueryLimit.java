package datawave.webservice.query.limit;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeSet;

/**
 * This class represents the query limits to be enforced for an individual user.
 */
public class UserQueryLimit {

    // The user DN.
    private final String userDn;

    // The user's concurrent query limit. This applies to the total number of queries the user may run across all systems.
    private final int queryLimit;

    // Set of query logic group limits sorted by best match and lowest limit.
    private final SortedSet<MatchableLimit> queryLogicGroupLimits;

    // The source of the limits (configuration vs. default limits).
    private final LimitSource source;

    /**
     * Returns a new {@link UserQueryLimit} with limits that originated from a matching user query limit configuration.
     *
     * @param userDn
     *            the user DN
     * @param queryLimit
     *            the query limit
     * @param queryLogicGroupLimits
     *            the overridden query logic group limits
     * @return the new {@link UserQueryLimit}
     */
    public static UserQueryLimit fromConfig(String userDn, int queryLimit, Set<MatchableLimit> queryLogicGroupLimits) {
        return new UserQueryLimit(userDn, queryLimit, queryLogicGroupLimits, LimitSource.CONFIG);
    }

    /**
     * Returns a new {@link UserQueryLimit} with limits that originated from the default limits.
     *
     * @param userDn
     *            the user DN
     * @param queryLimit
     *            the query limit
     * @return the new {@link UserQueryLimit}
     */
    public static UserQueryLimit fromDefaults(String userDn, int queryLimit) {
        return new UserQueryLimit(userDn, queryLimit, null, LimitSource.DEFAULTS);
    }

    private UserQueryLimit(String userDn, int queryLimit, Set<MatchableLimit> queryLogicGroupLimits, LimitSource source) {
        this.userDn = userDn;
        this.queryLimit = queryLimit;
        this.queryLogicGroupLimits = queryLogicGroupLimits == null ? Collections.emptySortedSet()
                        : Collections.unmodifiableSortedSet(new TreeSet<>(queryLogicGroupLimits));
        this.source = source;
    }

    public String getUserDn() {
        return userDn;
    }

    public int getQueryLimit() {
        return queryLimit;
    }

    public SortedSet<MatchableLimit> getQueryLogicGroupLimits() {
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
