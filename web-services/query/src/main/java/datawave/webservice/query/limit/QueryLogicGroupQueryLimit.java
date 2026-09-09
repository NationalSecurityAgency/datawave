package datawave.webservice.query.limit;

import java.util.Objects;
import java.util.StringJoiner;

/**
 * This class represents the query limits to be enforced for query logic groups.
 */
public class QueryLogicGroupQueryLimit {

    // The query logic group name.
    private final String groupName;

    // The default concurrency limit for users.
    private final int queryLimit;

    // The source of the limits (configuration vs. default limits).
    private final LimitSource source;

    /**
     * Return a new {@link QueryLogicGroupQueryLimit} from a configuration.
     *
     * @param groupName
     *            the group name
     * @param queryLimit
     *            the query limit
     * @return the new {@link QueryLogicGroupQueryLimit}
     */
    public static QueryLogicGroupQueryLimit fromConfig(String groupName, int queryLimit) {
        return new QueryLogicGroupQueryLimit(groupName, queryLimit, LimitSource.CONFIG);
    }

    private QueryLogicGroupQueryLimit(String groupName, int queryLimit, LimitSource source) {
        this.queryLimit = queryLimit;
        this.source = source;
        this.groupName = groupName;
    }

    public String getGroupName() {
        return groupName;
    }

    public int getQueryLimit() {
        return queryLimit;
    }

    public LimitSource getSource() {
        return source;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryLogicGroupQueryLimit that = (QueryLogicGroupQueryLimit) o;
        return queryLimit == that.queryLimit && Objects.equals(groupName, that.groupName) && source == that.source;
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupName, queryLimit, source);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", QueryLogicGroupQueryLimit.class.getSimpleName() + "[", "]").add("groupName='" + groupName + "'")
                        .add("queryLimit=" + queryLimit).add("source=" + source).toString();
    }
}
