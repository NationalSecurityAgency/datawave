package datawave.webservice.query.limit;

import java.util.Objects;
import java.util.StringJoiner;

/**
 * This class represents a sortable query logic group and its limit.
 */
public class QueryLogicGroupLimit implements Comparable<QueryLogicGroupLimit> {

    private final String groupName;
    private final Matcher matcher;
    private final int queryLimit;

    public QueryLogicGroupLimit(String groupName, Matcher matcher, int queryLimit) {
        this.groupName = groupName;
        this.matcher = matcher;
        this.queryLimit = queryLimit;
    }

    public String getGroupName() {
        return groupName;
    }

    public Matcher getMatcher() {
        return matcher;
    }

    public int getQueryLimit() {
        return queryLimit;
    }

    @Override
    public int compareTo(QueryLogicGroupLimit o) {
        // First sort by the matcher type, sorting in order EXACT, PARTIAL, then ALL.
        int comparison = matcher.getType().compareTo(o.matcher.getType());

        // Then sort by the query limit from lowest to highest.
        if (comparison == 0) {
            comparison = Integer.compare(queryLimit, o.queryLimit);
        }

        // Then sort by the group name. In practice, this should always be unique.
        if (comparison == 0) {
            comparison = groupName.compareTo(o.groupName);
        }
        return comparison;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryLogicGroupLimit that = (QueryLogicGroupLimit) o;
        return queryLimit == that.queryLimit && Objects.equals(groupName, that.groupName) && Objects.equals(matcher, that.matcher);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupName, matcher, queryLimit);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", QueryLogicGroupLimit.class.getSimpleName() + "[", "]").add("groupName='" + groupName + "'").add("matcher=" + matcher)
                        .add("queryLimit=" + queryLimit).toString();
    }
}
