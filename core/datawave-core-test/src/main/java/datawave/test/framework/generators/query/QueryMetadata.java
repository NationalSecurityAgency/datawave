package datawave.test.framework.generators.query;

import java.util.List;
import java.util.Objects;

/**
 * Encapsulates everything required for the AbstractQueryTest to run a query
 */
public class QueryMetadata {

    private final String query;
    private final String plan;
    private final List<Integer> ids;

    /**
     * Static entry point
     *
     * @param query
     *            the query
     * @param plan
     *            the query plan
     * @param ids
     *            the list of result event ids
     * @return the {@link QueryMetadata}
     */
    public static QueryMetadata of(String query, String plan, List<Integer> ids) {
        return new QueryMetadata(query, plan, ids);
    }

    /**
     * Private constructor helps enforce static access
     *
     * @param query
     *            the query
     * @param plan
     *            the query plan
     * @param ids
     *            the result event ids
     */
    private QueryMetadata(String query, String plan, List<Integer> ids) {
        this.query = query;
        this.plan = plan;
        this.ids = ids;
    }

    public String getQuery() {
        return query;
    }

    public String getPlan() {
        return plan;
    }

    public List<Integer> getIds() {
        return ids;
    }

    @Override
    public String toString() {
        return query;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryMetadata metadata = (QueryMetadata) o;
        return Objects.equals(query, metadata.query) && Objects.equals(plan, metadata.plan);
    }

    @Override
    public int hashCode() {
        // No need to include ids because the same query and plan will also produce the same ids
        return Objects.hash(query, plan);
    }
}
