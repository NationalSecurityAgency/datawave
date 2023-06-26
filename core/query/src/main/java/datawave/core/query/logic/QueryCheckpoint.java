package datawave.core.query.logic;

import datawave.core.query.configuration.QueryData;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

/**
 * A query checkpoint will be very different depending on the query logic. It is expected that whatever the query state is can be encoded in a map of
 * properties.
 */
public class QueryCheckpoint implements Serializable {
    private static final long serialVersionUID = -9201879510622137934L;

    private final QueryKey queryKey;
    private final Collection<QueryData> queries;

    public QueryCheckpoint(String queryPool, String queryId, String queryLogic, Collection<QueryData> queries) {
        this(new QueryKey(queryPool, queryId, queryLogic), queries);
    }

    public QueryCheckpoint(QueryKey queryKey) {
        this.queryKey = queryKey;
        this.queries = null;
    }

    public QueryCheckpoint(QueryKey queryKey, Collection<QueryData> queries) {
        this.queryKey = queryKey;
        this.queries = queries;
    }

    public QueryCheckpoint(QueryCheckpoint checkpoint) {
        this.queryKey = new QueryKey(checkpoint.queryKey.toString());
        this.queries = new ArrayList<>(checkpoint.queries.size());
        for (QueryData query : checkpoint.queries) {
            this.queries.add(new QueryData(query));
        }
    }

    /**
     * Get the query key
     *
     * @return the query key
     */
    public QueryKey getQueryKey() {
        return queryKey;
    }

    /**
     * Get the QueryData objects representing the state of the query.
     *
     * @return The QueryData objects representing the query checkpoint
     */
    public Collection<QueryData> getQueries() {
        return queries;
    }

    @Override
    public String toString() {
        return getQueryKey() + ": " + getQueries();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof QueryCheckpoint) {
            QueryCheckpoint other = (QueryCheckpoint) o;
            return new EqualsBuilder().append(getQueryKey(), other.getQueryKey()).append(getQueries(), other.getQueries()).isEquals();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getQueryKey()).append(getQueries()).toHashCode();
    }
}
