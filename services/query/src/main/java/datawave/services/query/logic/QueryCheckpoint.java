package datawave.services.query.logic;

import datawave.services.query.configuration.GenericQueryConfiguration;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;

/**
 * A query checkpoint will be very different depending on the query logic. It is expected that whatever the query state is can be encoded in a map of
 * properties.
 */
public class QueryCheckpoint implements Serializable {
    private static final long serialVersionUID = -9201879510622137934L;
    
    private final QueryKey queryKey;
    private final GenericQueryConfiguration config;
    
    public QueryCheckpoint(String queryPool, String queryId, String queryLogic, GenericQueryConfiguration config) {
        this(new QueryKey(queryPool, queryId, queryLogic), config);
    }
    
    public QueryCheckpoint(QueryKey queryKey) {
        this.queryKey = queryKey;
        this.config = null;
    }
    
    public QueryCheckpoint(QueryKey queryKey, GenericQueryConfiguration config) {
        this.queryKey = queryKey;
        this.config = config;
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
     * Get the configuration representing the state of the query.
     * 
     * @return The configuration
     */
    public GenericQueryConfiguration getConfig() {
        return config;
    }
    
    @Override
    public String toString() {
        return getQueryKey() + ": " + getConfig();
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof QueryCheckpoint) {
            QueryCheckpoint other = (QueryCheckpoint) o;
            return new EqualsBuilder().append(getQueryKey(), other.getQueryKey()).append(getConfig(), other.getConfig()).isEquals();
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getQueryKey()).append(getConfig()).toHashCode();
    }
}
