package datawave.webservice.common.storage;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * A query checkpoint will be very different depending on the query logic. It is expected that whatever the query state is can be encoded in a map of
 * properties.
 */
public class QueryCheckpoint implements Serializable {
    private static final long serialVersionUID = -9201879510622137934L;
    
    // This is the property name for the initial datawave.webservice.query.Query object
    public static final String INITIAL_QUERY_PROPERTY = "QUERY";
    
    private final QueryKey queryKey;
    private final Map<String,Object> properties;
    
    public QueryCheckpoint(UUID queryId, QueryType queryType, Map<String,Object> properties) {
        this(new QueryKey(queryType, queryId), properties);
    }
    
    public QueryCheckpoint(QueryKey queryKey, Map<String,Object> properties) {
        this.queryKey = queryKey;
        this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
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
     * Get the properties representing the state of the query.
     * 
     * @return The properties
     */
    public Map<String,Object> getProperties() {
        return properties;
    }
    
    @Override
    public String toString() {
        return getQueryKey() + ": " + getProperties();
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof QueryCheckpoint) {
            QueryCheckpoint other = (QueryCheckpoint) o;
            return new EqualsBuilder().append(getQueryKey(), other.getQueryKey()).append(getProperties(), other.getProperties()).isEquals();
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getQueryKey()).append(getProperties()).toHashCode();
    }
}
