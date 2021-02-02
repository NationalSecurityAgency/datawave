package datawave.microservice.common.storage;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.UUID;

public class QueryKey implements Serializable {
    private static final long serialVersionUID = -2589618312956104322L;
    
    public static final String QUERY_ID_PREFIX = " Q:";
    public static final String TYPE_PREFIX = " T:";
    
    private final QueryType type;
    private final UUID queryId;
    
    public QueryKey(QueryType type, UUID queryId) {
        this.type = type;
        this.queryId = queryId;
    }
    
    public QueryType getType() {
        return type;
    }
    
    public UUID getQueryId() {
        return queryId;
    }
    
    public String toKey() {
        return QUERY_ID_PREFIX + queryId.toString() + TYPE_PREFIX + type.getType();
    }
    
    @Override
    public String toString() {
        return toKey();
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof QueryKey) {
            QueryKey other = (QueryKey) o;
            return new EqualsBuilder().append(getType(), other.getType()).append(getQueryId(), other.getQueryId()).isEquals();
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getType()).append(getQueryId()).toHashCode();
    }
}
