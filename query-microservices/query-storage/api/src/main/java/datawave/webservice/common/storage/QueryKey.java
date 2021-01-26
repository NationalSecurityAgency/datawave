package datawave.webservice.common.storage;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.UUID;

public class QueryKey implements Serializable {
    private static final long serialVersionUID = -2589618312956104322L;
    
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
        return queryId.toString() + ':' + type.getType();
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
