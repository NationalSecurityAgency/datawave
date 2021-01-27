package datawave.webservice.common.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;

/**
 * A query type. It is expected that this type will correspond to a class of query executor service.
 */
public class QueryType implements Serializable {
    private static final long serialVersionUID = -1790098342235290281L;
    
    private final String type;

    @JsonCreator
    public QueryType(@JsonProperty("type") String type) {
        this.type = type;
    }
    
    public String getType() {
        return type;
    }
    
    @Override
    public String toString() {
        return type;
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof QueryType) {
            QueryType other = (QueryType) o;
            return new EqualsBuilder().append(getType(), other.getType()).isEquals();
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getType()).toHashCode();
    }
}
