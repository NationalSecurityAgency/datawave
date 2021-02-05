package datawave.microservice.common.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;

/**
 * A query pool
 */
public class QueryPool implements Serializable {
    private static final long serialVersionUID = -1790098342235290281L;
    
    private final String name;
    
    @JsonCreator
    public QueryPool(@JsonProperty("name") String name) {
        this.name = name;
    }
    
    public String getName() {
        return name;
    }
    
    @Override
    public String toString() {
        return name;
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof QueryPool) {
            QueryPool other = (QueryPool) o;
            return new EqualsBuilder().append(getName(), other.getName()).isEquals();
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getName()).toHashCode();
    }
}
