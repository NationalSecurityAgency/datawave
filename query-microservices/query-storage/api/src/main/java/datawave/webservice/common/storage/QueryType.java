package datawave.webservice.common.storage;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * A query type. It is expected that this type will correspond to a class of query executor service.
 */
public class QueryType {
    private String type;
    
    public QueryType(String type) {
        this.type = type;
    }
    
    public String getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof QueryType) {
            QueryType other = (QueryType)o;
            return new EqualsBuilder()
                    .append(getType(), other.getType())
                    .isEquals();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(getType())
                .toHashCode();
    }
}
