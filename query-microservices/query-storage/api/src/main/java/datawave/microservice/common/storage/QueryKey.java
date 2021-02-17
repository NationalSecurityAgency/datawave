package datawave.microservice.common.storage;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.UUID;

public class QueryKey implements Serializable {
    private static final long serialVersionUID = -2589618312956104322L;
    
    public static final String QUERY_ID_PREFIX = "Q:";
    public static final String POOL_PREFIX = "P:";
    public static final String LOGIC_PREFIX = "L:";
    
    private final QueryPool queryPool;
    private final UUID queryId;
    private final String queryLogic;
    
    public QueryKey(QueryPool queryPool, UUID queryId, String queryLogic) {
        this.queryPool = queryPool;
        this.queryId = queryId;
        this.queryLogic = queryLogic;
    }
    
    public QueryPool getQueryPool() {
        return queryPool;
    }
    
    public UUID getQueryId() {
        return queryId;
    }
    
    public String getQueryLogic() {
        return queryLogic;
    }
    
    public String toKey() {
        return POOL_PREFIX + queryPool.getName() + ' ' + QUERY_ID_PREFIX + queryId.toString() + ' ' + LOGIC_PREFIX + queryLogic;
    }
    
    public String toRoutingKey() {
        return POOL_PREFIX + queryPool.getName() + ".#";
    }
    
    @Override
    public String toString() {
        return toKey();
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof QueryKey) {
            QueryKey other = (QueryKey) o;
            return new EqualsBuilder().append(getQueryPool(), other.getQueryPool()).append(getQueryId(), other.getQueryId())
                            .append(getQueryLogic(), other.getQueryLogic()).isEquals();
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getQueryPool()).append(getQueryId()).append(getQueryLogic()).toHashCode();
    }
}
