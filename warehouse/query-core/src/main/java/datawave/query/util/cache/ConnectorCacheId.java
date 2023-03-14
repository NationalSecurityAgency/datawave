package datawave.query.util.cache;

import java.util.Set;

import datawave.ingest.util.cache.CacheId;

import org.apache.accumulo.core.security.Authorizations;

/**
 * 
 */
public class ConnectorCacheId extends CacheId {
    
    String toString;
    
    String authorizations;
    
    int hashCode = 31;
    
    public ConnectorCacheId(String instanceId, Set<Authorizations> auths) {
        super(instanceId);
        
        this.authorizations = auths.toString();
        
        StringBuilder builder = new StringBuilder(instanceId).append("\\").append(authorizations);
        
        toString = builder.toString();
        
        hashCode += toString.hashCode();
    }
    
    @Override
    public String toString() {
        return toString;
    }
    
    @Override
    public int hashCode() {
        return hashCode;
    }
    
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ConnectorCacheId))
            return false;
        ConnectorCacheId other = (ConnectorCacheId) o;
        return other.toString.equals(toString);
    }
}
