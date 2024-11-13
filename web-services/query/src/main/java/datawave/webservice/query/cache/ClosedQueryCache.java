package datawave.webservice.query.cache;

import java.util.concurrent.TimeUnit;

import javax.inject.Singleton;

import org.apache.log4j.Logger;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * A cache for storing query ID of auto-closed queries.
 */
@Singleton
// CDI singleton
public class ClosedQueryCache {
    private final Cache<String,Boolean> queryIdCache = CacheBuilder.newBuilder().maximumSize(100000).expireAfterWrite(10, TimeUnit.MINUTES).build();
    private final Logger log = Logger.getLogger(ClosedQueryCache.class);

    public void add(String queryId) {
        queryIdCache.put(queryId, Boolean.TRUE);
        if (log.isDebugEnabled()) {
            log.debug("Added " + queryId + " to ClosedQueryCache");
        }
    }

    public void remove(String queryId) {
        queryIdCache.invalidate(queryId);
        if (log.isDebugEnabled()) {
            log.debug("Removed " + queryId + " to ClosedQueryCache");
        }
    }

    public boolean exists(String queryId) {
        boolean exists = queryIdCache.getIfPresent(queryId) != null;
        if (log.isDebugEnabled()) {
            log.debug(queryId + (exists ? " exists" : " does not exist") + " in ClosedQueryCache");
        }
        return exists;
    }
}
