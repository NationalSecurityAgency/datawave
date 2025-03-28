package datawave.microservice.query.storage;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;

import datawave.core.query.logic.QueryKey;
import datawave.microservice.cached.LockableCacheInspector;

@CacheConfig(cacheNames = QueryStatusCache.CACHE_NAME)
public class QueryStatusCache {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    public static final String CACHE_NAME = "QueryStatusCache";
    
    private final LockableCacheInspector cacheInspector;
    
    public QueryStatusCache(LockableCacheInspector cacheInspector) {
        this.cacheInspector = cacheInspector;
    }
    
    /**
     * Store the query status for a query.
     * 
     * @param queryStatus
     *            the query status
     * @return the stored query status
     */
    @CachePut(key = "#queryStatus.getQueryKey().toUUIDKey()")
    public QueryStatus updateQueryStatus(QueryStatus queryStatus) {
        logStatus("Storing", queryStatus, queryStatus.getQueryKey().getQueryId());
        return queryStatus;
    }
    
    /**
     * Delete the query status for a query
     * 
     * @param queryId
     *            The query id
     */
    @CacheEvict(key = "T(datawave.core.query.logic.QueryKey).toUUIDKey(#queryId)")
    public void deleteQueryStatus(String queryId) {
        if (log.isDebugEnabled()) {
            log.debug("Deleted query status for " + queryId);
        }
    }
    
    /**
     * Return the query status for a query
     *
     * @param queryId
     *            The query id
     * @return The query status
     */
    public QueryStatus getQueryStatus(String queryId) {
        QueryStatus props = null;
        try {
            props = cacheInspector.list(CACHE_NAME, QueryStatus.class, QueryKey.toUUIDKey(queryId));
            logStatus("Retrieved", props, queryId);
        } catch (RuntimeException e) {
            log.error("Failed to retrieve status for " + queryId, e);
            throw e;
        }
        return props;
    }
    
    /**
     * Get all of the existing query status
     * 
     * @return A list of query status
     */
    public List<QueryStatus> getQueryStatus() {
        List<? extends Object> queryStatuses = cacheInspector.listAll(CACHE_NAME, Object.class);
        if (queryStatuses == null) {
            return Collections.emptyList();
        }
        return queryStatuses.stream().filter(o -> o instanceof QueryStatus).map(QueryStatus.class::cast).collect(Collectors.toList());
    }
    
    /**
     * Clear out the cache
     *
     * @return a clear message
     */
    @CacheEvict(allEntries = true, beforeInvocation = true)
    public String clear() {
        return "Cleared " + CACHE_NAME + " cache";
    }
    
    /**
     * A convience method for logging query status
     *
     * @param msg
     *            A message to prepend
     * @param status
     *            the query status
     * @param key
     *            the query id
     */
    private void logStatus(String msg, QueryStatus status, String key) {
        if (log.isTraceEnabled()) {
            log.trace(msg + ' ' + (status == null ? "null query for " + key : status.toString()));
        } else if (log.isDebugEnabled()) {
            log.debug(msg + ' ' + (status == null ? "null query for " + key : "query for " + key));
        }
    }
    
    /**
     * Get a query status lock for a given query id.
     * 
     * @param queryId
     *            the query id
     * @return a query status lock
     */
    public QueryStorageLock getQueryStatusLock(String queryId) {
        return new QueryStatusLock(queryId);
    }
    
    /**
     * A lock object for a query status
     */
    public class QueryStatusLock extends QueryStorageLockImpl {
        public QueryStatusLock(String queryId) {
            super(CACHE_NAME, QueryKey.toUUIDKey(queryId), cacheInspector);
        }
    }
    
}
