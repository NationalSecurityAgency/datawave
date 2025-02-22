package datawave.microservice.query.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;

import datawave.core.query.logic.QueryKey;
import datawave.microservice.cached.LockableCacheInspector;

@CacheConfig(cacheNames = TaskStatesCache.CACHE_NAME)
public class TaskStatesCache {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    public static final String CACHE_NAME = "TaskStatesCache";
    
    private final LockableCacheInspector cacheInspector;
    
    public TaskStatesCache(LockableCacheInspector cacheInspector) {
        this.cacheInspector = cacheInspector;
    }
    
    /**
     * Store the task states for a query.
     * 
     * @param taskStates
     *            the task states
     * @return the stored task states
     */
    @CachePut(key = "#taskStates.getQueryKey().toUUIDKey()")
    public TaskStates updateTaskStates(TaskStates taskStates) {
        logStatus("Storing", taskStates, taskStates.getQueryKey().getQueryId());
        return taskStates;
    }
    
    /**
     * Delete the task states for a query
     * 
     * @param queryId
     *            The query id
     */
    @CacheEvict(key = "T(datawave.core.query.logic.QueryKey).toUUIDKey(#queryId)")
    public void deleteTaskStates(String queryId) {
        if (log.isDebugEnabled()) {
            log.debug("Deleted task statuses for " + queryId);
        }
    }
    
    /**
     * Return the task states for a query
     *
     * @param queryId
     *            The query id
     * @return The task states
     */
    public TaskStates getTaskStates(String queryId) {
        TaskStates states = cacheInspector.list(CACHE_NAME, TaskStates.class, QueryKey.toUUIDKey(queryId));
        logStatus("Retrieved", states, queryId);
        return states;
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
     * A convenience method for logging task statuses
     *
     * @param msg
     *            A message to prepend
     * @param status
     *            the task statueses
     * @param key
     *            the query id
     */
    private void logStatus(String msg, TaskStates status, String key) {
        if (log.isTraceEnabled()) {
            log.trace(msg + ' ' + (status == null ? "null tasks for " + key : status.toString()));
        } else if (log.isDebugEnabled()) {
            log.debug(msg + ' ' + (status == null ? "null tasks for " + key : "tasks for " + key));
        }
    }
    
    /**
     * Get a query task states lock for a given query id
     * 
     * @param queryId
     *            the query id
     * @return a task states lock
     */
    public QueryStorageLock getTaskStatesLock(String queryId) {
        return new TaskStatesLock(queryId);
    }
    
    /**
     * A lock object for the task states
     */
    public class TaskStatesLock extends QueryStorageLockImpl {
        public TaskStatesLock(String queryId) {
            super(CACHE_NAME, QueryKey.toUUIDKey(queryId), cacheInspector);
        }
    }
    
}
