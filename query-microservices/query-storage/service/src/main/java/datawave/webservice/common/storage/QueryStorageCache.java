package datawave.webservice.common.storage;

import datawave.microservice.cached.LockableCacheInspector;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;

import javax.annotation.CheckReturnValue;
import java.util.List;
import java.util.UUID;

@CacheConfig(cacheNames = QueryStorageCache.CACHE_NAME)
public class QueryStorageCache {
    
    public static final String CACHE_NAME = "QueryCache";
    
    private final LockableCacheInspector cacheInspector;
    
    public QueryStorageCache(LockableCacheInspector cacheInspector) {
        this.cacheInspector = cacheInspector;
    }
    
    /**
     * This will create and store a new query task.
     * 
     * @param action
     *            The query action
     * @param checkpoint
     *            The query checkpoint
     * @return The new query task
     */
    @CachePut(key = "#result.taskId")
    public QueryTask addQueryTask(QueryTask.QUERY_ACTION action, QueryCheckpoint checkpoint) {
        return new QueryTask(action, checkpoint);
    }
    
    /**
     * Update a stored query task with an updated checkpoint
     * 
     * @param taskId
     *            The task id to update
     * @param checkpoint
     *            The new query checkpoint
     * @return The updated query task
     */
    @CachePut(key = "#taskId")
    public QueryTask updateQueryTask(UUID taskId, QueryCheckpoint checkpoint) {
        QueryTask task = getTask(taskId);
        if (task == null) {
            throw new NullPointerException("Could not find a query task for " + taskId);
        }
        return new QueryTask(task.getTaskId(), task.getAction(), checkpoint);
    }
    
    /**
     * Get a task for a given task id
     * 
     * @param taskId
     *            The task id
     * @return The query task
     */
    public QueryTask getTask(UUID taskId) {
        return cacheInspector.list(CACHE_NAME, QueryTask.class, taskId.toString());
    }
    
    /**
     * Delete a task
     * 
     * @param taskId
     *            The task to delete
     * @return True if found and deleted
     */
    public boolean deleteTask(UUID taskId) {
        QueryTask task = getTask(taskId);
        if (task != null) {
            _deleteTask(taskId);
            return true;
        }
        return false;
    }
    
    /**
     * Delete a query task
     * 
     * @param taskId
     *            The task id
     * @return An eviction message
     */
    @CacheEvict(key = "#taskId")
    @CheckReturnValue
    private String _deleteTask(UUID taskId) {
        return "Evicted " + taskId;
    }
    
    public List<QueryState> getQueries() {
        // TODO
        return null;
    }
    
    public List<TaskDescription> getTasks(UUID queryId) {
        // TODO
        return null;
    }
    
    public List<QueryState> getQueries(QueryType type) {
        // TODO
        return null;
    }
}
