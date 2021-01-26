package datawave.webservice.common.storage;

import datawave.microservice.cached.LockableCacheInspector;
import org.apache.commons.lang3.mutable.MutableInt;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;

import javax.annotation.CheckReturnValue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    @CachePut(key = "#result.toKey()")
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
    @CachePut(key = "QueryTask.toKey(#taskId, #checkpoint.queryKey)")
    public QueryTask updateQueryTask(UUID taskId, QueryCheckpoint checkpoint) {
        QueryTask task = getTask(taskId, checkpoint.getQueryKey());
        if (task == null) {
            throw new NullPointerException("Could not find a query task for " + taskId);
        }
        return new QueryTask(task.getTaskId(), task.getAction(), checkpoint);
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
            _deleteTask(task);
            return true;
        }
        return false;
    }
    
    /**
     * Delete a query task
     * 
     * @param task
     *            The task
     * @return An eviction message
     */
    @CacheEvict(key = "#task.getKey()")
    @CheckReturnValue
    private String _deleteTask(QueryTask task) {
        return "Evicted " + task.toKey();
    }
    
    /**
     * Get a task for a given task id
     *
     * @param taskId
     *            The task id
     * @return The query task
     */
    public QueryTask getTask(UUID taskId) {
        List<? extends QueryTask> tasks = cacheInspector.listMatching(CACHE_NAME, QueryTask.class, taskId.toString());
        if (tasks == null || tasks.isEmpty()) {
            return null;
        } else if (tasks.size() == 1) {
            return tasks.get(0);
        } else {
            throw new IllegalStateException("Found " + tasks.size() + " tasks matching the specified taskId : " + taskId);
        }
    }
    
    /**
     * Return the task for a given task id and query key
     * 
     * @param taskId
     *            The task id
     * @param queryKey
     *            The query key
     * @return The query task
     */
    public QueryTask getTask(UUID taskId, QueryKey queryKey) {
        return cacheInspector.list(CACHE_NAME, QueryTask.class, QueryTask.toKey(taskId, queryKey));
    }
    
    /**
     * Get a list of query states from the cache
     * 
     * @return A list of query states
     */
    public List<QueryState> getQueries() {
        Map<QueryKey,Map<QueryTask.QUERY_ACTION,Integer>> queries = new HashMap<>();
        for (QueryTask task : cacheInspector.listAll(CACHE_NAME, QueryTask.class)) {
            // TODO
        }
        List<QueryState> states = new ArrayList<>();
        for (Map.Entry<QueryKey,Map<QueryTask.QUERY_ACTION,Integer>> entry : queries.entrySet()) {
            QueryState state = new QueryState(entry.getKey(), entry.getValue());
            states.add(state);
        }
        return states;
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
