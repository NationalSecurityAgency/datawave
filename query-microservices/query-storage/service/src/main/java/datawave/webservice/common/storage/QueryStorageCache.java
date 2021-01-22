package datawave.webservice.common.storage;

import datawave.microservice.cached.LockableCacheInspector;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;

import javax.annotation.CheckReturnValue;

import java.util.UUID;

@CacheConfig(cacheNames = QueryStorageCache.CACHE_NAME)
public class QueryStorageCache {
    
    public static final String CACHE_NAME = "QueryCache";
    
    private final LockableCacheInspector cacheInspector;
    
    public QueryStorageCache(LockableCacheInspector cacheInspector) {
        this.cacheInspector = cacheInspector;
    }
    
    @CachePut(key = "#result.taskId")
    public QueryTask addQueryTask(QueryTask.QUERY_ACTION action, QueryCheckpoint checkpoint) {
        return new QueryTask(action, checkpoint);
    }
    
    @CachePut(key = "#taskId")
    public QueryTask updateQueryTask(UUID taskId, QueryCheckpoint checkpoint) {
        QueryTask task = getTask(taskId);
        if (task == null) {
            throw new NullPointerException("Could not find a query task for " + taskId);
        }
        return new QueryTask(task.getTaskId(), task.getAction(), checkpoint);
    }
    
    public QueryTask getTask(UUID taskId) {
        return cacheInspector.list(CACHE_NAME, QueryTask.class, taskId.toString());
    }
    
    @CacheEvict(key = "#taskId")
    @CheckReturnValue
    public String deleteTask(UUID taskId) {
        return "Evicted " + taskId;
    }
    
}
