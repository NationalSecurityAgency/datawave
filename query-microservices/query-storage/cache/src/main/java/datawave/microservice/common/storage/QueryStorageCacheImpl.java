package datawave.microservice.common.storage;

import datawave.microservice.common.storage.config.QueryStorageProperties;
import datawave.webservice.query.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

@Service
public class QueryStorageCacheImpl implements QueryStorageCache {
    
    @Autowired
    private QueryCache cache;
    
    @Autowired
    private QueryQueueManager queue;
    
    @Autowired
    private QueryStorageProperties properties;
    
    /**
     * Store/cache a new query. This will create a query task containing the query with a CREATE query action and send out a task notification on a channel
     * using the name of the query logic.
     * 
     * @param queryPool
     *            The query pool
     * @param query
     *            The query parameters
     * @return The task key
     */
    @Override
    public TaskKey storeQuery(QueryPool queryPool, Query query) {
        UUID queryUuid = query.getId();
        if (queryUuid == null) {
            // create the query UUID
            queryUuid = UUID.randomUUID();
            query.setId(queryUuid);
        }
        
        // create the initial query checkpoint
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryPool, queryUuid, query.getQueryLogicName(), query);
        
        // create and store the initial create task with the checkpoint
        QueryTask task = createTask(QueryTask.QUERY_ACTION.CREATE, checkpoint);
        
        // return the task key
        return task.getTaskKey();
    }
    
    /**
     * Create a new query task. This will create a new query task, store it, and send out a task notification.
     * 
     * @param action
     *            The query action
     * @param checkpoint
     *            The query checkpoint
     * @return The new query task
     */
    @Override
    public QueryTask createTask(QueryTask.QUERY_ACTION action, QueryCheckpoint checkpoint) {
        // create a query task in the cache
        QueryTask task = cache.addQueryTask(action, checkpoint);
        
        // send a task notification
        if (properties.isSendNotifications()) {
            queue.sendMessage(task.getNotification());
        }
        
        // return the task
        return task;
    }
    
    /**
     * Update a stored query task with an updated checkpoint
     * 
     * @param taskKey
     *            The task key to update
     * @param checkpoint
     *            The new query checkpoint
     * @return The updated query task
     */
    @Override
    public QueryTask checkpointTask(TaskKey taskKey, QueryCheckpoint checkpoint) {
        return cache.updateQueryTask(taskKey, checkpoint);
    }
    
    /**
     * Get a task for a given task key
     * 
     * @param taskKey
     *            The task key
     * @return The query task
     */
    @Override
    public QueryTask getTask(TaskKey taskKey) {
        return cache.getTask(taskKey);
    }
    
    /**
     * Delete a query task
     * 
     * @param taskKey
     *            The task key
     */
    @Override
    public void deleteTask(TaskKey taskKey) {
        cache.deleteTask(taskKey);
    }
    
    /**
     * Get the tasks for a query
     *
     * @param queryId
     *            The query id
     * @return A list of tasks
     */
    @Override
    public List<QueryTask> getTasks(UUID queryId) {
        return cache.getTasks(queryId);
    }
    
    /**
     * Get the tasks for a query
     *
     * @param queryPool
     *            The query pool
     * @return A list of tasks
     */
    @Override
    public List<QueryTask> getTasks(QueryPool queryPool) {
        return cache.getTasks(queryPool);
    }
    
    /**
     * Delete a query
     *
     * @param queryId
     *            the query id
     * @return true if deleted
     */
    @Override
    public boolean deleteQuery(UUID queryId) {
        return (cache.deleteTasks(queryId) > 0);
    }
    
    /**
     * Delete all queries for a query pool
     *
     * @param queryPool
     *            The query pool
     * @return true if anything deleted
     */
    @Override
    public boolean deleteQueryPool(QueryPool queryPool) {
        return (cache.deleteTasks(queryPool) > 0);
    }
    
    /**
     * Clear the cache
     */
    @Override
    public void clear() {
        cache.clear();
    }
    
}
