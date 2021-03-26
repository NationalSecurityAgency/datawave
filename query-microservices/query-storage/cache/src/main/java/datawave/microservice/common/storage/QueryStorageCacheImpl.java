package datawave.microservice.common.storage;

import datawave.microservice.common.storage.config.QueryStorageProperties;
import datawave.webservice.query.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
public class QueryStorageCacheImpl implements QueryStorageCache {
    
    @Autowired
    private QueryLockManager lockManager;
    
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
     * @param count
     *            The number of available locks which equates to the number of concurrent executors that can act on this query
     * @return The task key
     */
    @Override
    public TaskKey storeQuery(QueryPool queryPool, Query query, int count) {
        UUID queryUuid = query.getId();
        if (queryUuid == null) {
            // create the query UUID
            queryUuid = UUID.randomUUID();
            query.setId(queryUuid);
        }
        
        // create the initial query checkpoint
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryPool, queryUuid, query.getQueryLogicName(), query);
        
        // create the query semaphore
        lockManager.createSemaphore(queryUuid, count);
        
        // create and store the initial create task with the checkpoint. This will send out the task notification.
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
     * Get a task for a given task key and lock it for processing. This return null if the task no longer exists. This will throw an exception if the task is
     * already locked.
     *
     * @param taskKey
     *            The task key
     * @param waitMs
     *            How long to wait to get a task lock
     * @return The query task, null if deleted
     * @throws TaskLockException
     *             if the task is already locked
     */
    @Override
    public QueryTask getTask(TaskKey taskKey, long waitMs) throws TaskLockException {
        if (lockManager.acquireLock(taskKey, waitMs)) {
            return cache.getTask(taskKey);
        } else {
            return null;
        }
    }
    
    /**
     * Update a stored query task with an updated checkpoint. This will also release the lock. This will throw an exception is the task is not locked.
     *
     * @param taskKey
     *            The task key to update
     * @param checkpoint
     *            The new query checkpoint
     * @return The updated query task
     * @throws TaskLockException
     *             if the task is not locked
     */
    @Override
    public QueryTask checkpointTask(TaskKey taskKey, QueryCheckpoint checkpoint) throws TaskLockException {
        if (lockManager.isLocked(taskKey)) {
            QueryTask queryTask = cache.updateQueryTask(taskKey, checkpoint);
            lockManager.releaseLock(taskKey);
            return queryTask;
        } else {
            throw new TaskLockException("Attempting to checkpoint a task that is not locked: " + taskKey);
        }
    }
    
    /**
     * Delete a query task. This will also release the lock. This will throw an exception if the task is not locked.
     *
     * @param taskKey
     *            The task key
     * @throws TaskLockException
     *             if the task is not locked
     */
    @Override
    public void deleteTask(TaskKey taskKey) throws TaskLockException {
        if (lockManager.isLocked(taskKey)) {
            cache.deleteTask(taskKey);
            lockManager.releaseLock(taskKey);
        } else {
            throw new TaskLockException("Attempting to delete a task that is not locked: " + taskKey);
        }
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
        int deletedTasks = cache.deleteTasks(queryId);
        lockManager.deleteSemaphore(queryId);
        return (deletedTasks > 0);
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
        int deletedTasks = cache.deleteTasks(queryPool);
        for (QueryState query : cache.getQueries(queryPool)) {
            lockManager.deleteSemaphore(query.getQueryId());
        }
        return (deletedTasks > 0);
    }
    
    /**
     * Clear the cache
     */
    @Override
    public void clear() {
        cache.clear();
        for (QueryState queries : cache.getQueries()) {
            lockManager.deleteSemaphore(queries.getQueryId());
        }
    }
    
    /**
     * Get queries that are in storage for a specified query pool
     *
     * @param queryPool
     *            The query pool
     * @return The list of query IDs
     */
    @Override
    public List<UUID> getQueries(QueryPool queryPool) {
        List<UUID> queries = new ArrayList<>();
        for (QueryState query : cache.getQueries(queryPool)) {
            queries.add(query.getQueryId());
        }
        return queries;
    }
    
    /**
     * Get the tasks that are stored for a specified query
     *
     * @param queryId
     *            The query id
     * @return The list of task keys
     */
    @Override
    public List<TaskKey> getTasks(UUID queryId) {
        List<TaskKey> tasks = new ArrayList<>();
        for (QueryTask task : cache.getTasks(queryId)) {
            tasks.add(task.getTaskKey());
        }
        return tasks;
    }
    
}
