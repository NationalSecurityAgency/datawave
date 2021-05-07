package datawave.microservice.common.storage;

import datawave.microservice.common.storage.config.QueryStorageProperties;
import datawave.webservice.query.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

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
    public TaskKey storeQuery(QueryPool queryPool, Query query, int count) throws IOException {
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
        
        // store the initial query properties
        QueryStatus queryStatus = new QueryStatus(checkpoint.getQueryKey());
        queryStatus.setQuery(query);
        cache.updateQueryStatus(queryStatus);
        
        // create and store the initial create task with the checkpoint. This will send out the task notification.
        QueryTask task = createTask(QueryTask.QUERY_ACTION.CREATE, checkpoint);
        
        // return the task key
        return task.getTaskKey();
    }
    
    /**
     * Get the current query state. This includes the query properties, stats, and task summary
     * 
     * @param queryId
     *            the query id
     * @return query stats
     */
    public QueryState getQueryState(UUID queryId) {
        return cache.getQuery(queryId);
    }
    
    /**
     * Get the current query properties.
     * 
     * @param queryId
     *            the query id
     * @return the query properties
     */
    public QueryStatus getQueryStatus(UUID queryId) {
        return cache.getQueryStatus(queryId);
    }
    
    /**
     * Get all query propertis
     * 
     * @return the query properties
     */
    public List<QueryStatus> getQueryStatus() {
        return cache.getQueryStatus();
    }
    
    /**
     * update the query properties
     * 
     * @param queryStatus
     *            the query properties
     */
    public void updateQueryStatus(QueryStatus queryStatus) {
        cache.updateQueryStatus(queryStatus);
    }
    
    /**
     * Get the current task states.
     *
     * @param queryId
     *            the query id
     * @return the task states
     */
    @Override
    public TaskStates getTaskStates(UUID queryId) {
        return cache.getTaskStates(queryId);
    }
    
    /**
     * update the query status
     *
     * @param taskStates
     *            the task states
     */
    @Override
    public void updateTaskStates(TaskStates taskStates) {
        cache.updateTaskStates(taskStates);
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
    public QueryTask getTask(TaskKey taskKey, long waitMs) throws TaskLockException, IOException, InterruptedException {
        if (lockManager.getLock(taskKey).tryLock(waitMs, TimeUnit.MILLISECONDS)) {
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
    public QueryTask checkpointTask(TaskKey taskKey, QueryCheckpoint checkpoint) throws TaskLockException, IOException {
        if (lockManager.isLocked(taskKey)) {
            QueryTask queryTask = cache.updateQueryTask(taskKey, checkpoint);
            lockManager.getLock(taskKey).unlock();
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
    public void deleteTask(TaskKey taskKey) throws TaskLockException, IOException {
        if (lockManager.isLocked(taskKey)) {
            cache.deleteTask(taskKey);
            lockManager.getLock(taskKey).unlock();
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
    public boolean deleteQuery(UUID queryId) throws IOException {
        int deleted = cache.deleteQuery(queryId);
        queue.deleteQueue(queryId);
        lockManager.deleteSemaphore(queryId);
        return (deleted > 0);
    }
    
    /**
     * Clear the cache
     */
    @Override
    public void clear() throws IOException {
        for (QueryState queries : cache.getQueries()) {
            queue.emptyQueue(queries.getQueryId());
            lockManager.deleteSemaphore(queries.getQueryId());
        }
        cache.clear();
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
