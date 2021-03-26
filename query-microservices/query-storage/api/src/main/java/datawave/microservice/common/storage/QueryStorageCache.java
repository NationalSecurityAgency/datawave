package datawave.microservice.common.storage;

import datawave.webservice.query.Query;

import java.util.List;
import java.util.UUID;

/**
 * This is an interface to the query storage service
 */
public interface QueryStorageCache {
    /**
     * Store/cache a new query. This will create a query task containing the query with a CREATE query action and send out a task notification.
     * 
     * @param queryPool
     *            The query pool
     * @param query
     *            The query parameters
     * @param count
     *            The number of available locks which equates to the number of concurrent executors that can act on this query
     * @return The create task key
     */
    TaskKey storeQuery(QueryPool queryPool, Query query, int count);
    
    /**
     * Create a new query task. This will create a new query task, store it, and send out a task notification.
     * 
     * @param action
     *            The query action
     * @param checkpoint
     *            The query checkpoint
     * @return The new query task
     */
    QueryTask createTask(QueryTask.QUERY_ACTION action, QueryCheckpoint checkpoint);
    
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
    QueryTask getTask(TaskKey taskKey, long waitMs) throws TaskLockException;
    
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
    QueryTask checkpointTask(TaskKey taskKey, QueryCheckpoint checkpoint) throws TaskLockException;
    
    /**
     * Delete a query task. This will also release the lock. This will throw an exception if the task is not locked.
     * 
     * @param taskKey
     *            The task key
     * @throws TaskLockException
     *             if the task is not locked
     */
    void deleteTask(TaskKey taskKey) throws TaskLockException;
    
    /**
     * Delete a query
     *
     * @param queryId
     *            the query id
     * @return true if deleted
     */
    public boolean deleteQuery(UUID queryId);
    
    /**
     * Delete all queries for a query pool
     *
     * @param queryPool
     *            The query pool
     * @return true if anything deleted
     */
    public boolean deleteQueryPool(QueryPool queryPool);
    
    /**
     * Clear the cache
     */
    public void clear();
    
    /**
     * Get queries that are in storage for a specified query pool
     *
     * @param queryPool
     *            The query pool
     * @return The list of query IDs
     */
    public List<UUID> getQueries(QueryPool queryPool);
    
    /**
     * Get the tasks that are stored for a specified query
     *
     * @param queryId
     *            The query id
     * @return The list of task keys
     */
    public List<TaskKey> getTasks(UUID queryId);
}
