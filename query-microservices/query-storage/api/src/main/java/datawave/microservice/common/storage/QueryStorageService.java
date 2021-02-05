package datawave.microservice.common.storage;

import datawave.webservice.query.Query;

import java.util.List;
import java.util.UUID;

/**
 * This is an interface to the query storage service
 */
public interface QueryStorageService {
    /**
     * Store/cache a new query. This will create a query task containing the query with a CREATE query action and send out a task notification.
     * 
     * @param queryPool
     *            The query pool
     * @param query
     *            The query parameters
     * @return The create task key
     */
    TaskKey storeQuery(QueryPool queryPool, Query query);
    
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
     * Update a stored query task with an updated checkpoint
     * 
     * @param taskKey
     *            The task key to update
     * @param checkpoint
     *            The new query checkpoint
     * @return The updated query task
     */
    QueryTask checkpointTask(TaskKey taskKey, QueryCheckpoint checkpoint);
    
    /**
     * Get a task for a given task key
     * 
     * @param taskKey
     *            The task key
     * @return The query task
     */
    QueryTask getTask(TaskKey taskKey);
    
    /**
     * Delete a query task
     * 
     * @param taskKey
     *            The task key
     */
    void deleteTask(TaskKey taskKey);
    
    /**
     * Get the tasks for a query
     *
     * @param queryId
     *            The query id
     * @return A list of tasks
     */
    List<QueryTask> getTasks(UUID queryId);
    
    /**
     * Get the tasks for a query
     *
     * @param queryPool
     *            The query pool
     * @return A list of tasks
     */
    List<QueryTask> getTasks(QueryPool queryPool);
    
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
}
