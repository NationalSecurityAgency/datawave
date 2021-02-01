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
     * @param queryType
     *            The query type
     * @param query
     *            The query parameters
     * @return The query UUID
     */
    UUID storeQuery(QueryType queryType, Query query);
    
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
     * @param taskId
     *            The task id to update
     * @param checkpoint
     *            The new query checkpoint
     * @return The updated query task
     */
    QueryTask checkpointTask(UUID taskId, QueryCheckpoint checkpoint);
    
    /**
     * Get a task for a given task id
     * 
     * @param taskId
     *            The task id
     * @return The query task
     */
    QueryTask getTask(UUID taskId);
    
    /**
     * Delete a query task
     * 
     * @param taskId
     *            The task id
     * @return true if found and deleted
     */
    boolean deleteTask(UUID taskId);
    
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
     * @param type
     *            The query type
     * @return A list of tasks
     */
    List<QueryTask> getTasks(QueryType type);
    
    /**
     * Delete a query
     *
     * @param queryId
     *            the query id
     * @return true if deleted
     */
    public boolean deleteQuery(UUID queryId);
    
    /**
     * Delete all queries for a query type
     *
     * @param type
     *            The query type
     * @return true if anything deleted
     */
    public boolean deleteQueryType(QueryType type);
    
    /**
     * Clear the cache
     */
    public void clear();
}
