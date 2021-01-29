package datawave.microservice.common.storage;

import datawave.webservice.query.Query;

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
     * @param taskNotification
     *            The task notification
     * @return The query task
     */
    QueryTask getTask(QueryTaskNotification taskNotification);
    
    /**
     * Delete a query task
     * 
     * @param taskId
     *            The task id
     * @return true if found and deleted
     */
    boolean deleteTask(UUID taskId);
}
