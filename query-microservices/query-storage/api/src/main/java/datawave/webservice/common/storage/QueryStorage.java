package datawave.webservice.common.storage;

import datawave.webservice.query.Query;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * This is the interface to the query storage service
 */
public interface QueryStorage {
    /**
     * Store a new query. This will automatically create an initial CREATE query task and send a task notification message.
     * 
     * @param queryType
     *            The query type for this query
     * @param query
     *            The query to be executed
     * @return The assigned query id
     */
    default UUID storeQuery(QueryType queryType, Query query) {
        UUID uuid = UUID.randomUUID();
        Map<String,Object> props = new HashMap<>();
        props.put(QueryCheckpoint.INITIAL_QUERY_PROPERTY, query);
        QueryCheckpoint checkpoint = new QueryCheckpoint(uuid, queryType, props);
        QueryTask task = createTask(QueryTask.QUERY_ACTION.CREATE, checkpoint);
        return task.getTaskId();
    }
    
    /**
     * Store a new query task
     *
     * @param action
     *            The action to perform
     * @param checkpoint
     *            The query state/checkpoint to perform the action on
     * @return The new query task with a newly generated taskId
     */
    QueryTask createTask(QueryTask.QUERY_ACTION action, QueryCheckpoint checkpoint);
    
    /**
     * Get a query task for the specified queue
     *
     * @param msg
     *            The query task notification
     * @return the query task to perform
     */
    QueryTask getQueryTask(QueryTaskNotification msg);
    
    /**
     * Checkpoint a task in progress
     *
     * @param taskId
     *            A query task id
     * @param checkpoint
     *            The new checkpoint
     */
    void checkpointTask(UUID taskId, QueryCheckpoint checkpoint);
    
    /**
     * Complete a query task
     * 
     * @param taskId
     *            The completed query task id
     */
    void completeTask(UUID taskId);
}
