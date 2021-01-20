package datawave.webservice.common.storage;

import datawave.webservice.query.Query;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * This is the interface to the query storage service
 */
public interface QueryStorage {
    /**
     * Store a new query. This will automatically create an initial CREATE query task.
     * 
     * @param queryType
     *            The query type for this query
     * @param query
     *            The query to be executed
     * @return The assigned query id
     */
    default UUID storeQuery(QueryType queryType, Query query) {
        UUID uuid = UUID.randomUUID();
        Map<String, Object> props = new HashMap<>();
        props.put(QueryCheckpoint.INITIAL_QUERY_PROPERTY, query);
        QueryCheckpoint checkpoint = new QueryCheckpoint(uuid, queryType, props);
        this.addQueryTasks(Collections.singletonList(new QueryTask(QueryTask.QUERY_ACTION.CREATE, checkpoint)));
        return uuid;
    }
    
    /**
     * Add a set of query tasks to be performed for a query
     * 
     * @param tasks
     *            A list of query tasks to perform
     */
    void addQueryTasks(List<QueryTask> tasks);
    
    /**
     * Get a query task for the specified queue
     *
     * @param queryType
     *            The type of query for which to get a query task
     * @return a query state or null if none to be found
     * @throws java.lang.Exception
     *             if there is any problem retrieving a query
     */
    QueryTask getQueryTask(QueryType queryType);
    
    /**
     * Checkpoint a task in progress
     * 
     * @param task
     *            A query task checkpoint
     */
    void checkpointTask(QueryTask task);
    
    /**
     * Complete a query task
     * 
     * @param task
     *            The completed query task
     */
    void completeTask(QueryTask task);
}
