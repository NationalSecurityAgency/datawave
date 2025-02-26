package datawave.microservice.query.executor;

import java.util.List;

import datawave.microservice.query.storage.QueryState;
import datawave.microservice.query.storage.TaskDescription;

/**
 * This is the interface to the query storage state service
 */
public interface QueryExecutorStateService {
    
    /**
     * Get the list of queries running.
     *
     * @return The query states
     */
    List<QueryState> getRunningQueries();
    
    /**
     * Get the query state for a specified query state
     * 
     * @param queryId
     *            The query id
     * @return The query state
     */
    QueryState getQuery(String queryId);
    
    /**
     * Get the list of task descriptions for a query
     *
     * @param queryId
     *            The query id
     * @return The task descriptions
     */
    List<TaskDescription> getTasks(String queryId);
    
}
