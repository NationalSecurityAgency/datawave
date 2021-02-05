package datawave.microservice.common.storage;

import java.util.List;

/**
 * This is the interface to the query storage state service
 */
public interface QueryStorageStateService {
    
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
    
    /**
     * Get the running queries for a query pool
     *
     * @param queryPool
     *            The query pool
     * @return The query states
     */
    List<QueryState> getRunningQueries(String queryPool);
}
