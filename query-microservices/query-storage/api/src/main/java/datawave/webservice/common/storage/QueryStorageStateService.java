package datawave.webservice.common.storage;

import java.util.List;

/**
 * This is the interface to the query storage state service
 */
public interface QueryStorageStateService {
    
    /**
     * Get the list of queries running.
     */
    List<QueryState> getRunningQueries();

    /**
     * Get the query state for a specified query state
     * @param queryId
     *         The query id
     * @return The query state
     */
    QueryState getQuery(String queryId);

    /**
     * Get the list of task descriptions for a query
     */
    List<TaskDescription> getTasks(String queryId);
    
    /**
     * Get the running queries for a query type
     */
    List<QueryState> getRunningQueries(String type);
}
