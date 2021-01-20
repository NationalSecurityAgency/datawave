package datawave.webservice.common.storage;

public interface QueryStorage {
    
    /**
     * update a query state
     *
     * @param queryState
     *            The query state
     * @return uuid for the query (generated if not already in the queryState)
     * @throws Exception
     *             if there is any problem storing
     */
    String update(QueryState queryState) throws Exception;
    
    /**
     * Get a query state using the provided priority policy
     *
     * @param queryPriorityPolicy
     * @return a query state or null if none to be found
     * @throws java.lang.Exception
     *             if there is any problem retrieving a query
     */
    QueryState getQuery(QueryPriorityPolicy queryPriorityPolicy);
    
}
