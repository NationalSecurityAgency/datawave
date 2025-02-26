package datawave.microservice.query.messaging;

/**
 * This is the interface for a query results manager which handles sending and listening for query task notifications
 */
public interface QueryResultsManager {
    
    /**
     * Create a listener for a specified listener id. Calling close on the listener will destroy it.
     *
     * @param listenerId
     *            The listener id
     * @param queryId
     *            The query ID to listen to
     * @return a query queue listener
     */
    QueryResultsListener createListener(String listenerId, String queryId);
    
    /**
     * Create a publisher for a specific query ID.
     *
     * @param queryId
     *            The query ID to publish to
     * @return a query results publisher
     */
    QueryResultsPublisher createPublisher(String queryId);
    
    /**
     * Delete a queue for a query
     * 
     * @param queryId
     *            the query ID
     */
    void deleteQuery(String queryId);
    
    /**
     * Empty a queue for a query
     * 
     * @param queryId
     *            the query ID
     */
    void emptyQuery(String queryId);
    
    /**
     * Get the number of results left to be consumed for a query
     *
     * @param queryId
     *            The query Id
     * @return the number of remaining results
     */
    int getNumResultsRemaining(String queryId);
}
