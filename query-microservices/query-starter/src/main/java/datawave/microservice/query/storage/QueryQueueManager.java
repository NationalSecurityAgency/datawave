package datawave.microservice.query.storage;

/**
 * This is the interface for a query queue manager which handles sending and listening for query task notificatgions
 */
public interface QueryQueueManager {
    
    /**
     * Create a listener for a specified listener id. Calling stop on the listener will destroy it.
     *
     * @param listenerId
     *            The listener id
     * @param queueName
     *            The queue to listen to
     * @return a query queue listener
     */
    QueryQueueListener createListener(String listenerId, String queueName);
    
    /**
     * Ensure a queue is created for a query results queue. This will create an exchange, a queue, and a binding between them for the results queue.
     *
     * @param queryId
     *            the query ID
     */
    void ensureQueueCreated(String queryId);
    
    /**
     * Delete a queue for a query
     * 
     * @param queryId
     *            the query ID
     */
    void deleteQueue(String queryId);
    
    /**
     * Empty a queue for a query
     * 
     * @param queryId
     *            the query ID
     */
    void emptyQueue(String queryId);
    
    /**
     * Get the queue size
     *
     * @param queryId
     *            The query Id
     * @return the number of elements.
     */
    int getQueueSize(String queryId);
    
    /**
     * This will send a result message. This will call ensureQueueCreated before sending the message.
     *
     * @param queryId
     *            the query ID
     * @param result
     *            the result
     */
    void sendMessage(String queryId, Result result);
    
}
