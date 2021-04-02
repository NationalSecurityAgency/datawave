package datawave.microservice.common.storage;

import java.util.UUID;

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
     * Ensure a queue is created for a pool. This will create an exchange, a queue, and a binding between them for the query pool.
     *
     * @param queryPool
     *            the query pool
     */
    void ensureQueueCreated(QueryPool queryPool);
    
    /**
     * A mechanism to delete a queue for a pool.
     *
     * @param queryPool
     *            the query pool
     */
    void deleteQueue(QueryPool queryPool);
    
    /**
     * A mechanism to empty a queues messages for a pool
     * 
     * @param queryPool
     *            the query pool
     */
    void emptyQueue(QueryPool queryPool);
    
    /**
     * This will send a query task notification message. This will call ensureQueueCreated before sending the message.
     *
     * @param taskNotification
     *            the task notification to send
     */
    void sendMessage(QueryTaskNotification taskNotification);
    
    /**
     * Ensure a queue is created for a query results queue. This will create an exchange, a queue, and a binding between them for the results queue.
     *
     * @param queryId
     *            the query ID
     */
    void ensureQueueCreated(UUID queryId);
    
    /**
     * Delete a queue for a query
     * 
     * @param queryId
     *            the query ID
     */
    void deleteQueue(UUID queryId);
    
    /**
     * Empty a queue for a query
     * 
     * @param queryId
     *            the query ID
     */
    void emptyQueue(UUID queryId);
    
    /**
     * This will send a result message. This will call ensureQueueCreated before sending the message.
     *
     * TODO Should the result be more strongly typed?
     *
     * @param queryId
     *            the query ID
     * @param resultId
     *            a unique id for the result
     * @param result
     *            the result
     */
    void sendMessage(UUID queryId, String resultId, Object result);
}
