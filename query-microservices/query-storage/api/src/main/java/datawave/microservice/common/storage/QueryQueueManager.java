package datawave.microservice.common.storage;

import java.util.UUID;

/**
 * This is the interface for a query queue manager which handles sending and listening for query task notificatgions
 */
public interface QueryQueueManager {
    
    /**
     * Create a listener for a specified listener id
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
     *            the query poll
     */
    void ensureQueueCreated(QueryPool queryPool);
    
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
