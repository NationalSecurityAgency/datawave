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
     * @return a query queue listener
     */
    QueryQueueListener createListener(String listenerId);
    
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
    
    /**
     * Add a queue to a listener specified by the listener id. It is presumed that a listener was created and registered previously. For example by adding the
     * notation @RabbitListener(id = LISTENER_ID, autoStartup = "true") to a method.
     *
     * @param listenerId
     *            the listener id
     * @param queueName
     *            the queue name
     */
    void addQueueToListener(String listenerId, String queueName);
    
    /**
     * Remove a queue from a listener specified by the listener id
     *
     * @param listenerId
     *            the listener id
     * @param queueName
     *            the queue name
     */
    void removeQueueFromListener(String listenerId, String queueName);
}
