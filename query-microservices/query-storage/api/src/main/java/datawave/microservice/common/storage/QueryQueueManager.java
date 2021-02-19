package datawave.microservice.common.storage;

/**
 * This is the interface for a query queue manager which handles sending and listening for query task notificatgions
 */
public interface QueryQueueManager {

    /**
     * Ensure a queue is created.  This will create an exchange, a queue, and a binding between them
     * for the query pool.
     *
     * @param queryPool
     *            the query poll
     */
    void ensureQueueCreated(QueryPool queryPool);

    /**
     * This will send a query task notification message.  This will call ensureQueueCreated before sending the message.
     *
     * @param taskNotification
     *           the task notification to send
     */
    void sendMessage(QueryTaskNotification taskNotification);

    /**
     * Add a queue to a listener specified by the listener id.
     * It is presumed that a listener was created and registered previously.
     * For example by adding the notation @RabbitListener(id = LISTENER_ID, autoStartup = "true")
     * to a method.
     *
     * @param listenerId
     *          the listener id
     * @param queueName
     *          the queue name
     */
    void addQueueToListener(String listenerId, String queueName);

    /**
     * Remove a queue from a listener specified by the listener id
     *
     * @param listenerId
     *           the listener id
     * @param queueName
     *           the queue name
     */
    void removeQueueFromListener(String listenerId, String queueName);
}
