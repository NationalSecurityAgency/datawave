package datawave.microservice.common.storage.queue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import datawave.microservice.common.storage.QueryPool;
import datawave.microservice.common.storage.QueryQueueListener;
import datawave.microservice.common.storage.QueryQueueManager;
import datawave.microservice.common.storage.QueryTaskNotification;
import org.apache.log4j.Logger;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePropertiesBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;

public class LocalQueryQueueManager implements QueryQueueManager {
    private static final Logger log = Logger.getLogger(QueryQueueManager.class);
    
    private Map<String,Queue<Message>> queues = Collections.synchronizedMap(new HashMap<>());
    private Map<String,Set<String>> listenerToQueue = Collections.synchronizedMap(new HashMap<>());

    /**
     * Create a listener
     * @param listenerId
     * @return a local queue listener
     */
    public QueryQueueListener createListener(String listenerId) {
        LocalQueueListener listener = new LocalQueueListener(listenerId);
        listenerToQueue.put(listener.getListenerId(), Collections.synchronizedSet(new HashSet<>()));
        listener.start();
        return listener;
    }

    /**
     * Ensure a queue is created for a given pool
     *
     * @param queryPool
     */
    @Override
    public void ensureQueueCreated(QueryPool queryPool) {
        if (!queues.containsKey(queryPool.getName())) {
            queues.put(queryPool.getName(), new ArrayBlockingQueue<>(10));
        }
    }

    /**
     * Passes task notifications to the messaging infrastructure.
     *
     * @param taskNotification
     *            The task notification to be sent
     */
    @Override
    public void sendMessage(QueryTaskNotification taskNotification) {
        QueryPool pool = taskNotification.getTaskKey().getQueryPool();
        
        ensureQueueCreated(pool);
        
        Queue<Message> queue = queues.get(pool.getName());
        Message message = null;
        try {
            message = new Message(new ObjectMapper().writeValueAsBytes(taskNotification),
                    MessagePropertiesBuilder.newInstance().setMessageId(taskNotification.getTaskKey().toKey()).build());
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialize a QueryTaskNotification", e);
        }
        queue.add(message);
    }

    /**
     * Ensure a queue is created for a query results queue.  This will create an exchange, a queue, and a binding between them
     * for the results queue.
     *
     * @param queryId the query ID
     */
    @Override
    public void ensureQueueCreated(UUID queryId) {
        if (!queues.containsKey(queryId.toString())) {
            queues.put(queryId.toString(), new ArrayBlockingQueue<>(10));
        }
    }

    /**
     * This will send a result message.  This will call ensureQueueCreated before sending the message.
     * <p>
     * TODO Should the result be more strongly typed?
     *
     * @param queryId  the query ID
     * @param resultId a unique id for the result
     * @param result
     */
    @Override
    public void sendMessage(UUID queryId, String resultId, Object result) {
        ensureQueueCreated(queryId);

        Queue<Message> queue = queues.get(queryId.toString());
        Message message = null;
        try {
            message = new Message(new ObjectMapper().writeValueAsBytes(result),
                    MessagePropertiesBuilder.newInstance().setMessageId(resultId).build());
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialize a QueryTaskNotification", e);
        }
        queue.add(message);
    }

    /**
     * Add a queue to a listener
     *
     * @param listenerId
     * @param queueName
     */
    @Override
    public void addQueueToListener(String listenerId, String queueName) {
        if (log.isDebugEnabled()) {
            log.debug("adding queue : " + queueName + " to listener with id : " + listenerId);
        }
        listenerToQueue.get(listenerId).add(queueName);
    }
    
    /**
     * Remove a queue from a listener
     *
     * @param listenerId
     * @param queueName
     */
    @Override
    public void removeQueueFromListener(String listenerId, String queueName) {
        if (log.isInfoEnabled()) {
            log.info("removing queue : " + queueName + " from listener : " + listenerId);
        }
        listenerToQueue.get(listenerId).remove(queueName);
    }
    
    /**
     * A listener for local queues
     */
    public class LocalQueueListener implements Runnable, QueryQueueListener {
        private static final long WAIT_MS_DEFAULT = 100;
        
        private Queue<Message> messageQueue = new ArrayBlockingQueue<>(100);
        private final String listenerId;
        private Thread thread = null;
        
        public LocalQueueListener(String listenerId) {
            this.listenerId = listenerId;
            new Thread(this).start();
        }

        @Override
        public String getListenerId() {
            return listenerId;
        }

        @Override
        public void start() {
            if (thread == null) {
                thread = new Thread(this);
                thread.start();
            }
        }

        @Override
        public void stop() {
            Thread thread = this.thread;
            this.thread = null;
            thread.interrupt();
            while (thread.isAlive()) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
        
        public void run() {
            while (thread != null) {
                for (String queue : listenerToQueue.get(listenerId)) {
                    Message message = queues.get(queue).poll();
                    if (message != null) {
                        message(message);
                    }
                }
            }
        }
        
        public void message(Message message) {
            messageQueue.add(message);
        }

        @Override
        public QueryTaskNotification receiveTaskNotification() throws IOException {
            return receiveTaskNotification(WAIT_MS_DEFAULT);
        }

        @Override
        public QueryTaskNotification receiveTaskNotification(long waitMs) throws IOException {
            Message message = receive(waitMs);
            if (message != null) {
                return new ObjectMapper().readerFor(QueryTaskNotification.class).readValue(message.getBody());
            }
            return null;
        }

        @Override
        public Message receive() {
            return receive(WAIT_MS_DEFAULT);
        }

        @Override
        public Message receive(long waitMs) {
            long start = System.currentTimeMillis();
            int count = 0;
            while (messageQueue.isEmpty() && ((System.currentTimeMillis() - start) < waitMs)) {
                count++;
                try {
                    Thread.sleep(1L);
                } catch (InterruptedException e) {
                    break;
                }
            }
            if (log.isTraceEnabled()) {
                log.trace("Cycled " + count + " rounds looking for message");
            }
            if (messageQueue.isEmpty()) {
                return null;
            } else {
                return messageQueue.remove();
            }
        }
    }
}
