package datawave.microservice.common.storage.queue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import datawave.microservice.common.storage.QueryPool;
import datawave.microservice.common.storage.QueryQueueListener;
import datawave.microservice.common.storage.QueryQueueManager;
import datawave.microservice.common.storage.QueryTaskNotification;
import org.apache.log4j.Logger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;

import static datawave.microservice.common.storage.queue.LocalQueryQueueManager.LOCAL;

@Component
@ConditionalOnProperty(name = "query.storage.backend", havingValue = LOCAL, matchIfMissing = true)
public class LocalQueryQueueManager implements QueryQueueManager {
    private static final Logger log = Logger.getLogger(QueryQueueManager.class);
    
    public static final String LOCAL = "local";
    
    public static final String MESSAGE_KEY = "messageKey";
    public static final String MESSAGE_ID = "messageId";
    
    private Map<String,Queue<Message<byte[]>>> queues = Collections.synchronizedMap(new HashMap<>());
    private Map<String,Set<String>> listenerToQueue = Collections.synchronizedMap(new HashMap<>());
    
    /**
     * Create a listener
     * 
     * @param listenerId
     *            The listener ID
     * @param queueName
     *            The queue name
     * @return a local queue listener
     */
    public QueryQueueListener createListener(String listenerId, String queueName) {
        LocalQueueListener listener = new LocalQueueListener(listenerId);
        synchronized (listenerToQueue) {
            listenerToQueue.put(listener.getListenerId(), Collections.synchronizedSet(new HashSet<>()));
            listenerToQueue.get(listenerId).add(queueName);
        }
        return listener;
    }
    
    private void ensureQueueCreated(String name) {
        synchronized (queues) {
            if (!queues.containsKey(name)) {
                queues.put(name, new ArrayBlockingQueue<>(10));
            }
        }
    }
    
    private void deleteQueue(String name) {
        synchronized (listenerToQueue) {
            for (Set<String> queues : listenerToQueue.values()) {
                queues.remove(name);
            }
        }
        queues.remove(name);
    }
    
    private void emptyQueue(String name) {
        synchronized (queues) {
            Queue queue = queues.get(name);
            if (queue != null) {
                queue.clear();
            }
        }
    }
    
    private void sendMessage(String name, Message<byte[]> message) {
        ensureQueueCreated(name);
        synchronized (queues) {
            Queue queue = queues.get(name);
            if (queue != null) {
                queue.add(message);
            }
        }
    }
    
    /**
     * Ensure a queue is created for a given pool
     *
     * @param queryPool
     *            the query pool
     */
    @Override
    public void ensureQueueCreated(QueryPool queryPool) {
        ensureQueueCreated(queryPool.getName());
    }
    
    /**
     * A mechanism to delete a queue for a pool.
     *
     * @param queryPool
     *            the query pool
     */
    @Override
    public void deleteQueue(QueryPool queryPool) {
        deleteQueue(queryPool.getName());
    }
    
    /**
     * A mechanism to empty a queues messages for a pool
     *
     * @param queryPool
     *            the query pool
     */
    @Override
    public void emptyQueue(QueryPool queryPool) {
        emptyQueue(queryPool.getName());
    }
    
    /**
     * Passes task notifications to the messaging infrastructure.
     *
     * @param taskNotification
     *            The task notification to be sent
     */
    @Override
    public void sendMessage(QueryTaskNotification taskNotification) {
        Message<byte[]> message = null;
        try {
            MessageHeaderAccessor header = new MessageHeaderAccessor();
            header.setHeader(MESSAGE_KEY, taskNotification.getTaskKey().toKey());
            message = MessageBuilder.createMessage(new ObjectMapper().writeValueAsBytes(taskNotification), header.toMessageHeaders());
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialize a QueryTaskNotification", e);
        }
        sendMessage(taskNotification.getTaskKey().getQueryPool().getName(), message);
    }
    
    /**
     * Ensure a queue is created for a query results queue. This will create an exchange, a queue, and a binding between them for the results queue.
     *
     * @param queryId
     *            the query ID
     */
    @Override
    public void ensureQueueCreated(UUID queryId) {
        ensureQueueCreated(queryId.toString());
    }
    
    /**
     * Delete a queue for a query
     *
     * @param queryId
     *            the query ID
     */
    @Override
    public void deleteQueue(UUID queryId) {
        deleteQueue(queryId.toString());
    }
    
    /**
     * Empty a queue for a query
     *
     * @param queryId
     *            the query ID
     */
    @Override
    public void emptyQueue(UUID queryId) {
        emptyQueue(queryId.toString());
    }
    
    /**
     * This will send a result message. This will call ensureQueueCreated before sending the message.
     * <p>
     *
     * @param queryId
     *            the query ID
     * @param resultId
     *            a unique id for the result
     * @param result
     */
    @Override
    public void sendMessage(UUID queryId, String resultId, Object result) {
        Message<byte[]> message = null;
        try {
            MessageHeaderAccessor header = new MessageHeaderAccessor();
            header.setHeader(MESSAGE_KEY, queryId);
            header.setHeader(MESSAGE_ID, resultId);
            message = MessageBuilder.createMessage(new ObjectMapper().writeValueAsBytes(result), header.toMessageHeaders());
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialize a QueryTaskNotification", e);
        }
        sendMessage(queryId.toString(), message);
    }
    
    /**
     * A listener for local queues
     */
    public class LocalQueueListener implements Runnable, QueryQueueListener {
        private static final long WAIT_MS_DEFAULT = 100;
        
        private Queue<Message<byte[]>> messageQueue = new ArrayBlockingQueue<>(100);
        private final String listenerId;
        private Thread thread;
        
        public LocalQueueListener(String listenerId) {
            this.listenerId = listenerId;
            this.thread = new Thread(this);
            this.thread.start();
        }
        
        @Override
        public String getListenerId() {
            return listenerId;
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
            listenerToQueue.remove(listenerId);
        }
        
        public void run() {
            while (thread != null) {
                if (listenerToQueue.containsKey(listenerId)) {
                    for (String queue : listenerToQueue.get(listenerId)) {
                        if (queues.containsKey(queue)) {
                            Message message = queues.get(queue).poll();
                            if (message != null) {
                                message(message);
                            }
                        }
                    }
                }
            }
        }
        
        public void message(Message<byte[]> message) {
            messageQueue.add(message);
        }
        
        @Override
        public QueryTaskNotification receiveTaskNotification() throws IOException {
            return receiveTaskNotification(WAIT_MS_DEFAULT);
        }
        
        @Override
        public QueryTaskNotification receiveTaskNotification(long waitMs) throws IOException {
            Message<byte[]> message = receive(waitMs);
            if (message != null) {
                return new ObjectMapper().readerFor(QueryTaskNotification.class).readValue(message.getPayload());
            }
            return null;
        }
        
        @Override
        public Message<byte[]> receive() {
            return receive(WAIT_MS_DEFAULT);
        }
        
        @Override
        public Message<byte[]> receive(long waitMs) {
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
