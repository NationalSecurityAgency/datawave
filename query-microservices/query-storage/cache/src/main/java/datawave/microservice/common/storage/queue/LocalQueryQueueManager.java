package datawave.microservice.common.storage.queue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import datawave.microservice.common.storage.QueryPool;
import datawave.microservice.common.storage.QueryQueueListener;
import datawave.microservice.common.storage.QueryQueueManager;
import datawave.microservice.common.storage.QueryTaskNotification;
import org.apache.log4j.Logger;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.messaging.support.MessageHeaderAccessor;

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
    
    public static final String MESSAGE_KEY = "messageKey";
    public static final String MESSAGE_ID = "messageId";
    
    private Map<String,Queue<Message<byte[]>>> queues = Collections.synchronizedMap(new HashMap<>());
    private Map<String,Set<String>> listenerToQueue = Collections.synchronizedMap(new HashMap<>());
    
    /**
     * Create a listener
     * 
     * @param listenerId
     * @param queueName
     * @return a local queue listener
     */
    public QueryQueueListener createListener(String listenerId, String queueName) {
        LocalQueueListener listener = new LocalQueueListener(listenerId);
        listenerToQueue.put(listener.getListenerId(), Collections.synchronizedSet(new HashSet<>()));
        listenerToQueue.get(listenerId).add(queueName);
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
        
        Queue<Message<byte[]>> queue = queues.get(pool.getName());
        Message<byte[]> message = null;
        try {
            MessageHeaderAccessor header = new MessageHeaderAccessor();
            header.setHeader(MESSAGE_KEY, taskNotification.getTaskKey().toKey());
            message = MessageBuilder.createMessage(new ObjectMapper().writeValueAsBytes(taskNotification), header.toMessageHeaders());
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialize a QueryTaskNotification", e);
        }
        queue.add(message);
    }
    
    /**
     * Ensure a queue is created for a query results queue. This will create an exchange, a queue, and a binding between them for the results queue.
     *
     * @param queryId
     *            the query ID
     */
    @Override
    public void ensureQueueCreated(UUID queryId) {
        if (!queues.containsKey(queryId.toString())) {
            queues.put(queryId.toString(), new ArrayBlockingQueue<>(10));
        }
    }
    
    /**
     * This will send a result message. This will call ensureQueueCreated before sending the message.
     * <p>
     * TODO Should the result be more strongly typed?
     *
     * @param queryId
     *            the query ID
     * @param resultId
     *            a unique id for the result
     * @param result
     */
    @Override
    public void sendMessage(UUID queryId, String resultId, Object result) {
        ensureQueueCreated(queryId);
        
        Queue<Message<byte[]>> queue = queues.get(queryId.toString());
        Message<byte[]> message = null;
        try {
            MessageHeaderAccessor header = new MessageHeaderAccessor();
            header.setHeader(MESSAGE_KEY, queryId);
            header.setHeader(MESSAGE_ID, resultId);
            message = MessageBuilder.createMessage(new ObjectMapper().writeValueAsBytes(result), header.toMessageHeaders());
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialize a QueryTaskNotification", e);
        }
        queue.add(message);
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
