package datawave.microservice.query.storage.queue;

import datawave.microservice.query.storage.QueryQueueListener;
import datawave.microservice.query.storage.QueryQueueManager;
import datawave.microservice.query.storage.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import static datawave.microservice.query.storage.queue.TestQueryQueueManager.TEST;

@Component
@ConditionalOnProperty(name = "query.storage.backend", havingValue = TEST, matchIfMissing = true)
@ConditionalOnMissingBean(type = "QueryQueueManager")
public class TestQueryQueueManager implements QueryQueueManager {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    public static final String TEST = "test";
    
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
     * @return a test queue listener
     */
    public QueryQueueListener createListener(String listenerId, String queueName) {
        TestQueueListener listener = new TestQueueListener(listenerId);
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
    
    private int getQueueSize(String name) {
        synchronized (queues) {
            Queue queue = queues.get(name);
            if (queue != null) {
                return queue.size();
            }
        }
        return 0;
    }
    
    private void sendMessage(String name, Message<Result> message) {
        ensureQueueCreated(name);
        synchronized (queues) {
            Queue queue = queues.get(name);
            if (queue != null) {
                queue.add(message);
            }
        }
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
     * Get the queue size
     *
     * @param queryId
     *            The query Id
     * @return the number of elements
     */
    @Override
    public int getQueueSize(UUID queryId) {
        return getQueueSize(queryId.toString());
    }
    
    /**
     * This will send a result message. This will call ensureQueueCreated before sending the message.
     * <p>
     *
     * @param queryId
     *            the query ID
     * @param result
     */
    @Override
    public void sendMessage(UUID queryId, Result result) {
        Message<Result> message = null;
        MessageHeaderAccessor header = new MessageHeaderAccessor();
        header.setHeader(MESSAGE_KEY, queryId);
        message = MessageBuilder.createMessage(result, header.toMessageHeaders());
        sendMessage(queryId.toString(), message);
    }
    
    /**
     * A listener for test queues
     */
    public class TestQueueListener implements Runnable, QueryQueueListener {
        private static final long WAIT_MS_DEFAULT = 100;
        
        private ArrayBlockingQueue<Message<Result>> messageQueue = new ArrayBlockingQueue<>(100);
        private final String listenerId;
        private Thread thread;
        
        public TestQueueListener(String listenerId) {
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
        
        public void message(Message<Result> message) {
            messageQueue.add(message);
        }
        
        @Override
        public Message<Result> receive() {
            return receive(WAIT_MS_DEFAULT);
        }
        
        @Override
        public Message<Result> receive(long waitMs) {
            Message<Result> result = null;
            try {
                result = messageQueue.poll(waitMs, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                if (log.isTraceEnabled()) {
                    log.trace("Interrupted while waiting for query results");
                }
            }
            return result;
        }
    }
}
