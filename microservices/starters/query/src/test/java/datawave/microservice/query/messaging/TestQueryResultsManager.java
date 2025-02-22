package datawave.microservice.query.messaging;

import static datawave.microservice.query.messaging.TestQueryResultsManager.TEST;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "datawave.query.messaging.backend", havingValue = TEST, matchIfMissing = true)
public class TestQueryResultsManager implements QueryResultsManager {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    public static final String TEST = "test";
    
    private final Map<String,Queue<Result>> queues = Collections.synchronizedMap(new HashMap<>());
    private final Map<String,String> listenerToQueue = Collections.synchronizedMap(new HashMap<>());
    private final List<QueryResultsListener> listeners = new ArrayList<>();
    
    /**
     * Create a listener
     * 
     * @param listenerId
     *            The listener ID
     * @param queueName
     *            The queue name
     * @return a test queue listener
     */
    public QueryResultsListener createListener(String listenerId, String queueName) {
        TestListenerQuery listener = new TestListenerQuery(listenerId);
        synchronized (listenerToQueue) {
            listenerToQueue.put(listener.getListenerId(), queueName);
        }
        listeners.add(listener);
        return listener;
    }
    
    @Override
    public QueryResultsPublisher createPublisher(String queryId) {
        return new QueryResultsPublisher() {
            @Override
            public boolean publish(Result result, long interval, TimeUnit timeUnit) {
                sendMessage(queryId, result);
                return true;
            }
            
            @Override
            public void close() throws IOException {
                // do nothing
            }
        };
    }
    
    public boolean queueExists(String name) {
        return queues.containsKey(name);
    }
    
    @Override
    public void deleteQuery(String name) {
        synchronized (listenerToQueue) {
            Set<Map.Entry<String,String>> entriesToRemove = new HashSet<>();
            for (Map.Entry<String,String> entry : listenerToQueue.entrySet()) {
                if (entry.getValue().equals(name)) {
                    entriesToRemove.add(entry);
                }
            }
            for (Map.Entry<String,String> entry : entriesToRemove) {
                listenerToQueue.remove(entry.getKey(), entry.getValue());
            }
        }
        queues.remove(name);
    }
    
    @Override
    public void emptyQuery(String name) {
        synchronized (queues) {
            Queue<Result> queue = queues.get(name);
            if (queue != null) {
                queue.clear();
            }
        }
    }
    
    @Override
    public int getNumResultsRemaining(String name) {
        synchronized (queues) {
            Queue<Result> queue = queues.get(name);
            if (queue != null) {
                return queue.size();
            }
        }
        return 0;
    }
    
    /**
     * This will send a result message. This will call ensureQueueCreated before sending the message.
     * <p>
     *
     * @param queryId
     *            the query ID
     * @param result
     *            the result to send
     */
    private void sendMessage(String queryId, Result result) {
        synchronized (queues) {
            Queue<Result> queue = queues.get(queryId);
            if (queue == null) {
                queue = new LinkedBlockingQueue<>();
                queues.put(queryId, queue);
            }
            queue.add(result);
        }
    }
    
    /**
     * A listener for test queues
     */
    public class TestListenerQuery implements Runnable, QueryResultsListener {
        private final LinkedBlockingQueue<Result> resultQueue = new LinkedBlockingQueue<>();
        private final String listenerId;
        private Thread thread;
        private CountDownLatch stoppedLatch = new CountDownLatch(1);
        private CountDownLatch closedLatch = new CountDownLatch(1);
        
        public TestListenerQuery(String listenerId) {
            this.listenerId = listenerId;
            this.thread = new Thread(this);
            this.thread.start();
        }
        
        @Override
        public String getListenerId() {
            return listenerId;
        }
        
        @Override
        public void close() {
            if (this.thread != null) {
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
                
                try {
                    stoppedLatch.await();
                } catch (Exception e) {
                    log.error("Interrupted while waiting for latch");
                }
                
                String queueId = listenerToQueue.get(listenerId);
                if (queueId != null) {
                    Queue<Result> queue = queues.get(queueId);
                    if (queue != null) {
                        queue.addAll(resultQueue);
                    }
                }
                listenerToQueue.remove(listenerId);
                closedLatch.countDown();
            }
        }
        
        public void run() {
            while (thread != null) {
                if (listenerToQueue.containsKey(listenerId)) {
                    String queue = listenerToQueue.get(listenerId);
                    if (queues.containsKey(queue)) {
                        Result result = queues.get(queue).poll();
                        if (result != null) {
                            if (!message(result)) {
                                // if we aren't able to consume the result, put it back
                                queues.get(queue).offer(result);
                            }
                        }
                    }
                }
            }
            stoppedLatch.countDown();
            try {
                closedLatch.await();
            } catch (Exception e) {
                log.error("Interrupted while waiting for latch");
            }
        }
        
        public boolean message(Result result) {
            boolean success = false;
            if (this.thread != null) {
                try {
                    result.setAcknowledgementCallback(status -> log.debug("result acknowledged"));
                    if (!resultQueue.offer(result, 10, TimeUnit.SECONDS)) {
                        log.error("Messages are not being pulled off the queue in time.  " + result.getId() + " is being dropped!");
                    }
                    success = true;
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }
            return success;
        }
        
        @Override
        public boolean hasResults() {
            if (this.thread != null) {
                return !resultQueue.isEmpty();
            } else {
                return false;
            }
        }
        
        @Override
        public Result receive() {
            return receive(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        }
        
        @Override
        public Result receive(long interval, TimeUnit timeUnit) {
            Result result = null;
            if (this.thread != null) {
                try {
                    result = resultQueue.poll(interval, timeUnit);
                } catch (InterruptedException e) {
                    if (log.isTraceEnabled()) {
                        log.trace("Interrupted while waiting for query results");
                    }
                }
            }
            return result;
        }
    }
    
    public void clear() throws IOException {
        queues.clear();
        listenerToQueue.clear();
        for (QueryResultsListener queryResultsListener : listeners) {
            queryResultsListener.close();
        }
        listeners.clear();
    }
}
