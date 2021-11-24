package datawave.microservice.query.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static datawave.microservice.query.messaging.TestQueryResultsManager.TEST;

@Component
@ConditionalOnProperty(name = "query.messaging.backend", havingValue = TEST, matchIfMissing = true)
public class TestQueryResultsManager implements QueryResultsManager {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    public static final String TEST = "test";
    
    private final Map<String,Queue<Result>> queues = Collections.synchronizedMap(new HashMap<>());
    private final Map<String,Set<String>> listenerToQueue = Collections.synchronizedMap(new HashMap<>());
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
            listenerToQueue.put(listener.getListenerId(), Collections.synchronizedSet(new HashSet<>()));
            listenerToQueue.get(listenerId).add(queueName);
        }
        listeners.add(listener);
        return listener;
    }
    
    @Override
    public QueryResultsPublisher createPublisher(String queryId) {
        return (result, interval, timeUnit) -> {
            sendMessage(queryId, result);
            return true;
        };
    }
    
    public boolean queueExists(String name) {
        return queues.containsKey(name);
    }
    
    @Override
    public void deleteQuery(String name) {
        synchronized (listenerToQueue) {
            for (Set<String> queues : listenerToQueue.values()) {
                queues.remove(name);
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
                listenerToQueue.remove(listenerId);
            }
        }
        
        public void run() {
            while (thread != null) {
                if (listenerToQueue.containsKey(listenerId)) {
                    for (String queue : listenerToQueue.get(listenerId)) {
                        if (queues.containsKey(queue)) {
                            Result result = queues.get(queue).poll();
                            if (result != null) {
                                message(result);
                            }
                        }
                    }
                }
            }
        }
        
        public void message(Result result) {
            try {
                result.setAcknowledgementCallback(status -> log.debug("result acknowledged"));
                if (!resultQueue.offer(result, 10, TimeUnit.SECONDS)) {
                    log.error("Messages are not being pulled off the queue in time.  " + result.getId() + " is being dropped!");
                }
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }
        
        @Override
        public boolean hasResults() {
            return !resultQueue.isEmpty();
        }
        
        @Override
        public Result receive() {
            return receive(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        }
        
        @Override
        public Result receive(long interval, TimeUnit timeUnit) {
            Result result = null;
            try {
                result = resultQueue.poll(interval, timeUnit);
            } catch (InterruptedException e) {
                if (log.isTraceEnabled()) {
                    log.trace("Interrupted while waiting for query results");
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
