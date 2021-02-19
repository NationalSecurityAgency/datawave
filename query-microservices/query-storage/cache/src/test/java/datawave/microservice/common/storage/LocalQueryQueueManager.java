package datawave.microservice.common.storage;

import org.apache.log4j.Logger;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

@Service
@Primary
public class LocalQueryQueueManager implements QueryQueueManager {
    private static final Logger log = Logger.getLogger(QueryQueueManager.class);
    
    private Map<QueryPool,Queue<QueryTaskNotification>> queues = new HashMap<>();
    private Map<String,Set<QueryPool>> listenerToPools = new HashMap<>();
    
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
        
        Queue<QueryTaskNotification> queue = queues.get(pool);
        queue.add(taskNotification);
    }
    
    public LocalQueueListener createListener(String listenerId) {
        LocalQueueListener listener = new LocalQueueListener(listenerId);
        listenerToPools.put(listener.getListenerId(), new HashSet<>());
        listener.start();
        return listener;
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
        QueryPool pool = new QueryPool(queueName);
        listenerToPools.get(listenerId).add(pool);
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
        QueryPool pool = new QueryPool(queueName);
        listenerToPools.get(listenerId).remove(pool);
    }
    
    /**
     * Ensure a queue is created for a given pool
     *
     * @param queryPool
     */
    @Override
    public void ensureQueueCreated(QueryPool queryPool) {
        if (!queues.containsKey(queryPool)) {
            queues.put(queryPool, new ArrayBlockingQueue<>(10));
        }
    }
    
    /**
     * A listener for local queues
     */
    public class LocalQueueListener implements Runnable {
        private static final long WAIT_MS_DEFAULT = 100;
        
        private Queue<QueryTaskNotification> notificationQueue = new ArrayBlockingQueue<>(10);
        private final String listenerId;
        private Thread thread = null;
        
        public LocalQueueListener(String listenerId) {
            this.listenerId = listenerId;
            new Thread(this).start();
        }
        
        public String getListenerId() {
            return listenerId;
        }
        
        public void start() {
            if (thread == null) {
                thread = new Thread(this);
                thread.start();
            }
        }
        
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
                for (QueryPool pool : listenerToPools.get(listenerId)) {
                    QueryTaskNotification notification = queues.get(pool).poll();
                    if (notification != null) {
                        message(notification);
                    }
                }
            }
        }
        
        public void message(QueryTaskNotification notification) {
            notificationQueue.add(notification);
        }
        
        public QueryTaskNotification receive() {
            return receive(WAIT_MS_DEFAULT);
        }
        
        public QueryTaskNotification receive(long waitMs) {
            long start = System.currentTimeMillis();
            int count = 0;
            while (notificationQueue.isEmpty() && ((System.currentTimeMillis() - start) < waitMs)) {
                count++;
                try {
                    Thread.sleep(1L);
                } catch (InterruptedException e) {
                    break;
                }
            }
            if (log.isTraceEnabled()) {
                log.trace("Cycled " + count + " rounds looking for notification");
            }
            if (notificationQueue.isEmpty()) {
                return null;
            } else {
                return notificationQueue.remove();
            }
        }
    }
}
