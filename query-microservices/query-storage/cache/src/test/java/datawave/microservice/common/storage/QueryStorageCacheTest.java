package datawave.microservice.common.storage;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.spring.cache.HazelcastCacheManager;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.SimpleRoutingConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles({"QueryStorageCacheTest", "QueryStorageConfig", "sync-enabled"})
@EnableRabbit
public class QueryStorageCacheTest {
    private static final Logger log = Logger.getLogger(QueryStorageCacheTest.class);
    public static final String TEST_POOL = "testPool";
    
    @Autowired
    private QueryStorageCache storageService;

    @Autowired
    private QueryQueueManager queueManager;

    @Autowired
    private MessageConsumer messageConsumer;
    
    @Autowired
    ConnectionFactory connectionFactory;

    @Before
    public void before() {
        // create 
        QueryTaskNotification testNotification = new QueryTaskNotification(new TaskKey(UUID.randomUUID(), new QueryPool(TEST_POOL), UUID.randomUUID(), "None"), QueryTask.QUERY_ACTION.TEST);
        queueManager.ensureQueueCreated(testNotification);
        queueManager.addQueueToListener(messageConsumer.getListenerId(), TEST_POOL);
        cleanup();
    }

    @After
    public void cleanup() {
        storageService.clear();
        clearQueue();
    }
    
    void clearQueue() {
        QueryTaskNotification task;
        task = messageConsumer.receive();
        do {
            task = messageConsumer.receive(0L);
        } while (task != null);
    }
    
    @Test
    public void testStoreQuery() throws ParseException, InterruptedException {
        // ensure the message queue is empty
        assertNull(messageConsumer.receive(0));
        
        Query query = new QueryImpl();
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        QueryPool queryPool = new QueryPool(TEST_POOL);
        TaskKey key = storageService.storeQuery(queryPool, query);
        assertNotNull(key);
        
        // ensure we got a task notification
        QueryTaskNotification notification = messageConsumer.receive();
        assertNotNull(notification);
        assertEquals(key, notification.getTaskKey());
        
        // ensure the message queue is empty again
        assertNull(messageConsumer.receive(0));
        
        QueryTask task = storageService.getTask(key);
        assertQueryTask(key, QueryTask.QUERY_ACTION.CREATE, query, task);
    }
    
    @Test
    public void testStoreTask() throws ParseException, InterruptedException {
        // ensure the message queue is empty
        assertNull(messageConsumer.receive(0));
        
        Query query = new QueryImpl();
        query.setQueryLogicName("EventQuery");
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        UUID queryId = UUID.randomUUID();
        QueryPool queryPool = new QueryPool(TEST_POOL);
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryPool, queryId, query.getQueryLogicName(), query);
        QueryTask task = storageService.createTask(QueryTask.QUERY_ACTION.NEXT, checkpoint);
        TaskKey key = task.getTaskKey();
        assertEquals(checkpoint.getQueryKey(), key);
        
        // ensure we got a task notification
        QueryTaskNotification notification = messageConsumer.receive();
        assertNotNull(notification);
        assertEquals(key, notification.getTaskKey());
        
        // ensure the message queue is empty again
        assertNull(messageConsumer.receive(0));
        
        task = storageService.getTask(key);
        assertQueryTask(key, QueryTask.QUERY_ACTION.NEXT, query, task);
    }
    
    @Test
    public void testCheckpointTask() throws InterruptedException, ParseException {
        // ensure the message queue is empty
        assertNull(messageConsumer.receive(0));
        
        Query query = new QueryImpl();
        query.setQueryLogicName("EventQuery");
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        UUID queryId = UUID.randomUUID();
        QueryPool queryPool = new QueryPool(TEST_POOL);
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryPool, queryId, query.getQueryLogicName(), query);
        
        TaskKey key = new TaskKey(UUID.randomUUID(), queryPool, UUID.randomUUID(), query.getQueryLogicName());
        try {
            storageService.checkpointTask(key, checkpoint);
            fail("Expected storage service to fail checkpointing an invalid task ofkey");
        } catch (IllegalArgumentException e) {
            // expected
        }
        
        key = new TaskKey(UUID.randomUUID(), checkpoint.getQueryKey());
        try {
            storageService.checkpointTask(key, checkpoint);
            fail("Expected storage service to fail checkpointing a missing task");
        } catch (NullPointerException e) {
            // expected
        }
        
        storageService.createTask(QueryTask.QUERY_ACTION.NEXT, checkpoint);
        
        // clear out message queue
        QueryTaskNotification notification = messageConsumer.receive();
        assertNotNull(notification);
        assertNull(messageConsumer.receive(0));
        
        // now update the task
        Map<String,Object> props = new HashMap<>();
        props.put("checkpoint", Boolean.TRUE);
        checkpoint = new QueryCheckpoint(queryPool, queryId, query.getQueryLogicName(), props);
        storageService.checkpointTask(notification.getTaskKey(), checkpoint);
        
        // ensure we did not get another task notification
        assertNull(messageConsumer.receive(0));
        
        QueryTask task = storageService.getTask(notification.getTaskKey());
        assertEquals(checkpoint, task.getQueryCheckpoint());
    }
    
    @Test
    public void testGetAndDeleteTask() throws ParseException, InterruptedException {
        // ensure the message queue is empty
        assertNull(messageConsumer.receive(0));
        
        Query query = new QueryImpl();
        query.setQueryLogicName("EventQuery");
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        UUID queryId = UUID.randomUUID();
        QueryPool queryPool = new QueryPool(TEST_POOL);
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryPool, queryId, query.getQueryLogicName(), query);
        
        storageService.createTask(QueryTask.QUERY_ACTION.NEXT, checkpoint);
        
        // clear out message queue
        QueryTaskNotification notification = messageConsumer.receive();
        assertNotNull(notification);
        assertNull(messageConsumer.receive(0));
        
        // ensure we can get a task
        QueryTask task = storageService.getTask(notification.getTaskKey());
        assertQueryTask(notification.getTaskKey(), QueryTask.QUERY_ACTION.NEXT, query, task);
        
        // now delete the task
        storageService.deleteTask(notification.getTaskKey());
        
        // ensure we did not get another task notification
        assertNull(messageConsumer.receive(0));
        
        // ensure there is no more task stored
        task = storageService.getTask(notification.getTaskKey());
        assertNull(task);
    }
    
    @Test
    public void testGetAndDeleteQueryTasks() throws ParseException, InterruptedException {
        // ensure the message queue is empty
        assertNull(messageConsumer.receive(0));
        
        Query query = new QueryImpl();
        query.setQueryLogicName("EventQuery");
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        UUID queryId = UUID.randomUUID();
        QueryPool queryPool = new QueryPool(TEST_POOL);
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryPool, queryId, query.getQueryLogicName(), query);
        
        storageService.createTask(QueryTask.QUERY_ACTION.NEXT, checkpoint);
        
        // clear out message queue
        QueryTaskNotification notification = messageConsumer.receive();
        assertNotNull(notification);
        assertNull(messageConsumer.receive(0));
        
        // not get the query tasks
        List<QueryTask> tasks = storageService.getTasks(queryId);
        assertEquals(1, tasks.size());
        QueryTask task = tasks.get(0);
        assertQueryTask(notification.getTaskKey(), QueryTask.QUERY_ACTION.NEXT, query, task);
        
        // now delete the query tasks
        storageService.deleteQuery(queryId);
        
        // ensure we did not get another task notification
        assertNull(messageConsumer.receive(0));
        
        // make sure it deleted
        tasks = storageService.getTasks(queryId);
        assertEquals(0, tasks.size());
    }
    
    @Test
    public void testGetAndDeleteTypeTasks() throws ParseException, InterruptedException {
        // ensure the message queue is empty
        assertNull(messageConsumer.receive(0));
        
        Query query = new QueryImpl();
        query.setQueryLogicName("EventQuery");
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        UUID queryId = UUID.randomUUID();
        QueryPool queryPool = new QueryPool(TEST_POOL);
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryPool, queryId, query.getQueryLogicName(), query);
        
        storageService.createTask(QueryTask.QUERY_ACTION.NEXT, checkpoint);
        
        // clear out message queue
        QueryTaskNotification notification = messageConsumer.receive();
        assertNotNull(notification);
        assertNull(messageConsumer.receive(0));
        
        // not get the query tasks
        List<QueryTask> tasks = storageService.getTasks(queryPool);
        assertEquals(1, tasks.size());
        QueryTask task = tasks.get(0);
        assertQueryTask(notification.getTaskKey(), QueryTask.QUERY_ACTION.NEXT, query, task);
        
        // now delete the query tasks
        storageService.deleteQueryPool(queryPool);
        
        // ensure we did not get another task notification
        assertNull(messageConsumer.receive(0));
        
        // make sure it deleted
        tasks = storageService.getTasks(queryPool);
        assertEquals(0, tasks.size());
    }
    
    private void assertQueryTask(TaskKey key, QueryTask.QUERY_ACTION action, Query query, QueryTask task) throws ParseException {
        assertEquals(key, task.getTaskKey());
        assertEquals(action, task.getAction());
        assertEquals(task.getQueryCheckpoint().getQueryKey(), key);
        assertEquals(query, task.getQueryCheckpoint().getPropertiesAsQuery());
    }
    
    @Configuration
    @Profile("QueryStorageCacheTest")
    @ComponentScan(basePackages = {"datawave.microservice", "org.springframework.cloud.stream.test"})
    public static class QueryStorageTestConfiguration {
        @Bean
        public CacheManager cacheManager() {
            return new HazelcastCacheManager(Hazelcast.newHazelcastInstance());
        }
        
        @Bean
        public ConnectionFactory connectionFactory() {
            SimpleRoutingConnectionFactory factory = new SimpleRoutingConnectionFactory();
            factory.setDefaultTargetConnectionFactory(new CachingConnectionFactory());
            return factory;
        }

    }

    public static class ExceptionalQueryTaskNotification extends QueryTaskNotification {
        private static final long serialVersionUID = 9177184396078311888L;
        private Exception e;
        public ExceptionalQueryTaskNotification(Exception e) {
            super(null, null);
            this.e = e;
        }

        public Exception getException() {
            return e;
        }
    }

    @Component
    public static class MessageConsumer {
        // using a listener with the same name as the queue to ensure we don't miss messages from that queue
        // (automatically linked up when the message is sent)
        private static final String LISTENER_ID = "QueryStorageCacheTestListener";
        private static final long WAIT_MS_DEFAULT = 100;

        @Autowired
        private QueryQueueManager queueManager;

        private Queue<QueryTaskNotification> notificationQueue = new ArrayBlockingQueue<>(10);

        @RabbitListener(id = LISTENER_ID, autoStartup = "true")
        public void processMessage(QueryTaskNotification notification) {
            notificationQueue.add(notification);
        }

        public String getListenerId() {
            return LISTENER_ID;
        }

        public QueryTaskNotification receive() {
            return receive(WAIT_MS_DEFAULT);
        }

        public QueryTaskNotification receive(long waitMs) {
            queueManager.addQueueToListener(LISTENER_ID, TEST_POOL);
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
