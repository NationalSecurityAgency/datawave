package datawave.microservice.common.storage;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.spring.cache.HazelcastCacheManager;
import datawave.microservice.common.storage.queue.LocalQueryQueueManager;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.QueryParametersImpl;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.SimpleRoutingConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles({"QueryStorageCacheTest", "QueryStorageConfig", "sync-enabled", "send-notifications", "use-rabbitmq"})
@EnableRabbit
public class QueryStorageCacheTest {
    private static final Logger log = Logger.getLogger(QueryStorageCacheTest.class);
    public static final String TEST_POOL = "testPool";
    
    @Autowired
    private QueryCache queryCache;
    
    @Autowired
    private QueryStorageCache storageService;
    
    @Autowired
    private QueryQueueManager queueManager;
    
    private QueryQueueListener messageConsumer;
    
    private static final String LISTENER_ID = "QueryStorageCacheTestListener";
    
    @Before
    public void before() throws IOException {
        // ensure our pool is created so we can start listening to it
        queueManager.ensureQueueCreated(new QueryPool(TEST_POOL));
        messageConsumer = queueManager.createListener(LISTENER_ID);
        queueManager.addQueueToListener(LISTENER_ID, TEST_POOL);
        cleanupData();
    }
    
    @After
    public void cleanup() throws IOException {
        messageConsumer.stop();
        cleanupData();
    }
    
    public void cleanupData() throws IOException {
        storageService.clear();
        clearQueue();
    }
    
    void clearQueue() throws IOException {
        Object task;
        task = messageConsumer.receiveTaskNotification();
        do {
            task = messageConsumer.receiveTaskNotification(0L);
        } while (task != null);
    }
    
    @Test
    public void testStoreQuery() throws ParseException, InterruptedException, IOException {
        // ensure the message queue is empty
        assertNull(messageConsumer.receiveTaskNotification(0));
        
        Query query = new QueryImpl();
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        QueryPool queryPool = new QueryPool(TEST_POOL);
        TaskKey key = storageService.storeQuery(queryPool, query);
        assertNotNull(key);
        
        // ensure we got a task notification
        QueryTaskNotification notification = messageConsumer.receiveTaskNotification();
        assertNotNull(notification);
        assertEquals(key, notification.getTaskKey());
        
        // ensure the message queue is empty again
        assertNull(messageConsumer.receiveTaskNotification(0));
        
        QueryTask task = storageService.getTask(key);
        assertQueryTask(key, QueryTask.QUERY_ACTION.CREATE, query, task);
        
        List<QueryTask> tasks = storageService.getTasks(key.getQueryId());
        assertNotNull(tasks);
        assertEquals(1, tasks.size());
        assertQueryTask(key, QueryTask.QUERY_ACTION.CREATE, query, tasks.get(0));
        
        tasks = storageService.getTasks(queryPool);
        assertNotNull(tasks);
        assertEquals(1, tasks.size());
        assertQueryTask(key, QueryTask.QUERY_ACTION.CREATE, query, tasks.get(0));
        
        List<QueryState> queries = queryCache.getQueries();
        assertNotNull(queries);
        assertEquals(1, queries.size());
        assertQueryCreate(key.getQueryId(), queryPool, queries.get(0));
        
        queries = queryCache.getQueries(queryPool);
        assertNotNull(queries);
        assertEquals(1, queries.size());
        assertQueryCreate(key.getQueryId(), queryPool, queries.get(0));
        
        List<TaskDescription> taskDescs = queryCache.getTaskDescriptions(key.getQueryId());
        assertNotNull(taskDescs);
        assertEquals(1, taskDescs.size());
        assertQueryCreate(key.getQueryId(), queryPool, query, taskDescs.get(0));
    }
    
    @Test
    public void testStoreTask() throws ParseException, InterruptedException, IOException {
        // ensure the message queue is empty
        assertNull(messageConsumer.receiveTaskNotification(0));
        
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
        QueryTaskNotification notification = messageConsumer.receiveTaskNotification();
        assertNotNull(notification);
        assertEquals(key, notification.getTaskKey());
        
        // ensure the message queue is empty again
        assertNull(messageConsumer.receiveTaskNotification(0));
        
        task = storageService.getTask(key);
        assertQueryTask(key, QueryTask.QUERY_ACTION.NEXT, query, task);
    }
    
    @Test
    public void testCheckpointTask() throws InterruptedException, ParseException, IOException {
        // ensure the message queue is empty
        assertNull(messageConsumer.receiveTaskNotification(0));
        
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
        QueryTaskNotification notification = messageConsumer.receiveTaskNotification();
        assertNotNull(notification);
        assertNull(messageConsumer.receiveTaskNotification(0));
        
        // now update the task
        Map<String,Object> props = new HashMap<>();
        props.put("checkpoint", Boolean.TRUE);
        checkpoint = new QueryCheckpoint(queryPool, queryId, query.getQueryLogicName(), props);
        storageService.checkpointTask(notification.getTaskKey(), checkpoint);
        
        // ensure we did not get another task notification
        assertNull(messageConsumer.receiveTaskNotification(0));
        
        QueryTask task = storageService.getTask(notification.getTaskKey());
        assertEquals(checkpoint, task.getQueryCheckpoint());
    }
    
    @Test
    public void testGetAndDeleteTask() throws ParseException, InterruptedException, IOException {
        // ensure the message queue is empty
        assertNull(messageConsumer.receiveTaskNotification(0));
        
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
        QueryTaskNotification notification = messageConsumer.receiveTaskNotification();
        assertNotNull(notification);
        assertNull(messageConsumer.receiveTaskNotification(0));
        
        // ensure we can get a task
        QueryTask task = storageService.getTask(notification.getTaskKey());
        assertQueryTask(notification.getTaskKey(), QueryTask.QUERY_ACTION.NEXT, query, task);
        
        // now delete the task
        storageService.deleteTask(notification.getTaskKey());
        
        // ensure we did not get another task notification
        assertNull(messageConsumer.receiveTaskNotification(0));
        
        // ensure there is no more task stored
        task = storageService.getTask(notification.getTaskKey());
        assertNull(task);
    }
    
    @Test
    public void testGetAndDeleteQueryTasks() throws ParseException, InterruptedException, IOException {
        // ensure the message queue is empty
        assertNull(messageConsumer.receiveTaskNotification(0));
        
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
        QueryTaskNotification notification = messageConsumer.receiveTaskNotification();
        assertNotNull(notification);
        assertNull(messageConsumer.receiveTaskNotification(0));
        
        // not get the query tasks
        List<QueryTask> tasks = storageService.getTasks(queryId);
        assertEquals(1, tasks.size());
        QueryTask task = tasks.get(0);
        assertQueryTask(notification.getTaskKey(), QueryTask.QUERY_ACTION.NEXT, query, task);
        
        // now delete the query tasks
        storageService.deleteQuery(queryId);
        
        // ensure we did not get another task notification
        assertNull(messageConsumer.receiveTaskNotification(0));
        
        // make sure it deleted
        tasks = storageService.getTasks(queryId);
        assertEquals(0, tasks.size());
    }
    
    @Test
    public void testGetAndDeleteTypeTasks() throws ParseException, InterruptedException, IOException {
        // ensure the message queue is empty
        assertNull(messageConsumer.receiveTaskNotification(0));
        
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
        QueryTaskNotification notification = messageConsumer.receiveTaskNotification();
        assertNotNull(notification);
        assertNull(messageConsumer.receiveTaskNotification(0));
        
        // not get the query tasks
        List<QueryTask> tasks = storageService.getTasks(queryPool);
        assertEquals(1, tasks.size());
        QueryTask task = tasks.get(0);
        assertQueryTask(notification.getTaskKey(), QueryTask.QUERY_ACTION.NEXT, query, task);
        
        // now delete the query tasks
        storageService.deleteQueryPool(queryPool);
        
        // ensure we did not get another task notification
        assertNull(messageConsumer.receiveTaskNotification(0));
        
        // make sure it deleted
        tasks = storageService.getTasks(queryPool);
        assertEquals(0, tasks.size());
    }
    
    private void assertQueryCreate(UUID queryId, QueryPool queryPool, QueryState state) {
        assertEquals(queryId, state.getQueryId());
        assertEquals(queryPool, state.getQueryPool());
        Map<QueryTask.QUERY_ACTION,Integer> counts = state.getTaskCounts();
        assertEquals(1, counts.size());
        assertTrue(counts.containsKey(QueryTask.QUERY_ACTION.CREATE));
        assertEquals(1, counts.get(QueryTask.QUERY_ACTION.CREATE).intValue());
    }
    
    private void assertQueryCreate(UUID queryId, QueryPool queryPool, Query query, TaskDescription task) throws ParseException {
        assertNotNull(task.getTaskKey());
        assertEquals(queryId, task.getTaskKey().getQueryId());
        assertEquals(queryPool, task.getTaskKey().getQueryPool());
        assertEquals(QueryTask.QUERY_ACTION.CREATE, task.getAction());
        assertEquals(query.getQuery(), task.getParameters().get(QueryImpl.QUERY));
        assertEquals(QueryParametersImpl.formatDate(query.getBeginDate()), task.getParameters().get(QueryImpl.BEGIN_DATE));
        assertEquals(QueryParametersImpl.formatDate(query.getEndDate()), task.getParameters().get(QueryImpl.END_DATE));
    }
    
    private void assertQueryTask(TaskKey key, QueryTask.QUERY_ACTION action, Query query, QueryTask task) throws ParseException {
        assertEquals(key, task.getTaskKey());
        assertEquals(action, task.getAction());
        assertEquals(task.getQueryCheckpoint().getQueryKey(), key);
        assertEquals(query, task.getQueryCheckpoint().getPropertiesAsQuery());
    }
    
    @Configuration
    @Profile("QueryStorageCacheTest")
    @ComponentScan(basePackages = {"datawave.microservice"})
    @EnableCaching
    public static class QueryStorageCacheTestConfiguration {
        @Bean
        @Primary
        public CacheManager cacheManager() {
            return new HazelcastCacheManager(Hazelcast.newHazelcastInstance());
        }
        
        @Bean
        @Primary
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
    
}
