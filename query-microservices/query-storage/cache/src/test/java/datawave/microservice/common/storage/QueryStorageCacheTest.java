package datawave.microservice.common.storage;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.spring.cache.HazelcastCacheManager;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.SimpleRoutingConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles({"QueryStorageCacheTest", "QueryStorageConfig", "sync-disabled"})
@EnableRabbit
public class QueryStorageCacheTest {
    @Autowired
    private QueryStorageCache storageService;
    
    @Autowired
    private AmqpAdmin admin;
    
    @Autowired
    private AmqpTemplate template;
    
    @After
    public void cleanup() throws InterruptedException {
        storageService.clear();
        clearQueue();
    }
    
    void clearQueue() {
        QueryTaskNotification task;
        do {
            task = receive();
        } while (task != null);
    }
    
    QueryTaskNotification receive() {
        return template.receiveAndConvert("default", 10, new ParameterizedTypeReference<QueryTaskNotification>() {});
    }
    
    @Test
    public void testStoreQuery() throws ParseException, InterruptedException {
        // ensure the message queue is empty
        assertNull(receive());
        
        Query query = new QueryImpl();
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        QueryPool queryPool = new QueryPool("default");
        TaskKey key = storageService.storeQuery(queryPool, query);
        assertNotNull(key);
        
        // ensure we got a task notification
        QueryTaskNotification notification = receive();
        assertNotNull(notification);
        assertEquals(key, notification.getTaskKey());
        
        // ensure the message queue is empty again
        assertNull(receive());
        
        QueryTask task = storageService.getTask(key);
        assertQueryTask(key, QueryTask.QUERY_ACTION.CREATE, query, task);
    }
    
    @Test
    public void testStoreTask() throws ParseException, InterruptedException {
        // ensure the message queue is empty
        assertNull(receive());
        
        Query query = new QueryImpl();
        query.setQueryLogicName("EventQuery");
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        UUID queryId = UUID.randomUUID();
        QueryPool queryPool = new QueryPool("default");
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryPool, queryId, query.getQueryLogicName(), query);
        QueryTask task = storageService.createTask(QueryTask.QUERY_ACTION.NEXT, checkpoint);
        TaskKey key = task.getTaskKey();
        assertEquals(checkpoint.getQueryKey(), key);
        
        // ensure we got a task notification
        QueryTaskNotification notification = receive();
        assertNotNull(notification);
        assertEquals(key, notification.getTaskKey());
        
        // ensure the message queue is empty again
        assertNull(receive());
        
        task = storageService.getTask(key);
        assertQueryTask(key, QueryTask.QUERY_ACTION.NEXT, query, task);
    }
    
    @Test
    public void testCheckpointTask() throws InterruptedException, ParseException {
        // ensure the message queue is empty
        assertNull(receive());
        
        Query query = new QueryImpl();
        query.setQueryLogicName("EventQuery");
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        UUID queryId = UUID.randomUUID();
        QueryPool queryPool = new QueryPool("default");
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
        QueryTaskNotification notification = receive();
        assertNull(receive());
        
        // now update the task
        Map<String,Object> props = new HashMap<>();
        props.put("checkpoint", Boolean.TRUE);
        checkpoint = new QueryCheckpoint(queryPool, queryId, query.getQueryLogicName(), props);
        storageService.checkpointTask(notification.getTaskKey(), checkpoint);
        
        // ensure we did not get another task notification
        assertNull(receive());
        
        QueryTask task = storageService.getTask(notification.getTaskKey());
        assertEquals(checkpoint, task.getQueryCheckpoint());
    }
    
    @Test
    public void testGetAndDeleteTask() throws ParseException, InterruptedException {
        // ensure the message queue is empty
        assertNull(receive());
        
        Query query = new QueryImpl();
        query.setQueryLogicName("EventQuery");
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        UUID queryId = UUID.randomUUID();
        QueryPool queryPool = new QueryPool("default");
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryPool, queryId, query.getQueryLogicName(), query);
        
        storageService.createTask(QueryTask.QUERY_ACTION.NEXT, checkpoint);
        
        // clear out message queue
        QueryTaskNotification notification = receive();
        assertNull(receive());
        
        // ensure we can get a task
        QueryTask task = storageService.getTask(notification.getTaskKey());
        assertQueryTask(notification.getTaskKey(), QueryTask.QUERY_ACTION.NEXT, query, task);
        
        // now delete the task
        storageService.deleteTask(notification.getTaskKey());
        
        // ensure we did not get another task notification
        assertNull(receive());
        
        // ensure there is no more task stored
        task = storageService.getTask(notification.getTaskKey());
        assertNull(task);
    }
    
    @Test
    public void testGetAndDeleteQueryTasks() throws ParseException, InterruptedException {
        // ensure the message queue is empty
        assertNull(receive());
        
        Query query = new QueryImpl();
        query.setQueryLogicName("EventQuery");
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        UUID queryId = UUID.randomUUID();
        QueryPool queryPool = new QueryPool("default");
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryPool, queryId, query.getQueryLogicName(), query);
        
        storageService.createTask(QueryTask.QUERY_ACTION.NEXT, checkpoint);
        
        // clear out message queue
        QueryTaskNotification notification = receive();
        assertNull(receive());
        
        // not get the query tasks
        List<QueryTask> tasks = storageService.getTasks(queryId);
        assertEquals(1, tasks.size());
        QueryTask task = tasks.get(0);
        assertQueryTask(notification.getTaskKey(), QueryTask.QUERY_ACTION.NEXT, query, task);
        
        // now delete the query tasks
        storageService.deleteQuery(queryId);
        
        // ensure we did not get another task notification
        assertNull(receive());
        
        // make sure it deleted
        tasks = storageService.getTasks(queryId);
        assertEquals(0, tasks.size());
    }
    
    @Test
    public void testGetAndDeleteTypeTasks() throws ParseException, InterruptedException {
        // ensure the message queue is empty
        assertNull(receive());
        
        Query query = new QueryImpl();
        query.setQueryLogicName("EventQuery");
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        UUID queryId = UUID.randomUUID();
        QueryPool queryPool = new QueryPool("default");
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryPool, queryId, query.getQueryLogicName(), query);
        
        storageService.createTask(QueryTask.QUERY_ACTION.NEXT, checkpoint);
        
        // clear out message queue
        QueryTaskNotification notification = receive();
        assertNull(receive());
        
        // not get the query tasks
        List<QueryTask> tasks = storageService.getTasks(queryPool);
        assertEquals(1, tasks.size());
        QueryTask task = tasks.get(0);
        assertQueryTask(notification.getTaskKey(), QueryTask.QUERY_ACTION.NEXT, query, task);
        
        // now delete the query tasks
        storageService.deleteQueryPool(queryPool);
        
        // ensure we did not get another task notification
        assertNull(receive());
        
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
            return new SimpleRoutingConnectionFactory();
        }
    }
}
