package datawave.microservice.common.storage;

import datawave.microservice.common.storage.config.QueryStorageConfig;
import datawave.microservice.common.storage.config.QueryStorageProperties;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = QueryStorageCacheTest.QueryStorageTestConfiguration.class)
@ActiveProfiles({"QueryStorageCacheTest", "sync-disabled"})
@ComponentScan(basePackages = "datawave.microservice.common.storage.config, datawave.microservice.common.storage")
public class QueryStorageCacheTest {
    @Autowired
    private QueryStorageCache storageService;
    
    @Autowired
    private QueryStorageConfig.TaskNotificationSourceBinding taskNotificationSourceBinding;
    
    @Autowired
    @Qualifier(QueryStorageConfig.TaskNotificationSourceBinding.NAME)
    private MessageChannel taskNotificationChannel;
    
    @Autowired
    private MessageCollector messageCollector;
    
    @After
    public void cleanup() throws InterruptedException {
        storageService.clear();
        messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).clear();
    }
    
    @Test
    public void testStoreQuery() throws ParseException, InterruptedException {
        // ensure the message queue is empty
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
        Query query = new QueryImpl();
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        QueryPool queryPool = new QueryPool("default");
        TaskKey key = storageService.storeQuery(queryPool, query);
        assertNotNull(key);
        
        // ensure we got a task notification
        assertFalse(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        QueryTaskNotification notification = (QueryTaskNotification) messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).take()
                        .getPayload();
        assertEquals(key, notification.getTaskKey());
        
        // ensure the message queue is empty again
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
        QueryTask task = storageService.getTask(key);
        assertQueryTask(key, QueryTask.QUERY_ACTION.CREATE, query, task);
    }
    
    @Test
    public void testStoreTask() throws ParseException, InterruptedException {
        // ensure the message queue is empty
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
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
        assertFalse(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        QueryTaskNotification notification = (QueryTaskNotification) messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).take()
                        .getPayload();
        assertEquals(key, notification.getTaskKey());
        
        // ensure the message queue is empty again
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
        task = storageService.getTask(key);
        assertQueryTask(key, QueryTask.QUERY_ACTION.NEXT, query, task);
    }
    
    @Test
    public void testCheckpointTask() throws InterruptedException, ParseException {
        // ensure the message queue is empty
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
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
        QueryTaskNotification notification = (QueryTaskNotification) messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).take()
                        .getPayload();
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
        // now update the task
        Map<String,Object> props = new HashMap<>();
        props.put("checkpoint", Boolean.TRUE);
        checkpoint = new QueryCheckpoint(queryPool, queryId, query.getQueryLogicName(), props);
        storageService.checkpointTask(notification.getTaskKey(), checkpoint);
        
        // ensure we did not get another task notification
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
        QueryTask task = storageService.getTask(notification.getTaskKey());
        assertEquals(checkpoint, task.getQueryCheckpoint());
    }
    
    @Test
    public void testGetAndDeleteTask() throws ParseException, InterruptedException {
        // ensure the message queue is empty
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
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
        QueryTaskNotification notification = (QueryTaskNotification) messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).take()
                        .getPayload();
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
        // ensure we can get a task
        QueryTask task = storageService.getTask(notification.getTaskKey());
        assertQueryTask(notification.getTaskKey(), QueryTask.QUERY_ACTION.NEXT, query, task);
        
        // now delete the task
        storageService.deleteTask(notification.getTaskKey());
        
        // ensure we did not get another task notification
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
        // ensure there is no more task stored
        task = storageService.getTask(notification.getTaskKey());
        assertNull(task);
    }
    
    @Test
    public void testGetAndDeleteQueryTasks() throws ParseException, InterruptedException {
        // ensure the message queue is empty
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
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
        QueryTaskNotification notification = (QueryTaskNotification) messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).take()
                        .getPayload();
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
        // not get the query tasks
        List<QueryTask> tasks = storageService.getTasks(queryId);
        assertEquals(1, tasks.size());
        QueryTask task = tasks.get(0);
        assertQueryTask(notification.getTaskKey(), QueryTask.QUERY_ACTION.NEXT, query, task);
        
        // now delete the query tasks
        storageService.deleteQuery(queryId);
        
        // ensure we did not get another task notification
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
        // make sure it deleted
        tasks = storageService.getTasks(queryId);
        assertEquals(0, tasks.size());
    }
    
    @Test
    public void testGetAndDeleteTypeTasks() throws ParseException, InterruptedException {
        // ensure the message queue is empty
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
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
        QueryTaskNotification notification = (QueryTaskNotification) messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).take()
                        .getPayload();
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
        // not get the query tasks
        List<QueryTask> tasks = storageService.getTasks(queryPool);
        assertEquals(1, tasks.size());
        QueryTask task = tasks.get(0);
        assertQueryTask(notification.getTaskKey(), QueryTask.QUERY_ACTION.NEXT, query, task);
        
        // now delete the query tasks
        storageService.deleteQueryPool(queryPool);
        
        // ensure we did not get another task notification
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
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
    @Profile("QueryStorageCacheTestTest")
    @ComponentScan(basePackages = "datawave.microservice.common.storage.config, datawave.microservice.common.storage")
    public static class QueryStorageTestConfiguration {}
    
}
