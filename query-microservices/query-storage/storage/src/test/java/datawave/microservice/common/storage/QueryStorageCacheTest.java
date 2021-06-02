package datawave.microservice.common.storage;

import datawave.microservice.query.DefaultQueryParameters;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.bus.event.RemoteQueryTaskNotificationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.Message;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ContextConfiguration(classes = QueryStorageCacheTest.QueryStorageCacheTestConfiguration.class)
public abstract class QueryStorageCacheTest {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final long FIVE_MIN = 5 * 60 * 1000L;
    
    @ActiveProfiles({"QueryStorageCacheTest", "sync-enabled", "send-notifications"})
    public static class LocalQueryStorageCacheTest extends QueryStorageCacheTest {}
    
    @EmbeddedKafka
    @ActiveProfiles({"QueryStorageCacheTest", "sync-enabled", "send-notifications", "use-embedded-kafka"})
    public static class EmbeddedKafkaQueryStorageCacheTest extends QueryStorageCacheTest {}
    
    @Disabled("Cannot run this test without an externally deployed RabbitMQ instance.")
    @ActiveProfiles({"QueryStorageCacheTest", "sync-enabled", "send-notifications", "use-rabbit"})
    public static class RabbitQueryStorageCacheTest extends QueryStorageCacheTest {}
    
    @Disabled("Cannot run this test without an externally deployed Kafka instance.")
    @ActiveProfiles({"QueryStorageCacheTest", "sync-enabled", "send-notifications", "use-kafka"})
    public static class KafkaQueryStorageCacheTest extends QueryStorageCacheTest {}
    
    @Configuration
    @Profile("QueryStorageCacheTest")
    @ComponentScan(basePackages = "datawave.microservice")
    public static class QueryStorageCacheTestConfiguration {
        @Bean
        public LinkedList<QueryTaskNotification> queryTaskNotifications() {
            return new LinkedList<>();
        }
        
        @Bean
        @Primary
        public ApplicationEventPublisher publisher(ApplicationEventPublisher publisher) {
            return event -> {
                if (event instanceof RemoteQueryTaskNotificationEvent)
                    queryTaskNotifications().push(((RemoteQueryTaskNotificationEvent) event).getNotification());
            };
        }
    }
    
    @SpringBootApplication(scanBasePackages = "datawave.microservice")
    public static class TestApplication {
        public static void main(String[] args) {
            SpringApplication.run(QueryStorageCacheTest.TestApplication.class, args);
        }
    }
    
    @Autowired
    private QueryCache queryCache;
    
    @Autowired
    private QueryStorageCache storageService;
    
    @Autowired
    private QueryQueueManager queueManager;
    
    @Autowired
    private LinkedList<QueryTaskNotification> queryTaskNotifications;
    
    public String TEST_POOL = "TestPool";
    
    private Queue<QueryQueueListener> listeners = new LinkedList<>();
    private Queue<UUID> createdQueries = new LinkedList<>();
    
    @AfterEach
    public void cleanup() {
        while (!listeners.isEmpty()) {
            listeners.remove().stop();
        }
        while (!createdQueries.isEmpty()) {
            try {
                storageService.deleteQuery(createdQueries.remove());
            } catch (Exception e) {
                log.error("Failed to delete query", e);
            }
        }
    }
    
    @DirtiesContext
    @Test
    public void testLocking() throws ParseException, InterruptedException, IOException, TaskLockException {
        UUID queryId = UUID.randomUUID();
        QueryKey queryKey = new QueryKey(new QueryPool("default"), queryId, "EventQuery");
        queryCache.updateQueryStatus(new QueryStatus(queryKey));
        queryCache.updateTaskStates(new TaskStates(queryKey, 3));
        QueryTask task = queryCache.addQueryTask(QueryTask.QUERY_ACTION.CREATE, new QueryCheckpoint(queryKey, new HashMap<>()));
        TaskKey key = task.getTaskKey();
        QueryStorageLock qLock = queryCache.getQueryStatusLock(queryId);
        QueryStorageLock sLock = queryCache.getTaskStatesLock(queryId);
        QueryStorageLock tLock = queryCache.getTaskLock(key);
        assertFalse(qLock.isLocked());
        assertFalse(sLock.isLocked());
        assertFalse(tLock.isLocked());
        qLock.lock();
        assertTrue(qLock.isLocked());
        assertFalse(sLock.isLocked());
        assertFalse(tLock.isLocked());
        sLock.lock();
        assertTrue(qLock.isLocked());
        assertTrue(sLock.isLocked());
        assertFalse(tLock.isLocked());
        tLock.lock();
        assertTrue(qLock.isLocked());
        assertTrue(sLock.isLocked());
        assertTrue(tLock.isLocked());
        qLock.unlock();
        assertFalse(qLock.isLocked());
        assertTrue(sLock.isLocked());
        assertTrue(tLock.isLocked());
        sLock.unlock();
        assertFalse(qLock.isLocked());
        assertFalse(sLock.isLocked());
        assertTrue(tLock.isLocked());
        tLock.unlock();
        assertFalse(qLock.isLocked());
        assertFalse(sLock.isLocked());
        assertFalse(tLock.isLocked());
    }
    
    @DirtiesContext
    @Test
    public void testCreateQuery() throws ParseException, InterruptedException, IOException, TaskLockException {
        // ensure the message queue is empty
        assertTrue(queryTaskNotifications.isEmpty());
        
        Query query = new QueryImpl();
        query.setQuery("foo == bar");
        query.setQueryLogicName("EventQuery");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        QueryPool queryPool = new QueryPool(TEST_POOL);
        Set<Authorizations> auths = new HashSet<>();
        auths.add(new Authorizations("FOO", "BAR"));
        TaskKey key = storageService.createQuery(queryPool, query, auths, 3);
        createdQueries.add(key.getQueryId());
        assertNotNull(key);
        
        TaskStates states = storageService.getTaskStates(key.getQueryId());
        assertEquals(TaskStates.TASK_STATE.READY, states.getState(key));
        
        // ensure we got a task notification
        QueryTaskNotification notification = queryTaskNotifications.pop();
        assertNotNull(notification);
        assertEquals(key, notification.getTaskKey());
        
        // ensure the message queue is empty again
        assertTrue(queryTaskNotifications.isEmpty());
        
        QueryTask task = storageService.getTask(key, 0, FIVE_MIN);
        assertQueryTask(key, QueryTask.QUERY_ACTION.CREATE, query, task);
        
        List<QueryTask> tasks = queryCache.getTasks(key.getQueryId());
        assertNotNull(tasks);
        assertEquals(1, tasks.size());
        assertQueryTask(key, QueryTask.QUERY_ACTION.CREATE, query, tasks.get(0));
        
        tasks = queryCache.getTasks(queryPool);
        assertNotNull(tasks);
        assertEquals(1, tasks.size());
        assertQueryTask(key, QueryTask.QUERY_ACTION.CREATE, query, tasks.get(0));
        
        List<QueryState> queries = queryCache.getQueries();
        assertNotNull(queries);
        assertEquals(1, queries.size());
        assertQueryCreate(key.getQueryId(), queryPool, queries.get(0));
        
        List<TaskDescription> taskDescs = queryCache.getTaskDescriptions(key.getQueryId());
        assertNotNull(taskDescs);
        assertEquals(1, taskDescs.size());
        assertQueryCreate(key.getQueryId(), queryPool, query, taskDescs.get(0));
    }
    
    public static class QueryTaskHolder {
        public QueryTask task;
        public Exception throwable;
    }
    
    private QueryTask getTaskOnSeparateThread(final TaskKey key, final long waitMs) throws Exception {
        QueryTaskHolder taskHolder = new QueryTaskHolder();
        Thread t = new Thread(new Runnable() {
            public void run() {
                try {
                    taskHolder.task = storageService.getTask(key, waitMs, FIVE_MIN);
                } catch (Exception e) {
                    taskHolder.throwable = e;
                }
            }
        });
        t.start();
        while (t.isAlive()) {
            Thread.sleep(1);
        }
        if (taskHolder.throwable != null) {
            throw taskHolder.throwable;
        }
        return taskHolder.task;
    }
    
    @DirtiesContext
    @Test
    public void testStoreTask() throws ParseException, InterruptedException, IOException, TaskLockException {
        // ensure the message queue is empty
        assertTrue(queryTaskNotifications.isEmpty());
        
        Query query = new QueryImpl();
        query.setQueryLogicName("EventQuery");
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        UUID queryId = UUID.randomUUID();
        createdQueries.add(queryId);
        QueryPool queryPool = new QueryPool(TEST_POOL);
        QueryKey queryKey = new QueryKey(queryPool, queryId, query.getQueryLogicName());
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryKey, query);
        queryCache.updateTaskStates(new TaskStates(queryKey, 10));
        QueryTask task = storageService.createTask(QueryTask.QUERY_ACTION.NEXT, checkpoint);
        TaskKey key = task.getTaskKey();
        assertEquals(checkpoint.getQueryKey(), key);
        
        TaskStates states = storageService.getTaskStates(key.getQueryId());
        assertEquals(TaskStates.TASK_STATE.READY, states.getState(key));
        
        // ensure we got a task notification
        QueryTaskNotification notification = queryTaskNotifications.pop();
        assertNotNull(notification);
        assertEquals(key, notification.getTaskKey());
        
        // ensure the message queue is empty again
        assertTrue(queryTaskNotifications.isEmpty());
        
        assertFalse(storageService.getTaskLock(task.getTaskKey()).isLocked());
        
        task = storageService.getTask(key, 0, FIVE_MIN);
        assertQueryTask(key, QueryTask.QUERY_ACTION.NEXT, query, task);
        assertTrue(storageService.getTaskLock(task.getTaskKey()).isLocked());
        
        try {
            task = getTaskOnSeparateThread(key, 0);
            fail("Expected failure due to task already being locked");
        } catch (TaskLockException e) {
            storageService.checkpointTask(task.getTaskKey(), task.getQueryCheckpoint());
            task = storageService.getTask(key, 0, FIVE_MIN);
        } catch (Exception e) {
            fail("Unexpected exception " + e.getMessage());
        }
        assertQueryTask(key, QueryTask.QUERY_ACTION.NEXT, query, task);
        
        storageService.deleteTask(task.getTaskKey());
        
        assertFalse(storageService.getTaskLock(task.getTaskKey()).isLocked());
    }
    
    @DirtiesContext
    @Test
    public void testCheckpointTask() throws InterruptedException, ParseException, IOException, TaskLockException {
        // ensure the message queue is empty
        assertTrue(queryTaskNotifications.isEmpty());
        
        Query query = new QueryImpl();
        query.setQueryLogicName("EventQuery");
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        UUID queryId = UUID.randomUUID();
        createdQueries.add(queryId);
        QueryPool queryPool = new QueryPool(TEST_POOL);
        QueryKey queryKey = new QueryKey(queryPool, queryId, query.getQueryLogicName());
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryKey, query);
        queryCache.updateTaskStates(new TaskStates(queryKey, 10));
        
        TaskKey key = new TaskKey(UUID.randomUUID(), queryPool, UUID.randomUUID(), query.getQueryLogicName());
        try {
            storageService.checkpointTask(key, checkpoint);
            fail("Expected storage service to fail checkpointing an invalid task key without a lock");
        } catch (TaskLockException e) {
            // expected
        }
        
        assertTrue(storageService.getTaskLock(key).tryLock());
        try {
            storageService.checkpointTask(key, checkpoint);
            fail("Expected storage service to fail checkpointing an invalid task key");
        } catch (IllegalArgumentException e) {
            // expected
        }
        
        key = new TaskKey(UUID.randomUUID(), checkpoint.getQueryKey());
        assertTrue(storageService.getTaskLock(key).tryLock());
        try {
            storageService.checkpointTask(key, checkpoint);
            fail("Expected storage service to fail checkpointing a missing task");
        } catch (NullPointerException e) {
            // expected
        }
        
        storageService.createTask(QueryTask.QUERY_ACTION.NEXT, checkpoint);
        
        // clear out message queue
        QueryTaskNotification notification = queryTaskNotifications.pop();
        assertNotNull(notification);
        assertTrue(queryTaskNotifications.isEmpty());
        
        // get the task to aquire the lock
        storageService.getTask(notification.getTaskKey(), 0, FIVE_MIN);
        
        // now update the task
        Map<String,Object> props = new HashMap<>();
        props.put("checkpoint", Boolean.TRUE);
        checkpoint = new QueryCheckpoint(queryPool, queryId, query.getQueryLogicName(), props);
        storageService.checkpointTask(notification.getTaskKey(), checkpoint);
        
        // ensure we did not get another task notification
        assertTrue(queryTaskNotifications.isEmpty());
        
        QueryTask task = storageService.getTask(notification.getTaskKey(), 0, FIVE_MIN);
        assertEquals(checkpoint, task.getQueryCheckpoint());
    }
    
    @DirtiesContext
    @Test
    public void testGetAndDeleteTask() throws ParseException, InterruptedException, IOException, TaskLockException {
        // ensure the message queue is empty
        assertTrue(queryTaskNotifications.isEmpty());
        
        Query query = new QueryImpl();
        query.setQueryLogicName("EventQuery");
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        UUID queryId = UUID.randomUUID();
        createdQueries.add(queryId);
        QueryPool queryPool = new QueryPool(TEST_POOL);
        QueryKey queryKey = new QueryKey(queryPool, queryId, query.getQueryLogicName());
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryKey, query);
        queryCache.updateTaskStates(new TaskStates(queryKey, 10));
        
        storageService.createTask(QueryTask.QUERY_ACTION.NEXT, checkpoint);
        
        // clear out message queue
        QueryTaskNotification notification = queryTaskNotifications.pop();
        assertNotNull(notification);
        assertTrue(queryTaskNotifications.isEmpty());
        
        // ensure we can get a task
        QueryTask task = storageService.getTask(notification.getTaskKey(), 0, FIVE_MIN);
        assertQueryTask(notification.getTaskKey(), QueryTask.QUERY_ACTION.NEXT, query, task);
        
        // now delete the task
        storageService.deleteTask(notification.getTaskKey());
        
        // ensure we did not get another task notification
        assertTrue(queryTaskNotifications.isEmpty());
        
        // ensure there is no more task stored
        task = storageService.getTask(notification.getTaskKey(), 0, FIVE_MIN);
        assertNull(task);
    }
    
    @DirtiesContext
    @Test
    public void testGetAndDeleteQueryTasks() throws ParseException, InterruptedException, IOException, TaskLockException {
        // ensure the message queue is empty
        assertTrue(queryTaskNotifications.isEmpty());
        
        Query query = new QueryImpl();
        query.setQueryLogicName("EventQuery");
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        UUID queryId = UUID.randomUUID();
        createdQueries.add(queryId);
        QueryPool queryPool = new QueryPool(TEST_POOL);
        QueryKey queryKey = new QueryKey(queryPool, queryId, query.getQueryLogicName());
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryKey, query);
        queryCache.updateTaskStates(new TaskStates(queryKey, 10));
        
        storageService.createTask(QueryTask.QUERY_ACTION.NEXT, checkpoint);
        
        // clear out message queue
        QueryTaskNotification notification = queryTaskNotifications.pop();
        assertNotNull(notification);
        assertTrue(queryTaskNotifications.isEmpty());
        
        // not get the query tasks
        List<TaskKey> tasks = storageService.getTasks(queryId);
        assertEquals(1, tasks.size());
        QueryTask task;
        task = storageService.getTask(tasks.get(0), 0, FIVE_MIN);
        assertQueryTask(notification.getTaskKey(), QueryTask.QUERY_ACTION.NEXT, query, task);
        
        // now delete the query tasks
        storageService.deleteQuery(queryId);
        createdQueries.remove(queryId);
        
        // ensure we did not get another task notification
        assertTrue(queryTaskNotifications.isEmpty());
        
        // make sure it deleted
        tasks = storageService.getTasks(queryId);
        assertEquals(0, tasks.size());
    }
    
    @DirtiesContext
    @Test
    public void testGetAndDeleteTypeTasks() throws ParseException, InterruptedException, IOException, TaskLockException {
        // ensure the message queue is empty
        assertTrue(queryTaskNotifications.isEmpty());
        
        Query query = new QueryImpl();
        query.setQueryLogicName("EventQuery");
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        UUID queryId = UUID.randomUUID();
        createdQueries.add(queryId);
        QueryPool queryPool = new QueryPool(TEST_POOL);
        QueryKey queryKey = new QueryKey(queryPool, queryId, query.getQueryLogicName());
        QueryStatus queryStatus = new QueryStatus(queryKey);
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryKey, query);
        queryCache.updateTaskStates(new TaskStates(queryKey, 10));
        
        storageService.updateQueryStatus(queryStatus);
        storageService.createTask(QueryTask.QUERY_ACTION.NEXT, checkpoint);
        
        // clear out message queue
        QueryTaskNotification notification = queryTaskNotifications.pop();
        assertNotNull(notification);
        assertTrue(queryTaskNotifications.isEmpty());
        
        // now get the query tasks
        List<QueryStatus> queries = storageService.getQueryStatus();
        assertEquals(1, queries.size());
        List<TaskKey> tasks = storageService.getTasks(queries.get(0).getQueryKey().getQueryId());
        assertEquals(1, tasks.size());
        QueryTask task = storageService.getTask(tasks.get(0), 0, FIVE_MIN);
        assertQueryTask(notification.getTaskKey(), QueryTask.QUERY_ACTION.NEXT, query, task);
        
        // now delete the query tasks
        storageService.deleteQuery(queryId);
        createdQueries.remove(queryId);
        
        // ensure we did not get another task notification
        assertTrue(queryTaskNotifications.isEmpty());
        
        // make sure it deleted
        queries = storageService.getQueryStatus();
        assertEquals(0, queries.size());
        
    }
    
    @DirtiesContext
    @Test
    public void testTaskStateUpdate() throws ParseException, InterruptedException, IOException, TaskLockException {
        Query query = new QueryImpl();
        query.setQueryLogicName("EventQuery");
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        UUID queryId = UUID.randomUUID();
        createdQueries.add(queryId);
        QueryPool queryPool = new QueryPool(TEST_POOL);
        QueryKey queryKey = new QueryKey(queryPool, queryId, query.getQueryLogicName());
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryKey, query);
        TaskStates states = new TaskStates(queryKey, 2);
        TaskKey key = new TaskKey(UUID.randomUUID(), queryKey);
        TaskKey key2 = new TaskKey(UUID.randomUUID(), queryKey);
        TaskKey key3 = new TaskKey(UUID.randomUUID(), queryKey);
        states.setState(key, TaskStates.TASK_STATE.READY);
        
        storageService.updateTaskStates(states);
        
        assertEquals(TaskStates.TASK_STATE.READY, queryCache.getTaskStates(queryId).getState(key));
        assertTrue(storageService.updateTaskState(key, TaskStates.TASK_STATE.RUNNING));
        assertEquals(TaskStates.TASK_STATE.RUNNING, queryCache.getTaskStates(queryId).getState(key));
        assertTrue(storageService.updateTaskState(key2, TaskStates.TASK_STATE.RUNNING));
        // should fail trying to run another
        assertFalse(storageService.updateTaskState(key3, TaskStates.TASK_STATE.RUNNING));
        assertTrue(storageService.updateTaskState(key, TaskStates.TASK_STATE.COMPLETED));
        assertEquals(TaskStates.TASK_STATE.COMPLETED, queryCache.getTaskStates(queryId).getState(key));
        // now this should succeed
        assertTrue(storageService.updateTaskState(key3, TaskStates.TASK_STATE.RUNNING));
    }
    
    @DirtiesContext
    @Test
    public void testQueryStateUpdate() throws ParseException, InterruptedException, IOException, TaskLockException {
        Query query = new QueryImpl();
        query.setQueryLogicName("EventQuery");
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        QueryPool queryPool = new QueryPool(TEST_POOL);
        Set<Authorizations> auths = new HashSet<>();
        auths.add(new Authorizations("FOO", "BAR"));
        TaskKey taskKey = storageService.createQuery(queryPool, query, auths, 2);
        UUID queryId = taskKey.getQueryId();
        createdQueries.add(queryId);
        
        assertEquals(QueryStatus.QUERY_STATE.DEFINED, storageService.getQueryStatus(queryId).getQueryState());
        storageService.updateQueryStatus(queryId, QueryStatus.QUERY_STATE.CANCELED);
        assertEquals(QueryStatus.QUERY_STATE.CANCELED, storageService.getQueryStatus(queryId).getQueryState());
    }
    
    @DirtiesContext
    @Test
    public void testResultsQueue() throws Exception {
        // ensure the message queue is empty
        assertTrue(queryTaskNotifications.isEmpty());
        
        Query query = new QueryImpl();
        query.setQuery("foo == bar");
        query.setQueryLogicName("EventQuery");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        QueryPool queryPool = new QueryPool(TEST_POOL);
        Set<Authorizations> auths = new HashSet<>();
        auths.add(new Authorizations("FOO", "BAR"));
        TaskKey key = storageService.createQuery(queryPool, query, auths, 3);
        createdQueries.add(key.getQueryId());
        assertNotNull(key);
        
        // setup a listener for this query's result queue
        QueryQueueListener listener = queueManager.createListener("TestListener", key.getQueryId().toString());
        listeners.add(listener);
        
        // send a result
        Result result = new Result("result1", new Object[] {"Some result"});
        queueManager.sendMessage(key.getQueryId(), result);
        
        // receive the message
        Message<Result> msg = listener.receive();
        
        assertNotNull(msg, "Got no result message");
        assertEquals(result.getPayload()[0], msg.getPayload().getPayload()[0]);
    }
    
    private void assertQueryCreate(UUID queryId, QueryPool queryPool, QueryState state) {
        assertEquals(queryId, state.getQueryId());
        assertEquals(queryPool, state.getQueryPool());
        // TaskStates tasks = state.getTaskStates();
        // assertEquals(1, tasks.getTaskStates().size());
    }
    
    private void assertQueryCreate(UUID queryId, QueryPool queryPool, Query query, TaskDescription task) throws ParseException {
        assertNotNull(task.getTaskKey());
        assertEquals(queryId, task.getTaskKey().getQueryId());
        assertEquals(queryPool, task.getTaskKey().getQueryPool());
        assertEquals(QueryTask.QUERY_ACTION.CREATE, task.getAction());
        assertEquals(query.getQuery(), task.getParameters().get(QueryImpl.QUERY));
        assertEquals(DefaultQueryParameters.formatDate(query.getBeginDate()), task.getParameters().get(QueryImpl.BEGIN_DATE));
        assertEquals(DefaultQueryParameters.formatDate(query.getEndDate()), task.getParameters().get(QueryImpl.END_DATE));
    }
    
    private void assertQueryTask(TaskKey key, QueryTask.QUERY_ACTION action, Query query, QueryTask task) throws ParseException {
        assertEquals(key, task.getTaskKey());
        assertEquals(action, task.getAction());
        assertEquals(task.getQueryCheckpoint().getQueryKey(), key);
        assertEquals(query, task.getQueryCheckpoint().getPropertiesAsQuery());
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
