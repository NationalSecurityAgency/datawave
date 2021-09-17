package datawave.microservice.query.storage;

import datawave.microservice.query.DefaultQueryParameters;
import datawave.microservice.query.remote.QueryRequest;
import datawave.services.query.logic.QueryCheckpoint;
import datawave.services.query.logic.QueryKey;
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
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.Message;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
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
    
    public String TEST_POOL = "TestPool";
    
    private Queue<QueryQueueListener> listeners = new LinkedList<>();
    private Queue<String> createdQueries = new LinkedList<>();
    
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
        String queryId = UUID.randomUUID().toString();
        QueryKey queryKey = new QueryKey("default", queryId, "EventQuery");
        queryCache.updateQueryStatus(new QueryStatus(queryKey));
        queryCache.updateTaskStates(new TaskStates(queryKey, 3));
        QueryTask task = queryCache.addQueryTask(QueryRequest.Method.CREATE, new QueryCheckpoint(queryKey, new HashMap<>()));
        QueryStorageLock qLock = queryCache.getQueryStatusLock(queryId);
        QueryStorageLock sLock = queryCache.getTaskStatesLock(queryId);
        assertFalse(qLock.isLocked());
        assertFalse(sLock.isLocked());
        qLock.lock();
        assertTrue(qLock.isLocked());
        assertFalse(sLock.isLocked());
        sLock.lock();
        assertTrue(qLock.isLocked());
        assertTrue(sLock.isLocked());
        qLock.unlock();
        assertFalse(qLock.isLocked());
        assertTrue(sLock.isLocked());
        sLock.unlock();
        assertFalse(qLock.isLocked());
        assertFalse(sLock.isLocked());
    }
    
    @DirtiesContext
    @Test
    public void testCreateQuery() throws ParseException, InterruptedException, IOException, TaskLockException {
        Query query = new QueryImpl();
        query.setQuery("foo == bar");
        query.setQueryLogicName("EventQuery");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        String queryPool = TEST_POOL;
        Set<Authorizations> auths = new HashSet<>();
        auths.add(new Authorizations("FOO", "BAR"));
        TaskKey key = storageService.createQuery(queryPool, query, auths, 3);
        createdQueries.add(key.getQueryId());
        assertNotNull(key);
        
        TaskStates states = storageService.getTaskStates(key.getQueryId());
        assertEquals(TaskStates.TASK_STATE.READY, states.getState(key));
        
        QueryTask task = storageService.getTask(key);
        assertQueryTask(key.getQueryId(), QueryRequest.Method.CREATE, query, task);
        
        List<QueryTask> tasks = queryCache.getTasks(key.getQueryId());
        assertNotNull(tasks);
        assertEquals(1, tasks.size());
        assertQueryTask(key, QueryRequest.Method.CREATE, query, tasks.get(0));
        
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
                    taskHolder.task = storageService.getTask(key);
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
        Query query = new QueryImpl();
        query.setQueryLogicName("EventQuery");
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        String queryId = UUID.randomUUID().toString();
        createdQueries.add(queryId);
        String queryPool = TEST_POOL;
        QueryKey queryKey = new QueryKey(queryPool, queryId, query.getQueryLogicName());
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryKey, query);
        queryCache.updateTaskStates(new TaskStates(queryKey, 10));
        QueryTask task = storageService.createTask(QueryRequest.Method.NEXT, checkpoint);
        TaskKey key = task.getTaskKey();
        assertEquals(checkpoint.getQueryKey(), key);
        
        TaskStates states = storageService.getTaskStates(key.getQueryId());
        assertEquals(TaskStates.TASK_STATE.READY, states.getState(key));
        
        task = storageService.getTask(key);
        assertQueryTask(key, QueryRequest.Method.NEXT, query, task);
        
        storageService.checkpointTask(task.getTaskKey(), task.getQueryCheckpoint());
        task = storageService.getTask(key);
        assertQueryTask(key, QueryRequest.Method.NEXT, query, task);
        
        storageService.deleteTask(task.getTaskKey());
    }
    
    @DirtiesContext
    @Test
    public void testCheckpointTask() throws InterruptedException, ParseException, IOException, TaskLockException {
        Query query = new QueryImpl();
        query.setQueryLogicName("EventQuery");
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        String queryId = UUID.randomUUID().toString();
        createdQueries.add(queryId);
        String queryPool = TEST_POOL;
        QueryKey queryKey = new QueryKey(queryPool, queryId, query.getQueryLogicName());
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryKey, query);
        queryCache.updateTaskStates(new TaskStates(queryKey, 10));
        QueryRequest.Method action = QueryRequest.Method.CREATE;
        
        TaskKey key = new TaskKey(UUID.randomUUID().toString(), action, queryPool, UUID.randomUUID().toString(), query.getQueryLogicName());
        try {
            storageService.checkpointTask(key, checkpoint);
            fail("Expected storage service to fail checkpointing an inconsistent query key");
        } catch (Exception e) {
            // expected
        }
        
        key = new TaskKey(UUID.randomUUID().toString(), action, checkpoint.getQueryKey());
        try {
            storageService.checkpointTask(key, checkpoint);
            fail("Expected storage service to fail checkpointing a missing task");
        } catch (NullPointerException e) {
            // expected
        }
        
        QueryTask task = storageService.createTask(QueryRequest.Method.NEXT, checkpoint);
        
        QueryTask task2 = storageService.getTask(task.getTaskKey());
        
        assertEquals(task, task2);
        
        // now update the task
        Map<String,Object> props = new HashMap<>();
        props.put("checkpoint", Boolean.TRUE);
        checkpoint = new QueryCheckpoint(queryPool, queryId, query.getQueryLogicName(), props);
        storageService.checkpointTask(task.getTaskKey(), checkpoint);
        
        task2 = storageService.getTask(task.getTaskKey());
        assertEquals(checkpoint, task2.getQueryCheckpoint());
    }
    
    @DirtiesContext
    @Test
    public void testGetAndDeleteTask() throws ParseException, InterruptedException, IOException, TaskLockException {
        Query query = new QueryImpl();
        query.setQueryLogicName("EventQuery");
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        String queryId = UUID.randomUUID().toString();
        createdQueries.add(queryId);
        String queryPool = TEST_POOL;
        QueryKey queryKey = new QueryKey(queryPool, queryId, query.getQueryLogicName());
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryKey, query);
        queryCache.updateTaskStates(new TaskStates(queryKey, 10));
        
        QueryTask task = storageService.createTask(QueryRequest.Method.NEXT, checkpoint);
        
        task = storageService.getTask(task.getTaskKey());
        assertNotNull(task);
        
        // now delete the task
        storageService.deleteTask(task.getTaskKey());
        
        // ensure there is no more task stored
        task = storageService.getTask(task.getTaskKey());
        assertNull(task);
    }
    
    @DirtiesContext
    @Test
    public void testGetAndDeleteQueryTasks() throws ParseException, InterruptedException, IOException, TaskLockException {
        Query query = new QueryImpl();
        query.setQueryLogicName("EventQuery");
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        String queryId = UUID.randomUUID().toString();
        createdQueries.add(queryId);
        String queryPool = TEST_POOL;
        QueryKey queryKey = new QueryKey(queryPool, queryId, query.getQueryLogicName());
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryKey, query);
        queryCache.updateTaskStates(new TaskStates(queryKey, 10));
        
        TaskKey taskKey = storageService.createTask(QueryRequest.Method.NEXT, checkpoint).getTaskKey();
        
        // not get the query tasks
        List<TaskKey> tasks = storageService.getTasks(queryId);
        assertEquals(1, tasks.size());
        QueryTask task = storageService.getTask(tasks.get(0));
        
        assertQueryTask(taskKey, QueryRequest.Method.NEXT, query, task);
        
        // now delete the query tasks
        storageService.deleteQuery(queryId);
        createdQueries.remove(queryId);
        
        // make sure it deleted
        tasks = storageService.getTasks(queryId);
        assertEquals(0, tasks.size());
    }
    
    @DirtiesContext
    @Test
    public void testGetAndDeleteTypeTasks() throws ParseException, InterruptedException, IOException, TaskLockException {
        Query query = new QueryImpl();
        query.setQueryLogicName("EventQuery");
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        String queryId = UUID.randomUUID().toString();
        createdQueries.add(queryId);
        String queryPool = TEST_POOL;
        QueryKey queryKey = new QueryKey(queryPool, queryId, query.getQueryLogicName());
        QueryStatus queryStatus = new QueryStatus(queryKey);
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryKey, query);
        queryCache.updateTaskStates(new TaskStates(queryKey, 10));
        
        storageService.updateQueryStatus(queryStatus);
        TaskKey taskKey = storageService.createTask(QueryRequest.Method.NEXT, checkpoint).getTaskKey();
        
        // now get the query tasks
        List<QueryStatus> queries = storageService.getQueryStatus();
        assertEquals(1, queries.size());
        List<TaskKey> tasks = storageService.getTasks(queries.get(0).getQueryKey().getQueryId());
        assertEquals(1, tasks.size());
        QueryTask task = storageService.getTask(tasks.get(0));
        assertQueryTask(taskKey, QueryRequest.Method.NEXT, query, task);
        
        // now delete the query tasks
        storageService.deleteQuery(queryId);
        createdQueries.remove(queryId);
        
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
        String queryId = UUID.randomUUID().toString();
        createdQueries.add(queryId);
        String queryPool = TEST_POOL;
        QueryKey queryKey = new QueryKey(queryPool, queryId, query.getQueryLogicName());
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryKey, query);
        TaskStates states = new TaskStates(queryKey, 2);
        QueryRequest.Method action = QueryRequest.Method.CREATE;
        TaskKey key = new TaskKey(UUID.randomUUID().toString(), action, queryKey);
        TaskKey key2 = new TaskKey(UUID.randomUUID().toString(), action, queryKey);
        TaskKey key3 = new TaskKey(UUID.randomUUID().toString(), action, queryKey);
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
        String queryPool = TEST_POOL;
        Set<Authorizations> auths = new HashSet<>();
        auths.add(new Authorizations("FOO", "BAR"));
        TaskKey taskKey = storageService.createQuery(queryPool, query, auths, 2);
        String queryId = taskKey.getQueryId();
        createdQueries.add(queryId);
        
        assertEquals(QueryStatus.QUERY_STATE.CREATED, storageService.getQueryStatus(queryId).getQueryState());
        storageService.updateQueryStatus(queryId, QueryStatus.QUERY_STATE.CANCELED);
        assertEquals(QueryStatus.QUERY_STATE.CANCELED, storageService.getQueryStatus(queryId).getQueryState());
    }
    
    @DirtiesContext
    @Test
    public void testResultsQueue() throws Exception {
        Query query = new QueryImpl();
        query.setQuery("foo == bar");
        query.setQueryLogicName("EventQuery");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        String queryPool = TEST_POOL;
        Set<Authorizations> auths = new HashSet<>();
        auths.add(new Authorizations("FOO", "BAR"));
        TaskKey key = storageService.createQuery(queryPool, query, auths, 3);
        createdQueries.add(key.getQueryId());
        assertNotNull(key);
        
        // setup a listener for this query's result queue
        QueryQueueListener listener = queueManager.createListener("TestListener", key.getQueryId().toString());
        listeners.add(listener);
        
        // send a result
        Result result = new Result("result1", "Some result");
        queueManager.sendMessage(key.getQueryId(), result);
        
        // receive the message
        Message<Result> msg = listener.receive();
        
        assertNotNull(msg, "Got no result message");
        assertEquals(result.getPayload(), msg.getPayload().getPayload());
    }
    
    private void assertQueryCreate(String queryId, String queryPool, QueryState state) {
        assertEquals(queryId, state.getQueryStatus().getQueryKey().getQueryId());
        assertEquals(queryPool, state.getQueryStatus().getQueryKey().getQueryPool());
        // TaskStates tasks = state.getTaskStates();
        // assertEquals(1, tasks.getTaskStates().size());
    }
    
    private void assertQueryCreate(String queryId, String queryPool, Query query, TaskDescription task) throws ParseException {
        assertNotNull(task.getTaskKey());
        assertEquals(queryId, task.getTaskKey().getQueryId());
        assertEquals(queryPool, task.getTaskKey().getQueryPool());
        assertEquals(QueryRequest.Method.CREATE, task.getTaskKey().getAction());
        assertEquals(query.getQuery(), task.getParameters().get(QueryImpl.QUERY));
        assertEquals(DefaultQueryParameters.formatDate(query.getBeginDate()), task.getParameters().get(QueryImpl.BEGIN_DATE));
        assertEquals(DefaultQueryParameters.formatDate(query.getEndDate()), task.getParameters().get(QueryImpl.END_DATE));
    }
    
    private void assertQueryTask(String queryId, QueryRequest.Method action, Query query, QueryTask task) throws ParseException {
        assertEquals(queryId, task.getTaskKey().getQueryId());
        assertEquals(action, task.getAction());
        assertEquals(task.getQueryCheckpoint().getQueryKey().getQueryId(), queryId);
        assertEquals(query, task.getQueryCheckpoint().getPropertiesAsQuery());
    }
    
    private void assertQueryTask(TaskKey taskKey, QueryRequest.Method action, Query query, QueryTask task) throws ParseException {
        assertQueryTask(taskKey.getQueryId(), action, query, task);
    }
    
}
