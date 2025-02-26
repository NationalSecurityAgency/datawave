package datawave.microservice.query.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.QueryCheckpoint;
import datawave.core.query.logic.QueryKey;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.query.messaging.QueryResultsListener;
import datawave.microservice.query.messaging.QueryResultsManager;
import datawave.microservice.query.messaging.Result;
import datawave.microservice.query.remote.QueryRequest;
import datawave.query.config.ShardQueryConfiguration;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
public abstract class QueryStorageCacheTest {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    @ActiveProfiles({"QueryStarterDefaults", "QueryStorageCacheTest", "use-test"})
    @ContextConfiguration(classes = QueryStorageCacheTestConfiguration.class)
    public static class LocalQueryStorageCacheTest extends QueryStorageCacheTest {}
    
    @EmbeddedKafka
    @ActiveProfiles({"QueryStarterDefaults", "QueryStorageCacheTest", "use-embedded-kafka"})
    @ContextConfiguration(classes = QueryStorageCacheTestConfiguration.class)
    public static class EmbeddedKafkaQueryStorageCacheTest extends QueryStorageCacheTest {}
    
    @ActiveProfiles({"QueryStarterDefaults", "QueryStorageCacheTest", "use-hazelcast"})
    @ContextConfiguration(classes = QueryStorageCacheTestConfiguration.class)
    public static class HazelcastQueryStorageCacheTest extends QueryStorageCacheTest {
        @DirtiesContext
        @Test
        public void testStorageErrorHandler() throws ParseException, IOException {
            Query query = new QueryImpl();
            query.setQuery("foo == bar");
            query.setQueryLogicName("EventQuery");
            query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
            query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
            String queryPool = TEST_POOL;
            Set<Authorizations> auths = new HashSet<>();
            auths.add(new Authorizations("FOO", "BAR"));
            TaskKey key = storageService.createQuery(queryPool, query, null, auths, 3);
            QueryStatus queryStatus = storageService.getQueryStatus(key.getQueryId());
            queryStatus.setConfig(new ThisClassIsUnserializableOnPurposeToTestErrorHandling());
            try {
                storageService.updateQueryStatus(queryStatus);
                fail("Expected unserializable configuration to fail storage");
            } catch (Exception e) {
                // expected
            }
        }
        
    }
    
    @Disabled("Cannot run this test without an externally deployed RabbitMQ instance.")
    @ActiveProfiles({"QueryStarterDefaults", "QueryStorageCacheTest", "use-rabbit"})
    @ContextConfiguration(classes = QueryStorageCacheTestConfiguration.class)
    public static class RabbitQueryStorageCacheTest extends QueryStorageCacheTest {}
    
    @Disabled("Cannot run this test without an externally deployed Kafka instance.")
    @ActiveProfiles({"QueryStarterDefaults", "QueryStorageCacheTest", "use-kafka"})
    @ContextConfiguration(classes = QueryStorageCacheTestConfiguration.class)
    public static class KafkaQueryStorageCacheTest extends QueryStorageCacheTest {}
    
    @Configuration
    @Profile("QueryStorageCacheTest")
    @ComponentScan(basePackages = "datawave.microservice")
    public static class QueryStorageCacheTestConfiguration {
        @Profile("use-hazelcast")
        @Bean
        public HazelcastInstance hazelcastInstance() {
            Config config = new Config();
            config.setClusterName(UUID.randomUUID().toString());
            return Hazelcast.newHazelcastInstance(config);
        }
    }
    
    @Autowired
    protected QueryStatusCache queryStatusCache;
    
    @Autowired
    protected TaskStatesCache taskStatesCache;
    
    @Autowired
    protected TaskCache taskCache;
    
    @Autowired
    protected QueryStorageCache storageService;
    
    @Autowired
    protected QueryResultsManager queueManager;
    
    public String TEST_POOL = "TestPool";
    
    protected final Queue<QueryResultsListener> listeners = new LinkedList<>();
    protected final Queue<String> createdQueries = new LinkedList<>();
    
    @AfterEach
    public void cleanup() throws Exception {
        while (!listeners.isEmpty()) {
            listeners.remove().close();
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
    public void testLocking() {
        String queryId = UUID.randomUUID().toString();
        QueryKey queryKey = new QueryKey("default", queryId, "EventQuery");
        queryStatusCache.updateQueryStatus(new QueryStatus(queryKey));
        taskStatesCache.updateTaskStates(new TaskStates(queryKey, 3));
        taskCache.addQueryTask(0, QueryRequest.Method.CREATE, new QueryCheckpoint(queryKey));
        QueryStorageLock qLock = queryStatusCache.getQueryStatusLock(queryId);
        QueryStorageLock sLock = taskStatesCache.getTaskStatesLock(queryId);
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
    public void testCreateQuery() throws ParseException, IOException {
        Query query = new QueryImpl();
        query.setQuery("foo == bar");
        query.setQueryLogicName("EventQuery");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        String queryPool = TEST_POOL;
        Set<Authorizations> auths = new HashSet<>();
        auths.add(new Authorizations("FOO", "BAR"));
        TaskKey key = storageService.createQuery(queryPool, query, null, auths, 3);
        createdQueries.add(key.getQueryId());
        assertNotNull(key);
        
        TaskStates states = storageService.getTaskStates(key.getQueryId());
        assertEquals(TaskStates.TASK_STATE.READY, states.getState(key.getTaskId()));
        
        QueryTask task = storageService.getTask(key);
        assertCreateQueryTask(key.getQueryId(), QueryRequest.Method.CREATE, task);
        
        List<QueryTask> tasks = taskCache.getTasks(key.getQueryId());
        assertNotNull(tasks);
        assertEquals(1, tasks.size());
        assertCreateQueryTask(key, QueryRequest.Method.CREATE, tasks.get(0));
        
        List<QueryStatus> queries = queryStatusCache.getQueryStatus();
        assertNotNull(queries);
        assertEquals(1, queries.size());
        assertQueryCreate(key.getQueryId(), queryPool, queries.get(0));
        
        List<TaskDescription> taskDescs = taskCache.getTaskDescriptions(key.getQueryId());
        QueryStatus queryStatus = storageService.getQueryStatus(key.getQueryId());
        assertNotNull(taskDescs);
        assertNotNull(queryStatus);
        assertEquals(1, taskDescs.size());
        assertQueryCreate(key.getQueryId(), queryPool, query, taskDescs.get(0), queryStatus);
        
        queryStatus.setConfig(new ShardQueryConfiguration());
        storageService.updateQueryStatus(queryStatus);
        
        queryStatus = storageService.getQueryStatus(key.getQueryId());
    }
    
    @DirtiesContext
    @Test
    public void testStoreTask() throws ParseException, IOException {
        Query query = new QueryImpl();
        query.setQueryLogicName("EventQuery");
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setQuery(query);
        String queryId = UUID.randomUUID().toString();
        createdQueries.add(queryId);
        String queryPool = TEST_POOL;
        QueryKey queryKey = new QueryKey(queryPool, queryId, query.getQueryLogicName());
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryKey, config.getQueries());
        taskStatesCache.updateTaskStates(new TaskStates(queryKey, 10));
        QueryTask task = storageService.createTask(QueryRequest.Method.NEXT, checkpoint);
        TaskKey key = task.getTaskKey();
        assertEquals(checkpoint.getQueryKey(), key);
        
        TaskStates states = storageService.getTaskStates(key.getQueryId());
        assertEquals(TaskStates.TASK_STATE.READY, states.getState(key.getTaskId()));
        
        task = storageService.getTask(key);
        assertQueryTask(key, QueryRequest.Method.NEXT, task);
        
        storageService.checkpointTask(task.getTaskKey(), task.getQueryCheckpoint());
        task = storageService.getTask(key);
        assertQueryTask(key, QueryRequest.Method.NEXT, task);
        
        storageService.deleteTask(task.getTaskKey());
    }
    
    public static class ThisClassIsUnserializableOnPurposeToTestErrorHandling extends GenericQueryConfiguration {
        private ThisSubclassIsUnserializableOnPurposeToTestErrorHandling obj = new ThisSubclassIsUnserializableOnPurposeToTestErrorHandling();
        
        public static class ThisSubclassIsUnserializableOnPurposeToTestErrorHandling {
            private String bla = "bla";
            
            public String getBla() {
                throw new RuntimeException("Failed to get bla");
            }
            
            public void setBla(String bla) {
                throw new RuntimeException("Failed to set bla");
            }
        }
        
        public ThisSubclassIsUnserializableOnPurposeToTestErrorHandling getObj() {
            throw new RuntimeException("Failed to get obj");
        }
        
        public void setObj(ThisSubclassIsUnserializableOnPurposeToTestErrorHandling obj) {
            throw new RuntimeException("Failed to set obj");
        }
        
    }
    
    @DirtiesContext
    @Test
    public void testCheckpointTask() throws ParseException, IOException {
        Query query = new QueryImpl();
        query.setQueryLogicName("EventQuery");
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setQuery(query);
        String queryId = UUID.randomUUID().toString();
        createdQueries.add(queryId);
        String queryPool = TEST_POOL;
        QueryKey queryKey = new QueryKey(queryPool, queryId, query.getQueryLogicName());
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryKey, config.getQueries());
        taskStatesCache.updateTaskStates(new TaskStates(queryKey, 10));
        
        TaskKey key = new TaskKey(0, queryPool, UUID.randomUUID().toString(), query.getQueryLogicName());
        try {
            storageService.checkpointTask(key, checkpoint);
            fail("Expected storage service to fail checkpointing an inconsistent query key");
        } catch (Exception e) {
            // expected
        }
        
        Exception expectedException = null;
        
        key = new TaskKey(1, checkpoint.getQueryKey());
        try {
            storageService.checkpointTask(key, checkpoint);
            fail("Expected storage service to fail checkpointing a missing task");
        } catch (Exception e) {
            expectedException = e;
        }
        
        assertTrue(expectedException instanceof IllegalStateException, "Expected IllegalStateException to be thrown, but was " + expectedException);
        
        QueryTask task = storageService.createTask(QueryRequest.Method.NEXT, checkpoint);
        
        QueryTask task2 = storageService.getTask(task.getTaskKey());
        
        assertEquals(task, task2);
        
        // now update the task
        QueryImpl query2 = new QueryImpl();
        query2.setQueryName("update");
        ShardQueryConfiguration config2 = new ShardQueryConfiguration();
        config.setQuery(query2);
        checkpoint = new QueryCheckpoint(queryPool, queryId, query.getQueryLogicName(), config2.getQueries());
        storageService.checkpointTask(task.getTaskKey(), checkpoint);
        
        task2 = storageService.getTask(task.getTaskKey());
        assertEquals(checkpoint, task2.getQueryCheckpoint());
    }
    
    @DirtiesContext
    @Test
    public void testGetAndDeleteTask() throws ParseException, IOException {
        Query query = new QueryImpl();
        query.setQueryLogicName("EventQuery");
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setQuery(query);
        String queryId = UUID.randomUUID().toString();
        createdQueries.add(queryId);
        String queryPool = TEST_POOL;
        QueryKey queryKey = new QueryKey(queryPool, queryId, query.getQueryLogicName());
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryKey, config.getQueries());
        taskStatesCache.updateTaskStates(new TaskStates(queryKey, 10));
        
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
    public void testGetAndDeleteQueryTasks() throws ParseException, IOException {
        Query query = new QueryImpl();
        query.setQueryLogicName("EventQuery");
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setQuery(query);
        String queryId = UUID.randomUUID().toString();
        createdQueries.add(queryId);
        String queryPool = TEST_POOL;
        QueryKey queryKey = new QueryKey(queryPool, queryId, query.getQueryLogicName());
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryKey, config.getQueries());
        taskStatesCache.updateTaskStates(new TaskStates(queryKey, 10));
        
        TaskKey taskKey = storageService.createTask(QueryRequest.Method.NEXT, checkpoint).getTaskKey();
        
        // not get the query tasks
        List<TaskKey> tasks = storageService.getTasks(queryId);
        assertEquals(1, tasks.size());
        QueryTask task = storageService.getTask(tasks.get(0));
        
        assertQueryTask(taskKey, QueryRequest.Method.NEXT, task);
        
        // now delete the query tasks
        storageService.deleteQuery(queryId);
        createdQueries.remove(queryId);
        
        // make sure it deleted
        tasks = storageService.getTasks(queryId);
        assertEquals(0, tasks.size());
    }
    
    @DirtiesContext
    @Test
    public void testGetAndDeleteTypeTasks() throws ParseException, IOException {
        Query query = new QueryImpl();
        query.setQueryLogicName("EventQuery");
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setQuery(query);
        String queryId = UUID.randomUUID().toString();
        createdQueries.add(queryId);
        String queryPool = TEST_POOL;
        QueryKey queryKey = new QueryKey(queryPool, queryId, query.getQueryLogicName());
        QueryStatus queryStatus = new QueryStatus(queryKey);
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryKey, config.getQueries());
        taskStatesCache.updateTaskStates(new TaskStates(queryKey, 10));
        
        storageService.updateQueryStatus(queryStatus);
        TaskKey taskKey = storageService.createTask(QueryRequest.Method.NEXT, checkpoint).getTaskKey();
        
        // now get the query tasks
        List<QueryStatus> queries = storageService.getQueryStatus();
        assertEquals(1, queries.size());
        List<TaskKey> tasks = storageService.getTasks(queries.get(0).getQueryKey().getQueryId());
        assertEquals(1, tasks.size());
        QueryTask task = storageService.getTask(tasks.get(0));
        assertQueryTask(taskKey, QueryRequest.Method.NEXT, task);
        
        // now delete the query tasks
        storageService.deleteQuery(queryId);
        createdQueries.remove(queryId);
        
        // make sure it deleted
        queries = storageService.getQueryStatus();
        assertEquals(0, queries.size());
    }
    
    @DirtiesContext
    @Test
    public void testTaskStateUpdate() throws ParseException {
        Query query = new QueryImpl();
        query.setQueryLogicName("EventQuery");
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setQuery(query);
        String queryId = UUID.randomUUID().toString();
        createdQueries.add(queryId);
        String queryPool = TEST_POOL;
        QueryKey queryKey = new QueryKey(queryPool, queryId, query.getQueryLogicName());
        TaskStates states = new TaskStates(queryKey, 2);
        TaskKey key = new TaskKey(0, queryKey);
        TaskKey key2 = new TaskKey(10, queryKey);
        TaskKey key3 = new TaskKey(20, queryKey);
        states.setState(key.getTaskId(), TaskStates.TASK_STATE.READY);
        
        storageService.updateTaskStates(states);
        
        assertEquals(TaskStates.TASK_STATE.READY, taskStatesCache.getTaskStates(queryId).getState(key.getTaskId()));
        assertTrue(storageService.updateTaskState(key, TaskStates.TASK_STATE.RUNNING));
        assertEquals(TaskStates.TASK_STATE.RUNNING, taskStatesCache.getTaskStates(queryId).getState(key.getTaskId()));
        assertTrue(storageService.updateTaskState(key2, TaskStates.TASK_STATE.RUNNING));
        // should fail trying to run another
        assertFalse(storageService.updateTaskState(key3, TaskStates.TASK_STATE.RUNNING));
        assertTrue(storageService.updateTaskState(key, TaskStates.TASK_STATE.COMPLETED));
        assertEquals(TaskStates.TASK_STATE.COMPLETED, taskStatesCache.getTaskStates(queryId).getState(key.getTaskId()));
        // now this should succeed
        assertTrue(storageService.updateTaskState(key3, TaskStates.TASK_STATE.RUNNING));
    }
    
    @DirtiesContext
    @Test
    public void testQueryStateUpdate() throws ParseException, IOException {
        Query query = new QueryImpl();
        query.setQueryLogicName("EventQuery");
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        String queryPool = TEST_POOL;
        Set<Authorizations> auths = new HashSet<>();
        auths.add(new Authorizations("FOO", "BAR"));
        TaskKey taskKey = storageService.createQuery(queryPool, query, null, auths, 2);
        String queryId = taskKey.getQueryId();
        createdQueries.add(queryId);
        
        assertEquals(QueryStatus.QUERY_STATE.CREATE, storageService.getQueryStatus(queryId).getQueryState());
        storageService.updateQueryStatus(queryId, QueryStatus.QUERY_STATE.CANCEL);
        assertEquals(QueryStatus.QUERY_STATE.CANCEL, storageService.getQueryStatus(queryId).getQueryState());
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
        TaskKey key = storageService.createQuery(queryPool, query, null, auths, 3);
        createdQueries.add(key.getQueryId());
        assertNotNull(key);
        
        // setup a listener for this query's result queue
        QueryResultsListener listener = queueManager.createListener("TestListener", key.getQueryId());
        listeners.add(listener);
        
        // send a result
        Result result = new Result("result1", "Some result");
        queueManager.createPublisher(key.getQueryId()).publish(result);
        
        // receive the message
        Result msg = listener.receive(10000, TimeUnit.MILLISECONDS);
        
        assertNotNull(msg, "Got no result message");
        assertEquals(result.getPayload(), msg.getPayload());
    }
    
    private void assertQueryCreate(String queryId, String queryPool, QueryStatus status) {
        assertEquals(queryId, status.getQueryKey().getQueryId());
        assertEquals(queryPool, status.getQueryKey().getQueryPool());
    }
    
    private void assertQueryCreate(String queryId, String queryPool, Query query, TaskDescription task, QueryStatus queryStatus) {
        assertNotNull(task.getTaskKey());
        assertEquals(queryId, task.getTaskKey().getQueryId());
        assertEquals(queryPool, task.getTaskKey().getQueryPool());
        assertEquals(query.getQuery(), queryStatus.getQuery().getQuery());
        assertEquals(query.getBeginDate(), queryStatus.getQuery().getBeginDate());
        assertEquals(query.getEndDate(), queryStatus.getQuery().getEndDate());
    }
    
    private void assertCreateQueryTask(String queryId, QueryRequest.Method action, QueryTask task) {
        assertEquals(queryId, task.getTaskKey().getQueryId());
        assertEquals(action, task.getAction());
        assertEquals(task.getQueryCheckpoint().getQueryKey().getQueryId(), queryId);
    }
    
    private void assertQueryTask(String queryId, QueryRequest.Method action, QueryTask task) {
        assertEquals(queryId, task.getTaskKey().getQueryId());
        assertEquals(action, task.getAction());
        assertEquals(task.getQueryCheckpoint().getQueryKey().getQueryId(), queryId);
    }
    
    private void assertCreateQueryTask(TaskKey taskKey, QueryRequest.Method action, QueryTask task) {
        assertCreateQueryTask(taskKey.getQueryId(), action, task);
    }
    
    private void assertQueryTask(TaskKey taskKey, QueryRequest.Method action, QueryTask task) {
        assertQueryTask(taskKey.getQueryId(), action, task);
    }
}
