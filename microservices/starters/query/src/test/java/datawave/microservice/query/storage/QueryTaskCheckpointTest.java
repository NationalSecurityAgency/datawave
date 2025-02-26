package datawave.microservice.query.storage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;

import datawave.core.query.configuration.QueryData;
import datawave.core.query.logic.QueryCheckpoint;
import datawave.core.query.logic.QueryKey;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.query.remote.QueryRequest;
import datawave.query.config.ShardQueryConfiguration;

public class QueryTaskCheckpointTest {
    @Test
    public void testQueryKey() {
        String queryId = UUID.randomUUID().toString();
        String queryPool = "default";
        String queryLogic = "EventQuery";
        QueryKey key = new QueryKey(queryPool, queryId, queryLogic);
        Assertions.assertEquals(queryId, key.getQueryId());
        Assertions.assertEquals(queryPool, key.getQueryPool());
        
        String queryId2 = queryId;
        String queryPool2 = "default";
        String queryLogic2 = "EventQuery";
        QueryKey key2 = new QueryKey(queryPool2, queryId2, queryLogic2);
        Assertions.assertEquals(key, key2);
        Assertions.assertEquals(key.hashCode(), key2.hashCode());
        Assertions.assertEquals(key.toKey(), key2.toKey());
        
        Assertions.assertTrue(key.toKey().contains(queryId.toString()));
        Assertions.assertTrue(key.toKey().contains(queryPool.toString()));
        Assertions.assertTrue(key.toKey().contains(queryLogic));
        
        String otherId = UUID.randomUUID().toString();
        QueryKey otherKey = new QueryKey(queryPool, otherId, queryLogic);
        Assertions.assertNotEquals(key, otherKey);
        Assertions.assertNotEquals(key.toKey(), otherKey.toKey());
        
        String otherPool = "other";
        otherKey = new QueryKey(otherPool, queryId, queryLogic);
        Assertions.assertNotEquals(key, otherKey);
        Assertions.assertNotEquals(key.toKey(), otherKey.toKey());
        
        String otherLogic = "EdgeQuery";
        otherKey = new QueryKey(queryPool, queryId, otherLogic);
        Assertions.assertNotEquals(key, otherKey);
        Assertions.assertNotEquals(key.toKey(), otherKey.toKey());
    }
    
    @Test
    public void getTaskKey() {
        String queryId = UUID.randomUUID().toString();
        String queryPool = "default";
        String queryLogic = "EventQuery";
        int taskId = new Random().nextInt(Integer.MAX_VALUE);
        TaskKey key = new TaskKey(taskId, queryPool, queryId, queryLogic);
        Assertions.assertEquals(queryId, key.getQueryId());
        Assertions.assertEquals(queryPool, key.getQueryPool());
        Assertions.assertEquals(taskId, key.getTaskId());
        
        String queryId2 = queryId;
        String queryPool2 = "default";
        int taskId2 = taskId;
        String queryLogic2 = "EventQuery";
        TaskKey key2 = new TaskKey(taskId2, queryPool2, queryId2, queryLogic2);
        Assertions.assertEquals(key, key2);
        Assertions.assertEquals(key.hashCode(), key2.hashCode());
        Assertions.assertEquals(key.toKey(), key2.toKey());
        
        Assertions.assertTrue(key.toKey().contains(Integer.toString(taskId)));
        Assertions.assertTrue(key.toKey().contains(queryId));
        Assertions.assertTrue(key.toKey().contains(queryPool));
        
        String otherQqueryId = UUID.randomUUID().toString();
        int otherId = new Random().nextInt(Integer.MAX_VALUE);
        String otherPool = "other";
        String otherLogic = "EdgeQuery";
        TaskKey otherKey = new TaskKey(otherId, queryPool, queryId, queryLogic);
        Assertions.assertNotEquals(key, otherKey);
        Assertions.assertNotEquals(key.toKey(), otherKey.toKey());
        otherKey = new TaskKey(taskId, otherPool, queryId, queryLogic);
        Assertions.assertNotEquals(key, otherKey);
        Assertions.assertNotEquals(key.toKey(), otherKey.toKey());
        otherKey = new TaskKey(taskId + 1, queryPool, queryId, queryLogic);
        Assertions.assertNotEquals(key, otherKey);
        Assertions.assertNotEquals(key.toKey(), otherKey.toKey());
        otherKey = new TaskKey(taskId, queryPool, otherQqueryId, queryLogic);
        Assertions.assertNotEquals(key, otherKey);
        Assertions.assertNotEquals(key.toKey(), otherKey.toKey());
        otherKey = new TaskKey(taskId, queryPool, queryId, otherLogic);
        Assertions.assertNotEquals(key, otherKey);
        Assertions.assertNotEquals(key.toKey(), otherKey.toKey());
    }
    
    @Test
    public void testCheckpoint() {
        String uuid = UUID.randomUUID().toString();
        String queryPool = "default";
        String queryLogic = "EventQuery";
        QueryImpl query = new QueryImpl();
        query.setQueryName("foo");
        query.setQuery("foo == bar");
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setQuery(query);
        QueryData queryData = new QueryData("table", "logic", Collections.singletonList(new Range()), Collections.emptyList(),
                        Collections.singletonList(new IteratorSetting(10, "test", "test")));
        config.setQueries(Collections.singletonList(queryData));
        QueryCheckpoint qcp = new QueryCheckpoint(queryPool, uuid, queryLogic, config.getQueries());
        
        Assertions.assertEquals(queryPool, qcp.getQueryKey().getQueryPool());
        Assertions.assertEquals(Collections.singletonList(queryData), qcp.getQueries());
        Assertions.assertEquals(uuid, qcp.getQueryKey().getQueryId());
        
        String uuid2 = uuid;
        String queryPool2 = "default";
        String queryLogic2 = "EventQuery";
        QueryImpl query2 = new QueryImpl();
        query2.setQueryName("foo");
        query2.setQuery("foo == bar");
        ShardQueryConfiguration config2 = new ShardQueryConfiguration();
        config2.setQuery(query2);
        config2.setQueries(Collections.singletonList(queryData));
        Assertions.assertEquals(config, config2);
        QueryCheckpoint qcp2 = new QueryCheckpoint(queryPool2, uuid2, queryLogic2, config2.getQueries());
        
        Assertions.assertEquals(qcp, qcp2);
        Assertions.assertEquals(qcp.hashCode(), qcp2.hashCode());
        
        String otherId = UUID.randomUUID().toString();
        String otherPool = "other";
        String otherLogic = "EdgeQuery";
        QueryImpl otherQuery = new QueryImpl();
        otherQuery.setQueryName("bar");
        ShardQueryConfiguration otherConfig = new ShardQueryConfiguration();
        otherConfig.setQuery(otherQuery);
        QueryCheckpoint otherCp = new QueryCheckpoint(otherPool, uuid, queryLogic, config.getQueries());
        Assertions.assertNotEquals(otherCp, qcp);
        otherCp = new QueryCheckpoint(queryPool, otherId, queryLogic, config.getQueries());
        Assertions.assertNotEquals(otherCp, qcp);
        otherCp = new QueryCheckpoint(queryPool, uuid, otherLogic, config.getQueries());
        Assertions.assertNotEquals(otherCp, qcp);
        otherCp = new QueryCheckpoint(queryPool, uuid, queryLogic, otherConfig.getQueries());
        Assertions.assertNotEquals(otherCp, qcp);
    }
    
    @Test
    public void testTask() {
        String uuid = UUID.randomUUID().toString();
        String queryPool = "default";
        String queryLogic = "EventQuery";
        QueryImpl query = new QueryImpl();
        query.setQueryName("foo");
        query.setQuery("foo == bar");
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setQuery(query);
        QueryData queryData = new QueryData("table", "logic", Collections.singletonList(new Range()), Collections.emptyList(),
                        Collections.singletonList(new IteratorSetting(10, "test", "test")));
        config.setQueries(Collections.singletonList(queryData));
        QueryCheckpoint qcp = new QueryCheckpoint(queryPool, uuid, queryLogic, config.getQueries());
        QueryTask task = new QueryTask(0, QueryRequest.Method.CREATE, qcp);
        
        Assertions.assertEquals(QueryRequest.Method.CREATE, task.getAction());
        Assertions.assertEquals(qcp, task.getQueryCheckpoint());
        
        String uuid2 = uuid;
        String queryPool2 = "default";
        String queryLogic2 = "EventQuery";
        QueryImpl query2 = new QueryImpl();
        query2.setQueryName("foo");
        query2.setQuery("foo == bar");
        ShardQueryConfiguration config2 = new ShardQueryConfiguration();
        config2.setQuery(query2);
        config2.setQueries(Collections.singletonList(queryData));
        QueryCheckpoint qcp2 = new QueryCheckpoint(queryPool2, uuid2, queryLogic2, config2.getQueries());
        Assertions.assertEquals(qcp, qcp2);
        QueryTask task2 = new QueryTask(task.getTaskKey().getTaskId(), QueryRequest.Method.CREATE, qcp2);
        
        Assertions.assertEquals(task, task2);
        Assertions.assertEquals(task.hashCode(), task2.hashCode());
        Assertions.assertEquals(task.getTaskKey(), task2.getTaskKey());
        
        String otherId = UUID.randomUUID().toString();
        QueryCheckpoint otherCp = new QueryCheckpoint(queryPool, otherId, queryLogic, config.getQueries());
        QueryTask otherTask = new QueryTask(1, QueryRequest.Method.CREATE, qcp);
        Assertions.assertNotEquals(otherTask, task);
        Assertions.assertNotEquals(otherTask.getTaskKey(), task.getTaskKey());
        otherTask = new QueryTask(task.getTaskKey().getTaskId(), QueryRequest.Method.NEXT, qcp);
        Assertions.assertNotEquals(otherTask, task);
        Assertions.assertEquals(otherTask.getTaskKey(), task.getTaskKey());
        otherTask = new QueryTask(task.getTaskKey().getTaskId(), QueryRequest.Method.CREATE, otherCp);
        Assertions.assertNotEquals(otherTask, task);
        Assertions.assertNotEquals(otherTask.getTaskKey(), task.getTaskKey());
    }
    
    @Test
    public void testTaskDescription() throws JsonProcessingException {
        TaskKey key = new TaskKey(0, "default", UUID.randomUUID().toString(), "EventQuery");
        QueryImpl query = new QueryImpl();
        UUID queryId = UUID.randomUUID();
        query.setId(queryId);
        query.setQueryName("foo");
        query.setQuery("foo == bar");
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setQuery(query);
        QueryData queryData = new QueryData("table", "logic",
                        new HashSet<>(Collections
                                        .singleton(new Range(new Key("row1", "cf1", "cq1", "(FOO)"), true, new Key("row2", "cf2", "cq2", "(BAR)"), false))),
                        new HashSet<>(),
                        new ArrayList<>(Collections.singletonList(new IteratorSetting(10, "test", "test", Collections.singletonMap("key", "value")))));
        config.setQueries(Collections.singletonList(queryData));
        TaskDescription desc = new TaskDescription(key, config.getQueries());
        
        Assertions.assertEquals(key, desc.getTaskKey());
        Assertions.assertEquals(config.getQueries(), desc.getQueries());
        
        String json = new ObjectMapper().registerModule(new GuavaModule()).writeValueAsString(desc);
        TaskDescription desc2 = new ObjectMapper().registerModule(new GuavaModule()).readerFor(TaskDescription.class).readValue(json);
        // NOTE: This comparison is very brittle. It breaks if the collection types don't match (i.e. arraylist vs set vs singletonlist, etc.)
        Assertions.assertEquals(desc, desc2);
        Assertions.assertEquals(desc.hashCode(), desc2.hashCode());
        
        TaskKey key2 = new TaskKey(key.getTaskId(), key.getQueryPool(), key.getQueryId(), key.getQueryLogic());
        QueryImpl query2 = new QueryImpl();
        query2.setId(queryId);
        query2.setQueryName("foo");
        query2.setQuery("foo == bar");
        ShardQueryConfiguration config2 = new ShardQueryConfiguration();
        config2.setQuery(query2);
        config2.setQueries(Collections.singletonList(queryData));
        desc2 = new TaskDescription(key2, config2.getQueries());
        
        Assertions.assertEquals(desc, desc2);
        Assertions.assertEquals(desc.hashCode(), desc.hashCode());
        
        TaskKey otherKey = new TaskKey(key.getTaskId() + 1, key.getQueryPool(), key.getQueryId(), key.getQueryLogic());
        QueryImpl otherQuery = new QueryImpl();
        otherQuery.setQueryName("bar");
        otherQuery.setQuery("foo == bar");
        ShardQueryConfiguration otherConfig = new ShardQueryConfiguration();
        otherConfig.setQuery(otherQuery);
        TaskDescription otherDesc = new TaskDescription(otherKey, config.getQueries());
        Assertions.assertNotEquals(otherDesc, desc);
        otherDesc = new TaskDescription(key, otherConfig.getQueries());
        Assertions.assertNotEquals(otherDesc, desc);
    }
    
    @Test
    public void testQueryState() throws JsonProcessingException {
        String uuid = UUID.randomUUID().toString();
        String queryPool = "default";
        String queryLogic = "EventQuery";
        QueryStatus queryStatus = new QueryStatus(new QueryKey(queryPool, uuid, queryLogic));
        TaskStates tasks = new TaskStates(new QueryKey(queryPool, uuid, queryLogic), 10);
        Map<TaskStates.TASK_STATE,SortedSet<Integer>> states = new HashMap<>();
        QueryRequest.Method action = QueryRequest.Method.CREATE;
        states.put(TaskStates.TASK_STATE.READY, new TreeSet<>());
        states.get(TaskStates.TASK_STATE.READY).add(0);
        states.get(TaskStates.TASK_STATE.READY).add(1);
        tasks.setTaskStates(states);
        QueryState state = new QueryState(queryStatus, tasks);
        
        Assertions.assertEquals(uuid, state.getQueryStatus().getQueryKey().getQueryId());
        Assertions.assertEquals(queryPool, state.getQueryStatus().getQueryKey().getQueryPool());
        Assertions.assertEquals(queryLogic, state.getQueryStatus().getQueryKey().getQueryLogic());
        Assertions.assertEquals(tasks, state.getTaskStates());
        
        String uuid2 = uuid;
        String queryPool2 = "default";
        String queryLogic2 = "EventQuery";
        QueryStatus queryStatus2 = new QueryStatus(new QueryKey(queryPool2, uuid2, queryLogic2));
        TaskStates tasks2 = new TaskStates(new QueryKey(queryPool, uuid, queryLogic), 10);
        tasks2.setTaskStates(new HashMap<>(states));
        QueryState state2 = new QueryState(queryStatus2, tasks2);
        
        Assertions.assertEquals(state, state2);
        Assertions.assertEquals(state.hashCode(), state2.hashCode());
        
        String otherId = UUID.randomUUID().toString();
        String otherPool = "other";
        String otherLogic = "EdgeQuery";
        QueryStatus otherProperties = new QueryStatus(new QueryKey(otherPool, otherId, otherLogic));
        TaskStates otherTasks = new TaskStates(new QueryKey(queryPool, uuid, queryLogic), 10);
        Map<TaskStates.TASK_STATE,SortedSet<Integer>> otherStates = new HashMap<>();
        otherStates.put(TaskStates.TASK_STATE.READY, new TreeSet());
        otherStates.get(TaskStates.TASK_STATE.READY).add(2);
        otherStates.get(TaskStates.TASK_STATE.READY).add(3);
        otherTasks.setTaskStates(otherStates);
        QueryState otherState = new QueryState(otherProperties, tasks);
        Assertions.assertNotEquals(otherState, state);
        otherState = new QueryState(queryStatus, otherTasks);
        Assertions.assertNotEquals(otherState, state);
    }
}
