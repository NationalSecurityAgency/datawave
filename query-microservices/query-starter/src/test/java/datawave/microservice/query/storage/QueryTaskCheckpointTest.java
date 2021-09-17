package datawave.microservice.query.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import datawave.microservice.query.remote.QueryRequest;
import datawave.services.query.logic.QueryCheckpoint;
import datawave.services.query.logic.QueryKey;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class QueryTaskCheckpointTest {
    @Test
    public void testQueryKey() {
        String queryId = UUID.randomUUID().toString();
        String queryPool = "default";
        String queryLogic = "EventQuery";
        QueryKey key = new QueryKey(queryPool, queryId, queryLogic);
        assertEquals(queryId, key.getQueryId());
        assertEquals(queryPool, key.getQueryPool());
        
        String queryId2 = queryId;
        String queryPool2 = "default";
        String queryLogic2 = "EventQuery";
        QueryKey key2 = new QueryKey(queryPool2, queryId2, queryLogic2);
        assertEquals(key, key2);
        assertEquals(key.hashCode(), key2.hashCode());
        assertEquals(key.toKey(), key2.toKey());
        
        assertTrue(key.toKey().contains(queryId.toString()));
        assertTrue(key.toKey().contains(queryPool.toString()));
        assertTrue(key.toKey().contains(queryLogic));
        
        String otherId = UUID.randomUUID().toString();
        QueryKey otherKey = new QueryKey(queryPool, otherId, queryLogic);
        assertNotEquals(key, otherKey);
        assertNotEquals(key.toKey(), otherKey.toKey());
        
        String otherPool = "other";
        otherKey = new QueryKey(otherPool, queryId, queryLogic);
        assertNotEquals(key, otherKey);
        assertNotEquals(key.toKey(), otherKey.toKey());
        
        String otherLogic = "EdgeQuery";
        otherKey = new QueryKey(queryPool, queryId, otherLogic);
        assertNotEquals(key, otherKey);
        assertNotEquals(key.toKey(), otherKey.toKey());
    }
    
    @Test
    public void getTaskKey() {
        String queryId = UUID.randomUUID().toString();
        String queryPool = "default";
        String queryLogic = "EventQuery";
        String taskId = UUID.randomUUID().toString();
        QueryRequest.Method action = QueryRequest.Method.CREATE;
        TaskKey key = new TaskKey(taskId, action, queryPool, queryId, queryLogic);
        assertEquals(queryId, key.getQueryId());
        assertEquals(queryPool, key.getQueryPool());
        assertEquals(taskId, key.getTaskId());
        assertEquals(action, key.getAction());
        
        String queryId2 = queryId;
        String queryPool2 = "default";
        String taskId2 = taskId;
        String queryLogic2 = "EventQuery";
        QueryRequest.Method action2 = QueryRequest.Method.CREATE;
        TaskKey key2 = new TaskKey(taskId2, action2, queryPool2, queryId2, queryLogic2);
        assertEquals(key, key2);
        assertEquals(key.hashCode(), key2.hashCode());
        assertEquals(key.toKey(), key2.toKey());
        
        assertTrue(key.toKey().contains(taskId.toString()));
        assertTrue(key.toKey().contains(action.toString()));
        assertTrue(key.toKey().contains(queryId.toString()));
        assertTrue(key.toKey().contains(queryPool.toString()));
        
        String otherId = UUID.randomUUID().toString();
        String otherPool = "other";
        String otherLogic = "EdgeQuery";
        QueryRequest.Method otherAction = QueryRequest.Method.NEXT;
        TaskKey otherKey = new TaskKey(otherId, action, queryPool, queryId, queryLogic);
        assertNotEquals(key, otherKey);
        assertNotEquals(key.toKey(), otherKey.toKey());
        otherKey = new TaskKey(taskId, action, otherPool, queryId, queryLogic);
        assertNotEquals(key, otherKey);
        assertNotEquals(key.toKey(), otherKey.toKey());
        otherKey = new TaskKey(taskId, otherAction, queryPool, queryId, queryLogic);
        assertNotEquals(key, otherKey);
        assertNotEquals(key.toKey(), otherKey.toKey());
        otherKey = new TaskKey(taskId, action, queryPool, otherId, queryLogic);
        assertNotEquals(key, otherKey);
        assertNotEquals(key.toKey(), otherKey.toKey());
        otherKey = new TaskKey(taskId, action, queryPool, queryId, otherLogic);
        assertNotEquals(key, otherKey);
        assertNotEquals(key.toKey(), otherKey.toKey());
    }
    
    @Test
    public void testCheckpoint() {
        String uuid = UUID.randomUUID().toString();
        String queryPool = "default";
        String queryLogic = "EventQuery";
        Map<String,Object> props = new HashMap<>();
        props.put("name", "foo");
        props.put("query", "foo == bar");
        QueryCheckpoint qcp = new QueryCheckpoint(queryPool, uuid, queryLogic, props);
        
        assertEquals(queryPool, qcp.getQueryKey().getQueryPool());
        assertEquals(props, qcp.getProperties());
        assertEquals(uuid, qcp.getQueryKey().getQueryId());
        
        String uuid2 = uuid;
        String queryPool2 = "default";
        String queryLogic2 = "EventQuery";
        Map<String,Object> props2 = new HashMap<>();
        props2.put("name", "foo");
        props2.put("query", "foo == bar");
        assertEquals(props, props2);
        QueryCheckpoint qcp2 = new QueryCheckpoint(queryPool2, uuid2, queryLogic2, props2);
        
        assertEquals(qcp, qcp2);
        assertEquals(qcp.hashCode(), qcp2.hashCode());
        
        String otherId = UUID.randomUUID().toString();
        String otherPool = "other";
        String otherLogic = "EdgeQuery";
        Map<String,Object> otherProps = new HashMap<>();
        otherProps.put("name", "bar");
        QueryCheckpoint otherCp = new QueryCheckpoint(otherPool, uuid, queryLogic, props);
        assertNotEquals(otherCp, qcp);
        otherCp = new QueryCheckpoint(queryPool, otherId, queryLogic, props);
        assertNotEquals(otherCp, qcp);
        otherCp = new QueryCheckpoint(queryPool, uuid, otherLogic, props);
        assertNotEquals(otherCp, qcp);
        otherCp = new QueryCheckpoint(queryPool, uuid, queryLogic, otherProps);
        assertNotEquals(otherCp, qcp);
    }
    
    @Test
    public void testTask() {
        String uuid = UUID.randomUUID().toString();
        String queryPool = "default";
        String queryLogic = "EventQuery";
        Map<String,Object> props = new HashMap<>();
        props.put("name", "foo");
        props.put("query", "foo == bar");
        QueryCheckpoint qcp = new QueryCheckpoint(queryPool, uuid, queryLogic, props);
        QueryTask task = new QueryTask(QueryRequest.Method.CREATE, qcp);
        
        assertEquals(QueryRequest.Method.CREATE, task.getAction());
        assertEquals(qcp, task.getQueryCheckpoint());
        
        String uuid2 = uuid;
        String queryPool2 = "default";
        String queryLogic2 = "EventQuery";
        Map<String,Object> props2 = new HashMap<>();
        props2.put("name", "foo");
        props2.put("query", "foo == bar");
        QueryCheckpoint qcp2 = new QueryCheckpoint(queryPool2, uuid2, queryLogic2, props2);
        assertEquals(qcp, qcp2);
        QueryTask task2 = new QueryTask(task.getTaskKey().getTaskId(), QueryRequest.Method.CREATE, qcp2);
        
        assertEquals(task, task2);
        assertEquals(task.hashCode(), task2.hashCode());
        assertEquals(task.getTaskKey(), task2.getTaskKey());
        
        String otherId = UUID.randomUUID().toString();
        QueryCheckpoint otherCp = new QueryCheckpoint(queryPool, otherId, queryLogic, props);
        QueryTask otherTask = new QueryTask(otherId, QueryRequest.Method.CREATE, qcp);
        assertNotEquals(otherTask, task);
        assertNotEquals(otherTask.getTaskKey(), task.getTaskKey());
        otherTask = new QueryTask(task.getTaskKey().getTaskId(), QueryRequest.Method.NEXT, qcp);
        assertNotEquals(otherTask, task);
        assertNotEquals(otherTask.getTaskKey(), task.getTaskKey());
        otherTask = new QueryTask(task.getTaskKey().getTaskId(), QueryRequest.Method.CREATE, otherCp);
        assertNotEquals(otherTask, task);
        assertNotEquals(otherTask.getTaskKey(), task.getTaskKey());
    }
    
    @Test
    public void testTaskDescription() throws JsonProcessingException {
        TaskKey key = new TaskKey(UUID.randomUUID().toString(), QueryRequest.Method.CREATE, "default", UUID.randomUUID().toString(), "EventQuery");
        Map<String,String> props = new HashMap<>();
        props.put("name", "foo");
        props.put("query", "foo == bar");
        TaskDescription desc = new TaskDescription(key, props);
        
        assertEquals(key, desc.getTaskKey());
        assertEquals(QueryRequest.Method.CREATE, desc.getTaskKey().getAction());
        assertEquals(props, desc.getParameters());
        
        String json = new ObjectMapper().writeValueAsString(desc);
        TaskDescription desc2 = new ObjectMapper().readerFor(TaskDescription.class).readValue(json);
        assertEquals(desc, desc2);
        assertEquals(desc.hashCode(), desc2.hashCode());
        
        TaskKey key2 = new TaskKey(key.getTaskId(), key.getAction(), key.getQueryPool(), key.getQueryId(), key.getQueryLogic());
        Map<String,String> props2 = new HashMap<>();
        props2.put("name", "foo");
        props2.put("query", "foo == bar");
        desc2 = new TaskDescription(key2, props2);
        
        assertEquals(desc, desc2);
        assertEquals(desc.hashCode(), desc.hashCode());
        
        TaskKey otherKey = new TaskKey(UUID.randomUUID().toString(), key.getAction(), key.getQueryPool(), key.getQueryId(), key.getQueryLogic());
        Map<String,String> otherProps = new HashMap<>();
        otherProps.put("name", "bar");
        otherProps.put("query", "foo == bar");
        TaskDescription otherDesc = new TaskDescription(otherKey, props);
        assertNotEquals(otherDesc, desc);
        otherDesc = new TaskDescription(key, otherProps);
        assertNotEquals(otherDesc, desc);
    }
    
    @Test
    public void testQueryState() throws JsonProcessingException {
        String uuid = UUID.randomUUID().toString();
        String queryPool = "default";
        String queryLogic = "EventQuery";
        QueryStatus queryStatus = new QueryStatus(new QueryKey(queryPool, uuid, queryLogic));
        TaskStates tasks = new TaskStates(new QueryKey(queryPool, uuid, queryLogic), 10);
        Map<TaskStates.TASK_STATE,Set<TaskKey>> states = new HashMap<>();
        QueryRequest.Method action = QueryRequest.Method.CREATE;
        states.put(TaskStates.TASK_STATE.READY, new HashSet<>());
        states.get(TaskStates.TASK_STATE.READY).add(new TaskKey(UUID.randomUUID().toString(), action, queryPool, uuid, queryLogic));
        states.get(TaskStates.TASK_STATE.READY).add(new TaskKey(UUID.randomUUID().toString(), action, queryPool, uuid, queryLogic));
        tasks.setTaskStates(states);
        QueryState state = new QueryState(queryStatus, tasks);
        
        assertEquals(uuid, state.getQueryStatus().getQueryKey().getQueryId());
        assertEquals(queryPool, state.getQueryStatus().getQueryKey().getQueryPool());
        assertEquals(queryLogic, state.getQueryStatus().getQueryKey().getQueryLogic());
        assertEquals(tasks, state.getTaskStates());
        
        String json = new ObjectMapper().writeValueAsString(state);
        QueryState state2 = new ObjectMapper().readerFor(QueryState.class).readValue(json);
        assertEquals(state, state2);
        assertEquals(state.hashCode(), state2.hashCode());
        
        String uuid2 = uuid;
        String queryPool2 = "default";
        String queryLogic2 = "EventQuery";
        QueryStatus queryStatus2 = new QueryStatus(new QueryKey(queryPool2, uuid2, queryLogic2));
        TaskStates tasks2 = new TaskStates(new QueryKey(queryPool, uuid, queryLogic), 10);
        tasks2.setTaskStates(new HashMap<>(states));
        state2 = new QueryState(queryStatus2, tasks2);
        
        assertEquals(state, state2);
        assertEquals(state.hashCode(), state2.hashCode());
        
        String otherId = UUID.randomUUID().toString();
        String otherPool = "other";
        String otherLogic = "EdgeQuery";
        QueryStatus otherProperties = new QueryStatus(new QueryKey(otherPool, otherId, otherLogic));
        TaskStates otherTasks = new TaskStates(new QueryKey(queryPool, uuid, queryLogic), 10);
        Map<TaskStates.TASK_STATE,Set<TaskKey>> otherStates = new HashMap<>();
        otherStates.put(TaskStates.TASK_STATE.READY, new HashSet<>());
        otherStates.get(TaskStates.TASK_STATE.READY).add(new TaskKey(UUID.randomUUID().toString(), action, queryPool, uuid, queryLogic));
        otherStates.get(TaskStates.TASK_STATE.READY).add(new TaskKey(UUID.randomUUID().toString(), action, queryPool, uuid, queryLogic));
        otherTasks.setTaskStates(otherStates);
        QueryState otherState = new QueryState(otherProperties, tasks);
        assertNotEquals(otherState, state);
        otherState = new QueryState(queryStatus, otherTasks);
        assertNotEquals(otherState, state);
    }
}
