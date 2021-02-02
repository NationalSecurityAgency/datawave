package datawave.microservice.common.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class QueryTaskCheckpointTest {
    @Test
    public void testQueryType() {
        QueryType type = new QueryType("default");
        assertEquals("default", type.getType());
        
        QueryType type2 = new QueryType("default");
        assertEquals(type, type2);
        assertEquals(type.hashCode(), type2.hashCode());
        
        QueryType otherType = new QueryType("other");
        assertNotEquals(otherType, type);
    }
    
    @Test
    public void testQueryKey() {
        UUID queryId = UUID.randomUUID();
        QueryType type = new QueryType("default");
        QueryKey key = new QueryKey(type, queryId);
        assertEquals(queryId, key.getQueryId());
        assertEquals(type, key.getType());
        
        UUID queryId2 = UUID.fromString(queryId.toString());
        QueryType type2 = new QueryType("default");
        QueryKey key2 = new QueryKey(type2, queryId2);
        assertEquals(key, key2);
        assertEquals(key.hashCode(), key2.hashCode());
        assertEquals(key.toKey(), key2.toKey());
        
        assertTrue(key.toKey().contains(queryId.toString()));
        assertTrue(key.toKey().contains(type.toString()));
        
        UUID otherId = UUID.randomUUID();
        QueryKey otherKey = new QueryKey(type, otherId);
        assertNotEquals(key, otherKey);
        assertNotEquals(key.toKey(), otherKey.toKey());
        
        QueryType otherType = new QueryType("other");
        otherKey = new QueryKey(otherType, queryId);
        assertNotEquals(key, otherKey);
        assertNotEquals(key.toKey(), otherKey.toKey());
    }
    
    @Test
    public void getTaskKey() {
        UUID queryId = UUID.randomUUID();
        QueryType type = new QueryType("default");
        UUID taskId = UUID.randomUUID();
        TaskKey key = new TaskKey(taskId, type, queryId);
        assertEquals(queryId, key.getQueryId());
        assertEquals(type, key.getType());
        assertEquals(taskId, key.getTaskId());
        
        UUID queryId2 = UUID.fromString(queryId.toString());
        QueryType type2 = new QueryType("default");
        UUID taskId2 = UUID.fromString(taskId.toString());
        TaskKey key2 = new TaskKey(taskId2, type2, queryId2);
        assertEquals(key, key2);
        assertEquals(key.hashCode(), key2.hashCode());
        assertEquals(key.toKey(), key2.toKey());
        
        assertTrue(key.toKey().contains(taskId.toString()));
        assertTrue(key.toKey().contains(queryId.toString()));
        assertTrue(key.toKey().contains(type.toString()));
        
        UUID otherId = UUID.randomUUID();
        QueryType otherType = new QueryType("other");
        TaskKey otherKey = new TaskKey(otherId, type, queryId);
        assertNotEquals(key, otherKey);
        assertNotEquals(key.toKey(), otherKey.toKey());
        otherKey = new TaskKey(taskId, otherType, queryId);
        assertNotEquals(key, otherKey);
        assertNotEquals(key.toKey(), otherKey.toKey());
        otherKey = new TaskKey(taskId, type, otherId);
        assertNotEquals(key, otherKey);
        assertNotEquals(key.toKey(), otherKey.toKey());
    }
    
    @Test
    public void testQueryTaskNotification() {
        UUID queryId = UUID.randomUUID();
        UUID taskId = UUID.randomUUID();
        QueryType type = new QueryType("default");
        TaskKey taskKey = new TaskKey(taskId, type, queryId);
        QueryTaskNotification notification = new QueryTaskNotification(taskKey, QueryTask.QUERY_ACTION.CREATE);
        
        assertEquals(taskKey, notification.getTaskKey());
        
        UUID taskId2 = UUID.fromString(taskId.toString());
        UUID queryId2 = UUID.fromString(queryId.toString());
        QueryType type2 = new QueryType("default");
        TaskKey taskKey2 = new TaskKey(taskId2, type2, queryId2);
        QueryTaskNotification notification2 = new QueryTaskNotification(taskKey2, QueryTask.QUERY_ACTION.CREATE);
        assertEquals(notification, notification2);
        assertEquals(notification.hashCode(), notification2.hashCode());
        
        UUID otherId = UUID.randomUUID();
        TaskKey otherKey = new TaskKey(otherId, type, queryId);
        QueryTaskNotification otherNotification = new QueryTaskNotification(otherKey, QueryTask.QUERY_ACTION.CREATE);
        assertNotEquals(otherNotification, notification);
        otherNotification = new QueryTaskNotification(taskKey, QueryTask.QUERY_ACTION.NEXT);
        assertNotEquals(otherNotification, notification);
    }
    
    @Test
    public void testCheckpoint() {
        UUID uuid = UUID.randomUUID();
        QueryType type = new QueryType("default");
        Map<String,Object> props = new HashMap<>();
        props.put("name", "foo");
        props.put("query", "foo == bar");
        QueryCheckpoint qcp = new QueryCheckpoint(uuid, type, props);
        
        assertEquals(type, qcp.getQueryKey().getType());
        assertEquals(props, qcp.getProperties());
        assertEquals(uuid, qcp.getQueryKey().getQueryId());
        
        UUID uuid2 = UUID.fromString(uuid.toString());
        QueryType type2 = new QueryType("default");
        Map<String,Object> props2 = new HashMap<>();
        props2.put("name", "foo");
        props2.put("query", "foo == bar");
        assertEquals(props, props2);
        QueryCheckpoint qcp2 = new QueryCheckpoint(uuid2, type2, props2);
        
        assertEquals(qcp, qcp2);
        assertEquals(qcp.hashCode(), qcp2.hashCode());
        
        UUID otherId = UUID.randomUUID();
        QueryType otherType = new QueryType("other");
        Map<String,Object> otherProps = new HashMap<>();
        otherProps.put("name", "bar");
        QueryCheckpoint otherCp = new QueryCheckpoint(otherId, type, props);
        assertNotEquals(otherCp, qcp);
        otherCp = new QueryCheckpoint(uuid, otherType, props);
        assertNotEquals(otherCp, qcp);
        otherCp = new QueryCheckpoint(uuid, type, otherProps);
        assertNotEquals(otherCp, qcp);
    }
    
    @Test
    public void testTask() {
        UUID uuid = UUID.randomUUID();
        QueryType type = new QueryType("default");
        Map<String,Object> props = new HashMap<>();
        props.put("name", "foo");
        props.put("query", "foo == bar");
        QueryCheckpoint qcp = new QueryCheckpoint(uuid, type, props);
        QueryTask task = new QueryTask(QueryTask.QUERY_ACTION.CREATE, qcp);
        
        assertEquals(QueryTask.QUERY_ACTION.CREATE, task.getAction());
        assertEquals(qcp, task.getQueryCheckpoint());
        
        QueryTaskNotification notification = task.getNotification();
        assertEquals(task.getTaskKey(), notification.getTaskKey());
        assertEquals(QueryTask.QUERY_ACTION.CREATE, notification.getAction());
        
        UUID uuid2 = UUID.fromString(uuid.toString());
        QueryType type2 = new QueryType("default");
        Map<String,Object> props2 = new HashMap<>();
        props2.put("name", "foo");
        props2.put("query", "foo == bar");
        QueryCheckpoint qcp2 = new QueryCheckpoint(uuid2, type2, props2);
        assertEquals(qcp, qcp2);
        QueryTask task2 = new QueryTask(task.getTaskKey().getTaskId(), QueryTask.QUERY_ACTION.CREATE, qcp2);
        
        assertEquals(task, task2);
        assertEquals(task.hashCode(), task2.hashCode());
        assertEquals(task.getTaskKey(), task2.getTaskKey());
        
        UUID otherId = UUID.randomUUID();
        QueryCheckpoint otherCp = new QueryCheckpoint(otherId, type, props);
        QueryTask otherTask = new QueryTask(otherId, QueryTask.QUERY_ACTION.CREATE, qcp);
        assertNotEquals(otherTask, task);
        assertNotEquals(otherTask.getTaskKey(), task.getTaskKey());
        otherTask = new QueryTask(task.getTaskKey().getTaskId(), QueryTask.QUERY_ACTION.NEXT, qcp);
        assertNotEquals(otherTask, task);
        assertEquals(otherTask.getTaskKey(), task.getTaskKey()); // action should not affect the key
        otherTask = new QueryTask(task.getTaskKey().getTaskId(), QueryTask.QUERY_ACTION.CREATE, otherCp);
        assertNotEquals(otherTask, task);
        assertNotEquals(otherTask.getTaskKey(), task.getTaskKey());
    }
    
    @Test
    public void testTaskDescription() throws JsonProcessingException {
        TaskKey key = new TaskKey(UUID.randomUUID(), new QueryType("default"), UUID.randomUUID());
        Map<String,String> props = new HashMap<>();
        props.put("name", "foo");
        props.put("query", "foo == bar");
        TaskDescription desc = new TaskDescription(key, QueryTask.QUERY_ACTION.CREATE, props);
        
        assertEquals(key, desc.getTaskKey());
        assertEquals(QueryTask.QUERY_ACTION.CREATE, desc.getAction());
        assertEquals(props, desc.getParameters());
        
        String json = new ObjectMapper().writeValueAsString(desc);
        TaskDescription desc2 = new ObjectMapper().readerFor(TaskDescription.class).readValue(json);
        assertEquals(desc, desc2);
        assertEquals(desc.hashCode(), desc2.hashCode());
        
        TaskKey key2 = new TaskKey(key.getTaskId(), key.getType(), key.getQueryId());
        Map<String,String> props2 = new HashMap<>();
        props2.put("name", "foo");
        props2.put("query", "foo == bar");
        desc2 = new TaskDescription(key2, QueryTask.QUERY_ACTION.CREATE, props2);
        
        assertEquals(desc, desc2);
        assertEquals(desc.hashCode(), desc.hashCode());
        
        TaskKey otherKey = new TaskKey(UUID.randomUUID(), key.getType(), key.getQueryId());
        Map<String,String> otherProps = new HashMap<>();
        otherProps.put("name", "bar");
        otherProps.put("query", "foo == bar");
        TaskDescription otherDesc = new TaskDescription(otherKey, QueryTask.QUERY_ACTION.CREATE, props);
        assertNotEquals(otherDesc, desc);
        otherDesc = new TaskDescription(key, QueryTask.QUERY_ACTION.NEXT, props);
        assertNotEquals(otherDesc, desc);
        otherDesc = new TaskDescription(key, QueryTask.QUERY_ACTION.CREATE, otherProps);
        assertNotEquals(otherDesc, desc);
    }
    
    @Test
    public void testQueryState() throws JsonProcessingException {
        UUID uuid = UUID.randomUUID();
        QueryType type = new QueryType("default");
        Map<QueryTask.QUERY_ACTION,Integer> props = new HashMap<>();
        props.put(QueryTask.QUERY_ACTION.CREATE, 1);
        props.put(QueryTask.QUERY_ACTION.NEXT, 10);
        QueryState state = new QueryState(uuid, type, props);
        
        assertEquals(uuid, state.getQueryId());
        assertEquals(type, state.getQueryType());
        assertEquals(props, state.getTaskCounts());
        
        String json = new ObjectMapper().writeValueAsString(state);
        QueryState state2 = new ObjectMapper().readerFor(QueryState.class).readValue(json);
        assertEquals(state, state2);
        assertEquals(state.hashCode(), state2.hashCode());
        
        UUID uuid2 = UUID.fromString(uuid.toString());
        QueryType type2 = new QueryType("default");
        Map<QueryTask.QUERY_ACTION,Integer> props2 = new HashMap<>();
        props2.put(QueryTask.QUERY_ACTION.CREATE, 1);
        props2.put(QueryTask.QUERY_ACTION.NEXT, 10);
        state2 = new QueryState(uuid2, type2, props2);
        
        assertEquals(state, state2);
        assertEquals(state.hashCode(), state2.hashCode());
        
        UUID otherId = UUID.randomUUID();
        QueryType otherType = new QueryType("other");
        Map<QueryTask.QUERY_ACTION,Integer> otherProps = new HashMap<>();
        otherProps.put(QueryTask.QUERY_ACTION.CREATE, 1);
        otherProps.put(QueryTask.QUERY_ACTION.NEXT, 11);
        QueryState otherState = new QueryState(otherId, type, props);
        assertNotEquals(otherState, state);
        otherState = new QueryState(uuid, otherType, props);
        assertNotEquals(otherState, state);
        otherState = new QueryState(uuid, type, otherProps);
        assertNotEquals(otherState, state);
    }
}
