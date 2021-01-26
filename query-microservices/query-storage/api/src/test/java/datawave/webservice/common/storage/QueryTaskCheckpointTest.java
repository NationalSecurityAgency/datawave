package datawave.webservice.common.storage;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

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
    public void testQueryTaskNotification() {
        UUID queryId = UUID.randomUUID();
        UUID taskId = UUID.randomUUID();
        QueryType type = new QueryType("default");
        QueryTaskNotification notification = new QueryTaskNotification(taskId, queryId, type);
        
        assertEquals(queryId, notification.getQueryKey().getQueryId());
        assertEquals(taskId, notification.getTaskId());
        assertEquals(type, notification.getQueryKey().getType());
        
        QueryTaskNotification notification2 = new QueryTaskNotification(taskId, queryId, type);
        assertEquals(notification, notification2);
        assertEquals(notification.hashCode(), notification2.hashCode());
        
        UUID otherId = UUID.randomUUID();
        QueryType otherType = new QueryType("other");
        QueryTaskNotification otherNotification = new QueryTaskNotification(taskId, otherId, type);
        assertNotEquals(otherNotification, notification);
        otherNotification = new QueryTaskNotification(taskId, queryId, otherType);
        assertNotEquals(otherNotification, notification);
        otherNotification = new QueryTaskNotification(otherId, queryId, type);
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
        assertEquals(uuid, notification.getQueryKey().getQueryId());
        assertEquals(type, notification.getQueryKey().getType());
        assertEquals(task.getTaskId(), notification.getTaskId());
        
        UUID uuid2 = UUID.fromString(uuid.toString());
        QueryType type2 = new QueryType("default");
        Map<String,Object> props2 = new HashMap<>();
        props2.put("name", "foo");
        props2.put("query", "foo == bar");
        QueryCheckpoint qcp2 = new QueryCheckpoint(uuid2, type2, props2);
        assertEquals(qcp, qcp2);
        QueryTask task2 = new QueryTask(task.getTaskId(), QueryTask.QUERY_ACTION.CREATE, qcp2);
        
        assertEquals(task, task2);
        assertEquals(task.hashCode(), task2.hashCode());
        
        UUID otherId = UUID.randomUUID();
        QueryCheckpoint otherCp = new QueryCheckpoint(otherId, type, props);
        QueryTask otherTask = new QueryTask(otherId, QueryTask.QUERY_ACTION.CREATE, qcp);
        assertNotEquals(otherTask, task);
        otherTask = new QueryTask(task.getTaskId(), QueryTask.QUERY_ACTION.NEXT, qcp);
        assertNotEquals(otherTask, task);
        otherTask = new QueryTask(task.getTaskId(), QueryTask.QUERY_ACTION.CREATE, otherCp);
        assertNotEquals(otherTask, task);
    }
    
}
