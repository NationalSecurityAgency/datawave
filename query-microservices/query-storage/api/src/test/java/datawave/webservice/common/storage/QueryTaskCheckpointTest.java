package datawave.webservice.common.storage;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class QueryTaskCheckpointTest {
    @Test
    public void testCheckpoint() {
        UUID uuid = UUID.randomUUID();
        QueryType type = new QueryType("default");
        Map<String,Object> props = new HashMap<>();
        props.put("name", "foo");
        props.put("query", "foo == bar");
        QueryCheckpoint qcp = new QueryCheckpoint(uuid, type, props);
        
        assertEquals(type, qcp.getQueryType());
        assertEquals(props, qcp.getProperties());
        assertEquals(uuid, qcp.getQueryId());
        
        UUID uuid2 = UUID.fromString(uuid.toString());
        QueryType type2 = new QueryType("default");
        Map<String,Object> props2 = new HashMap<>();
        props2.put("name", "foo");
        props2.put("query", "foo == bar");
        QueryCheckpoint qcp2 = new QueryCheckpoint(uuid2, type2, props2);
        
        assertEquals(uuid, uuid2);
        assertEquals(type, type2);
        assertEquals(props, props2);
        assertEquals(qcp, qcp2);
        assertEquals(qcp.hashCode(), qcp2.hashCode());
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
        assertEquals(qcp, task.getQueryState());
        
        UUID uuid2 = UUID.fromString(uuid.toString());
        QueryType type2 = new QueryType("default");
        Map<String,Object> props2 = new HashMap<>();
        props2.put("name", "foo");
        props2.put("query", "foo == bar");
        QueryCheckpoint qcp2 = new QueryCheckpoint(uuid2, type2, props2);
        QueryTask task2 = new QueryTask(QueryTask.QUERY_ACTION.CREATE, qcp2);
        
        assertEquals(uuid, uuid2);
        assertEquals(type, type2);
        assertEquals(props, props2);
        assertEquals(qcp, qcp2);
        assertEquals(task, task2);
        assertEquals(task.hashCode(), task2.hashCode());
    }
}
