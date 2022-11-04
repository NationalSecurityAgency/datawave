package datawave.common.result;

import datawave.webservice.common.result.ConnectionPool;
import datawave.webservice.common.result.ConnectionPool.Priority;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

/**
 * 
 */
public class ConnectionPoolTest {
    
    List<ConnectionPool> connectionPools = null;
    
    private ConnectionPool createPool(String poolName, String priority) {
        ConnectionPool p = new ConnectionPool();
        p.setPoolName(poolName);
        p.setPriority(priority);
        return p;
    }
    
    @BeforeEach
    public void setup() {
        connectionPools = new LinkedList<>();
        connectionPools.add(createPool("WAREHOUSE", Priority.NORMAL.toString()));
        connectionPools.add(createPool("WAREHOUSE", Priority.HIGH.toString()));
        connectionPools.add(createPool("WAREHOUSE", Priority.ADMIN.toString()));
        connectionPools.add(createPool("WAREHOUSE", Priority.LOW.toString()));
        connectionPools.add(createPool("INGEST", Priority.LOW.toString()));
        connectionPools.add(createPool("INGEST", Priority.ADMIN.toString()));
        connectionPools.add(createPool("INGEST", Priority.NORMAL.toString()));
        connectionPools.add(createPool("INGEST", Priority.HIGH.toString()));
    }
    
    @Test
    public void testOrdering() {
        
        TreeSet<ConnectionPool> pools = new TreeSet<>();
        pools.addAll(connectionPools);
        Iterator<ConnectionPool> itr = pools.iterator();
        ConnectionPool p = null;
        
        p = itr.next();
        Assertions.assertEquals("INGEST", p.getPoolName());
        Assertions.assertEquals("ADMIN", p.getPriority());
        p = itr.next();
        Assertions.assertEquals("INGEST", p.getPoolName());
        Assertions.assertEquals("HIGH", p.getPriority());
        p = itr.next();
        Assertions.assertEquals("INGEST", p.getPoolName());
        Assertions.assertEquals("NORMAL", p.getPriority());
        p = itr.next();
        Assertions.assertEquals("INGEST", p.getPoolName());
        Assertions.assertEquals("LOW", p.getPriority());
        p = itr.next();
        Assertions.assertEquals("WAREHOUSE", p.getPoolName());
        Assertions.assertEquals("ADMIN", p.getPriority());
        p = itr.next();
        Assertions.assertEquals("WAREHOUSE", p.getPoolName());
        Assertions.assertEquals("HIGH", p.getPriority());
        p = itr.next();
        Assertions.assertEquals("WAREHOUSE", p.getPoolName());
        Assertions.assertEquals("NORMAL", p.getPriority());
        p = itr.next();
        Assertions.assertEquals("WAREHOUSE", p.getPoolName());
        Assertions.assertEquals("LOW", p.getPriority());
    }
}
