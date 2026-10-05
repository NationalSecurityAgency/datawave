package datawave.core.common.result;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.core.common.result.ConnectionPool.Priority;

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
        assertEquals("INGEST", p.getPoolName());
        assertEquals("ADMIN", p.getPriority());
        p = itr.next();
        assertEquals("INGEST", p.getPoolName());
        assertEquals("HIGH", p.getPriority());
        p = itr.next();
        assertEquals("INGEST", p.getPoolName());
        assertEquals("NORMAL", p.getPriority());
        p = itr.next();
        assertEquals("INGEST", p.getPoolName());
        assertEquals("LOW", p.getPriority());
        p = itr.next();
        assertEquals("WAREHOUSE", p.getPoolName());
        assertEquals("ADMIN", p.getPriority());
        p = itr.next();
        assertEquals("WAREHOUSE", p.getPoolName());
        assertEquals("HIGH", p.getPriority());
        p = itr.next();
        assertEquals("WAREHOUSE", p.getPoolName());
        assertEquals("NORMAL", p.getPriority());
        p = itr.next();
        assertEquals("WAREHOUSE", p.getPoolName());
        assertEquals("LOW", p.getPriority());
    }
}
