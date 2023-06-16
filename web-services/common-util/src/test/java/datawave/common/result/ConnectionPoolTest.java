package datawave.common.result;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

import datawave.webservice.common.result.ConnectionPool;
import datawave.webservice.common.result.ConnectionPool.Priority;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

    @Before
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
        Assert.assertEquals("INGEST", p.getPoolName());
        Assert.assertEquals("ADMIN", p.getPriority());
        p = itr.next();
        Assert.assertEquals("INGEST", p.getPoolName());
        Assert.assertEquals("HIGH", p.getPriority());
        p = itr.next();
        Assert.assertEquals("INGEST", p.getPoolName());
        Assert.assertEquals("NORMAL", p.getPriority());
        p = itr.next();
        Assert.assertEquals("INGEST", p.getPoolName());
        Assert.assertEquals("LOW", p.getPriority());
        p = itr.next();
        Assert.assertEquals("WAREHOUSE", p.getPoolName());
        Assert.assertEquals("ADMIN", p.getPriority());
        p = itr.next();
        Assert.assertEquals("WAREHOUSE", p.getPoolName());
        Assert.assertEquals("HIGH", p.getPriority());
        p = itr.next();
        Assert.assertEquals("WAREHOUSE", p.getPoolName());
        Assert.assertEquals("NORMAL", p.getPriority());
        p = itr.next();
        Assert.assertEquals("WAREHOUSE", p.getPoolName());
        Assert.assertEquals("LOW", p.getPriority());
    }
}
