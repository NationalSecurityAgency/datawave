package datawave.core.iterators.querylock;

import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

class ZookeeperQueryLockTest {

    private TestingServer server;
    
    @BeforeEach
    void setUp() throws Exception {
        server = new TestingServer();
    }
    
    @AfterEach
    void tearDown() throws IOException {
        if(server != null) {
            server.close();
        }
    }
    
    /**
     * Verify that a query is correctly evaluated as running/not running before and after starting/stopping the query.
     */
    @Test
    void testQueryLifecycle() throws Exception {
        String queryId = "1";
        ZookeeperQueryLock queryLock = new ZookeeperQueryLock(server.getConnectString(), 120000, queryId);
        Assertions.assertFalse(queryLock.isQueryRunning(), "Query should not be running before query is started");
        
        queryLock.startQuery();
        Assertions.assertTrue(queryLock.isQueryRunning(), "Query should be running after query is started");
        
        queryLock.stopQuery();
        Assertions.assertFalse(queryLock.isQueryRunning(), "Query should not be running after query is stopped");
        
    }
}
