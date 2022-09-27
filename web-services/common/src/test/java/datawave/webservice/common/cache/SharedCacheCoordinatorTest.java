package datawave.webservice.common.cache;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.SharedCountListener;
import org.apache.curator.framework.recipes.shared.SharedCountReader;
import org.apache.curator.framework.recipes.shared.VersionedValue;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.QuorumConfigBuilder;
import org.apache.curator.test.TestingZooKeeperServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests functionality in the {@link SharedCacheCoordinator}.
 */
public class SharedCacheCoordinatorTest {
    private TestingZooKeeperServer testingZooKeeperServer;
    private SharedCacheCoordinator cacheCoordinator;
    private CuratorFramework curatorClient;
    
    @BeforeEach
    public void setUp() throws Exception {
        InstanceSpec spec = new InstanceSpec(null, -1, -1, -1, true, -1);
        testingZooKeeperServer = new TestingZooKeeperServer(new QuorumConfigBuilder(spec));
        testingZooKeeperServer.start();
        
        cacheCoordinator = new SharedCacheCoordinator("CredentialsCacheBeanTest", spec.getConnectString(), 30, 300, 10);
        
        curatorClient = CuratorFrameworkFactory.builder().namespace("CredentialsCacheBeanTest").retryPolicy(new BoundedExponentialBackoffRetry(100, 200, 3))
                        .connectionTimeoutMs(200).sessionTimeoutMs(100).connectString(spec.getConnectString()).build();
        ReflectionTestUtils.setField(cacheCoordinator, "curatorClient", curatorClient);
        
        cacheCoordinator.start();
    }
    
    @AfterEach
    public void tearDown() throws Exception {
        cacheCoordinator.stop();
        testingZooKeeperServer.close();
    }
    
    @Test
    public void testEphemeralNodeReconnect() throws Exception {
        String ephemeralNodePath = (String) ReflectionTestUtils.getField(cacheCoordinator, "serverIdentifierPath");
        boolean exists = curatorClient.checkExists().forPath(ephemeralNodePath) != null;
        assertTrue(exists, "Ephemeral server node " + ephemeralNodePath + " doesn't exist before a zookeeper restart");
        
        final ConnectionState[] state = new ConnectionState[] {ConnectionState.CONNECTED};
        curatorClient.getConnectionStateListenable().addListener((client, newState) -> state[0] = newState);
        
        testingZooKeeperServer.restart();
        
        for (int i = 0; i < 50; ++i) {
            if (ConnectionState.RECONNECTED.equals(state[0]))
                break;
            Thread.sleep(200L);
        }
        assertEquals(ConnectionState.RECONNECTED, state[0], "Client never reconnected.");
        
        for (int i = 0; i < 50; ++i) {
            exists = curatorClient.checkExists().forPath(ephemeralNodePath) != null;
            if (exists)
                break;
            Thread.sleep(200L);
        }
        assertTrue(exists, "Ephemeral node " + ephemeralNodePath + " was not recreated.");
    }
    
    @Test
    public void testSharedCounterUpdateAfterReconnect() throws Exception {
        final String COUNTER = "testCounter";
        final ConnectionState[] state = new ConnectionState[] {ConnectionState.CONNECTED};
        final int[] count = new int[] {1};
        
        cacheCoordinator.registerCounter(COUNTER, new SharedCountListener() {
            @Override
            public void countHasChanged(SharedCountReader sharedCount, int newCount) throws Exception {
                count[0] = newCount;
                cacheCoordinator.checkCounter(COUNTER, newCount);
            }
            
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                state[0] = newState;
            }
        });
        
        SharedCount counter = new SharedCount(curatorClient, "/counters/" + COUNTER, 1);
        counter.start();
        
        try {
            
            int newCount = 10;
            VersionedValue<Integer> oldCount = counter.getVersionedValue();
            counter.trySetCount(oldCount, newCount);
            
            for (int i = 0; i < 50; ++i) {
                if (count[0] == newCount)
                    break;
                Thread.sleep(1000L);
            }
            assertEquals(newCount, count[0], "Counter never updated.");
            assertTrue(cacheCoordinator.checkCounter(COUNTER, newCount), "Counter never updated.");
            
            testingZooKeeperServer.restart();
            
            for (int i = 0; i < 50; ++i) {
                if (ConnectionState.RECONNECTED.equals(state[0]))
                    break;
                Thread.sleep(200L);
            }
            assertEquals(ConnectionState.RECONNECTED, state[0], "Client never reconnected.");
            
            newCount = 42;
            oldCount = counter.getVersionedValue();
            counter.trySetCount(oldCount, newCount);
            
            for (int i = 0; i < 50; ++i) {
                if (count[0] == newCount)
                    break;
                Thread.sleep(200L);
            }
            assertEquals(newCount, count[0], "Counter never updated after restart.");
            assertTrue(cacheCoordinator.checkCounter(COUNTER, newCount), "Counter never updated.");
        } finally {
            counter.close();
        }
    }
    
    @Test
    @Tag("IntegrationTest")
    public void testSharedCounterUpdateAfterConnectionLost() throws Exception {
        final String COUNTER = "testCounter";
        final ConnectionState[] state = new ConnectionState[] {ConnectionState.CONNECTED};
        final int[] count = new int[] {1};
        
        CuratorFramework curatorFramework = (CuratorFramework) ReflectionTestUtils.getField(cacheCoordinator, "curatorClient");
        curatorFramework.getConnectionStateListenable().addListener((client, newState) -> state[0] = newState);
        cacheCoordinator.registerCounter(COUNTER, new SharedCountListener() {
            @Override
            public void countHasChanged(SharedCountReader sharedCount, int newCount) throws Exception {
                count[0] = newCount;
                cacheCoordinator.checkCounter(COUNTER, newCount);
            }
            
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {}
        });
        
        SharedCount counter = new SharedCount(curatorClient, "/counters/" + COUNTER, 1);
        counter.start();
        
        try {
            
            int newCount = 10;
            VersionedValue<Integer> oldCount = counter.getVersionedValue();
            counter.trySetCount(oldCount, newCount);
            
            for (int i = 0; i < 50; ++i) {
                if (count[0] == newCount)
                    break;
                Thread.sleep(200L);
            }
            assertEquals(newCount, count[0], "Counter never updated.");
            assertTrue(cacheCoordinator.checkCounter(COUNTER, newCount), "Counter never updated.");
            
            testingZooKeeperServer.kill();
            
            for (int i = 0; i < 15; ++i) {
                if (ConnectionState.LOST.equals(state[0]))
                    break;
                Thread.sleep(3000L);
            }
            assertEquals(ConnectionState.LOST, state[0], "Client never lost connection.");
            
            testingZooKeeperServer.restart();
            
            for (int i = 0; i < 15; ++i) {
                if (ConnectionState.RECONNECTED.equals(state[0]))
                    break;
                Thread.sleep(3000L);
            }
            assertEquals(ConnectionState.RECONNECTED, state[0], "Client never reconnected.");
            
            newCount = 42;
            oldCount = counter.getVersionedValue();
            counter.trySetCount(oldCount, newCount);
            
            for (int i = 0; i < 10; ++i) {
                if (count[0] == newCount)
                    break;
                Thread.sleep(200L);
            }
            assertEquals(newCount, count[0], "Counter never updated after restart.");
            assertTrue(cacheCoordinator.checkCounter(COUNTER, newCount), "Counter never updated.");
        } finally {
            counter.close();
        }
    }
}
