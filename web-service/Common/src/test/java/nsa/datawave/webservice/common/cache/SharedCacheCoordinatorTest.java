package nsa.datawave.webservice.common.cache;

import static org.junit.Assert.*;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.SharedCountListener;
import org.apache.curator.framework.recipes.shared.SharedCountReader;
import org.apache.curator.framework.recipes.shared.VersionedValue;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

/**
 * Tests functionality in the {@link SharedCacheCoordinator}.
 */
public class SharedCacheCoordinatorTest {
    private TestingServer testZookeeper;
    private SharedCacheCoordinator cacheCoordinator;
    private CuratorFramework curatorClient;
    
    @Before
    public void setUp() throws Exception {
        testZookeeper = new TestingServer();
        
        cacheCoordinator = new SharedCacheCoordinator("CredentialsCacheBeanTest", testZookeeper.getConnectString(), 30, 300);
        
        curatorClient = CuratorFrameworkFactory.builder().namespace("CredentialsCacheBeanTest").retryPolicy(new RetryForever(500)).connectionTimeoutMs(500)
                        .sessionTimeoutMs(250).connectString(testZookeeper.getConnectString()).build();
        Whitebox.setInternalState(cacheCoordinator, CuratorFramework.class, curatorClient);
        
        cacheCoordinator.start();
    }
    
    @After
    public void tearDown() throws Exception {
        cacheCoordinator.stop();
        testZookeeper.close();
    }
    
    @Test
    public void testEphemeralNodeReconnect() throws Exception {
        String ephemeralNodePath = Whitebox.getInternalState(cacheCoordinator, "serverIdentifierPath");
        boolean exists = curatorClient.checkExists().forPath(ephemeralNodePath) != null;
        assertTrue("Ephemeral server node " + ephemeralNodePath + " doesn't exist before a zookeeper restart", exists);
        
        final ConnectionState[] state = new ConnectionState[] {ConnectionState.CONNECTED};
        curatorClient.getConnectionStateListenable().addListener(new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                state[0] = newState;
            }
        });
        
        testZookeeper.restart();
        
        for (int i = 0; i < 10; ++i) {
            if (ConnectionState.RECONNECTED.equals(state[0]))
                break;
            Thread.sleep(1000L);
        }
        assertEquals("Client never reconnected.", ConnectionState.RECONNECTED, state[0]);
        
        for (int i = 0; i < 10; ++i) {
            exists = curatorClient.checkExists().forPath(ephemeralNodePath) != null;
            if (exists)
                break;
            Thread.sleep(1000L);
        }
        assertTrue("Ephemeral node " + ephemeralNodePath + " was not recreated.", exists);
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
            
            for (int i = 0; i < 10; ++i) {
                if (count[0] == newCount)
                    break;
                Thread.sleep(1000L);
            }
            assertEquals("Counter never updated.", newCount, count[0]);
            assertTrue("Counter never updated.", cacheCoordinator.checkCounter(COUNTER, newCount));
            
            testZookeeper.restart();
            
            for (int i = 0; i < 10; ++i) {
                if (ConnectionState.RECONNECTED.equals(state[0]))
                    break;
                Thread.sleep(1000L);
            }
            assertEquals("Client never reconnected.", ConnectionState.RECONNECTED, state[0]);
            
            newCount = 42;
            oldCount = counter.getVersionedValue();
            counter.trySetCount(oldCount, newCount);
            
            for (int i = 0; i < 10; ++i) {
                if (count[0] == newCount)
                    break;
                Thread.sleep(1000L);
            }
            assertEquals("Counter never updated after restart.", newCount, count[0]);
            assertTrue("Counter never updated.", cacheCoordinator.checkCounter(COUNTER, newCount));
        } finally {
            counter.close();
        }
    }
}
