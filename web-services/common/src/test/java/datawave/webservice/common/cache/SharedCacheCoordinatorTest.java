package datawave.webservice.common.cache;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import datawave.common.test.integration.IntegrationTest;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.powermock.reflect.Whitebox;

/**
 * Tests functionality in the {@link SharedCacheCoordinator}.
 */
public class SharedCacheCoordinatorTest {
    private TestingZooKeeperServer testingZooKeeperServer;
    private SharedCacheCoordinator cacheCoordinator;
    private CuratorFramework curatorClient;
    
    @Before
    public void setUp() throws Exception {
        InstanceSpec spec = new InstanceSpec(null, -1, -1, -1, true, -1);
        testingZooKeeperServer = new TestingZooKeeperServer(new QuorumConfigBuilder(spec));
        testingZooKeeperServer.start();
        
        cacheCoordinator = new SharedCacheCoordinator("CredentialsCacheBeanTest", spec.getConnectString(), 30, 300, 10);
        // Previously, sessionTimeoutMS was set to 100 here. For whatever reason, after dependency bumps on ZK and Curator
        // to 3.7.0 and 5.2.0 respectively, that relatively short timeout led to a race condition here that resulted in
        // somewhat consistent test failure. Hence, the increased sessionTimeoutMs to mitigate the issue. I note also that
        // the ZK client logged a WARN here previously due to sessionTimeoutMs (100) being less than connectionTimeoutMs (200)
        curatorClient = CuratorFrameworkFactory.builder().namespace("CredentialsCacheBeanTest").retryPolicy(new BoundedExponentialBackoffRetry(100, 200, 3))
                        .connectionTimeoutMs(200).sessionTimeoutMs(1800).connectString(spec.getConnectString()).build();
        Whitebox.setInternalState(cacheCoordinator, CuratorFramework.class, curatorClient);
        
        cacheCoordinator.start();
    }
    
    @After
    public void tearDown() throws Exception {
        cacheCoordinator.stop();
        testingZooKeeperServer.close();
    }
    
    @Test
    public void testEphemeralNodeReconnect() throws Exception {
        String ephemeralNodePath = Whitebox.getInternalState(cacheCoordinator, "serverIdentifierPath");
        boolean exists = curatorClient.checkExists().forPath(ephemeralNodePath) != null;
        assertTrue("Ephemeral server node " + ephemeralNodePath + " doesn't exist before a zookeeper restart", exists);
        
        final ConnectionState[] state = new ConnectionState[] {ConnectionState.CONNECTED};
        curatorClient.getConnectionStateListenable().addListener((client, newState) -> state[0] = newState);
        
        testingZooKeeperServer.restart();
        
        for (int i = 0; i < 50; ++i) {
            if (ConnectionState.RECONNECTED.equals(state[0]))
                break;
            Thread.sleep(200L);
        }
        assertEquals("Client never reconnected.", ConnectionState.RECONNECTED, state[0]);
        
        for (int i = 0; i < 50; ++i) {
            exists = curatorClient.checkExists().forPath(ephemeralNodePath) != null;
            if (exists)
                break;
            Thread.sleep(200L);
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
            
            for (int i = 0; i < 50; ++i) {
                if (count[0] == newCount)
                    break;
                Thread.sleep(1000L);
            }
            assertEquals("Counter never updated.", newCount, count[0]);
            assertTrue("Counter never updated.", cacheCoordinator.checkCounter(COUNTER, newCount));
            
            testingZooKeeperServer.restart();
            
            for (int i = 0; i < 50; ++i) {
                if (ConnectionState.RECONNECTED.equals(state[0]))
                    break;
                Thread.sleep(200L);
            }
            assertEquals("Client never reconnected.", ConnectionState.RECONNECTED, state[0]);
            
            newCount = 42;
            oldCount = counter.getVersionedValue();
            counter.trySetCount(oldCount, newCount);
            
            for (int i = 0; i < 50; ++i) {
                if (count[0] == newCount)
                    break;
                Thread.sleep(200L);
            }
            assertEquals("Counter never updated after restart.", newCount, count[0]);
            assertTrue("Counter never updated.", cacheCoordinator.checkCounter(COUNTER, newCount));
        } finally {
            counter.close();
        }
    }
    
    @Test
    @Category(IntegrationTest.class)
    public void testSharedCounterUpdateAfterConnectionLost() throws Exception {
        final String COUNTER = "testCounter";
        final ConnectionState[] state = new ConnectionState[] {ConnectionState.CONNECTED};
        final int[] count = new int[] {1};
        
        CuratorFramework curatorFramework = Whitebox.getInternalState(cacheCoordinator, CuratorFramework.class);
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
            assertEquals("Counter never updated.", newCount, count[0]);
            assertTrue("Counter never updated.", cacheCoordinator.checkCounter(COUNTER, newCount));
            
            testingZooKeeperServer.kill();
            
            for (int i = 0; i < 15; ++i) {
                if (ConnectionState.LOST.equals(state[0]))
                    break;
                Thread.sleep(3000L);
            }
            assertEquals("Client never lost connection.", ConnectionState.LOST, state[0]);
            
            testingZooKeeperServer.restart();
            
            for (int i = 0; i < 15; ++i) {
                if (ConnectionState.RECONNECTED.equals(state[0]))
                    break;
                Thread.sleep(3000L);
            }
            assertEquals("Client never reconnected.", ConnectionState.RECONNECTED, state[0]);
            
            newCount = 42;
            oldCount = counter.getVersionedValue();
            counter.trySetCount(oldCount, newCount);
            
            for (int i = 0; i < 10; ++i) {
                if (count[0] == newCount)
                    break;
                Thread.sleep(200L);
            }
            assertEquals("Counter never updated after restart.", newCount, count[0]);
            assertTrue("Counter never updated.", cacheCoordinator.checkCounter(COUNTER, newCount));
        } finally {
            counter.close();
        }
    }
}
