package datawave.microservice.accumulo.stats.util;

import datawave.accumulo.inmemory.InMemoryInstance;
import org.apache.accumulo.core.client.Instance;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class AccumuloMonitorLocatorTest {
    
    private static final int ZK_PORT = 22181;
    private static final String MONITOR_LOC = "localhost:9995";
    
    private static TestingServer server;
    
    @BeforeClass
    public static void setupZk() throws Exception {
        server = new TestingServer(ZK_PORT, true);
    }
    
    private Instance accumuloInstance;
    private AccumuloMonitorLocator locator = new AccumuloMonitorLocator();
    
    @Before
    public void setup() throws Exception {
        accumuloInstance = new InMemoryInstance() {
            @Override
            public String getZooKeepers() {
                return String.format("localhost:%d", ZK_PORT);
            }
        };
        
        try (CuratorFramework curator = CuratorFrameworkFactory.newClient(String.format("localhost:%d", ZK_PORT), new RetryOneTime(500))) {
            curator.start();
            curator.create().creatingParentContainersIfNeeded().forPath("/accumulo/" + accumuloInstance.getInstanceID() + "/monitor/http_addr",
                            MONITOR_LOC.getBytes());
        }
    }
    
    @Test
    public void shouldFetchMonitorFromZookeeper() {
        assertThat(locator.getHostPort(accumuloInstance), is(MONITOR_LOC));
    }
    
    @AfterClass
    public static void tearDownZk() throws Exception {
        server.stop();
    }
}
