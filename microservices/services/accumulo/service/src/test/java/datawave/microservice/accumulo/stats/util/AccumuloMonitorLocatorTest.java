package datawave.microservice.accumulo.stats.util;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;

public class AccumuloMonitorLocatorTest {
    
    private static final int ZK_PORT = 22181;
    private static final String MONITOR_LOC = "localhost:9995";
    
    private static TestingServer server;
    
    @BeforeAll
    public static void setupZk() throws Exception {
        server = new TestingServer(ZK_PORT, true);
    }
    
    private AccumuloClient accumuloClient;
    private AccumuloMonitorLocator locator = new AccumuloMonitorLocator();
    
    @BeforeEach
    public void setup() throws Exception {
        Properties testProperties = new Properties();
        testProperties.setProperty(ClientProperty.INSTANCE_ZOOKEEPERS.getKey(), String.format("localhost:%d", ZK_PORT));
        accumuloClient = new InMemoryAccumuloClient("root", new InMemoryInstance("testInstance")) {
            @Override
            public Properties properties() {
                return testProperties;
            }
        };
        
        try (CuratorFramework curator = CuratorFrameworkFactory.newClient(String.format("localhost:%d", ZK_PORT), new RetryOneTime(500))) {
            curator.start();
            curator.create().creatingParentContainersIfNeeded()
                            .forPath("/accumulo/" + accumuloClient.instanceOperations().getInstanceId() + "/monitor/http_addr", MONITOR_LOC.getBytes());
        }
    }
    
    @Test
    public void shouldFetchMonitorFromZookeeper() {
        assertThat(locator.getHostPort(accumuloClient), is(MONITOR_LOC));
    }
    
    @AfterAll
    public static void tearDownZk() throws Exception {
        server.stop();
    }
}
