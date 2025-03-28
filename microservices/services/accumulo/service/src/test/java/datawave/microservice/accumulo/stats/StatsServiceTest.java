package datawave.microservice.accumulo.stats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
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
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.microservice.accumulo.TestHelper;
import datawave.microservice.authorization.jwt.JWTRestTemplate;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.webservice.response.StatsResponse;

/**
 * This tests both the {@link StatsService} and {@link StatsController} in as realistic a way as possible, without requiring fully-distributed instances of our
 * external dependencies to be active. Specifically, there are two external services that have to be accounted for:
 * <p>
 * (1) A ZooKeeper Server, which the StatsService uses to dynamically discover the Accumulo Monitor's [host:port]. For this, the test sets up a
 * {@link TestingServer}, which is initialized in advance to have the ZK data that the service needs.
 * <p>
 * (2) The Accumulo Monitor server itself, which the StatsService uses to fetch the <strong>{@code http://monitor-host:port/xml}</strong> response (a.k.a, our
 * {@link StatsResponse}). For this, we rely on a {@link MockRestServiceServer} to mock the monitor servlet's xml response, which is served from
 * {@code src/test/resources/accumulo-monitor-stats.xml}
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "spring.main.allow-bean-definition-overriding=true")
@ContextConfiguration(classes = StatsServiceTest.TestConfig.class)
@ActiveProfiles({"StatsServiceTest", "stats-service-enabled"})
public class StatsServiceTest {
    
    private static final String ZK_MONITOR_PATH = "/accumulo/%s/monitor/http_addr";
    private static final String ZK_MONITOR_DATA = "localhost:9995";
    
    private static final String EXPECTED_MONITOR_URI = String.format("http://%s/rest/xml", ZK_MONITOR_DATA);
    
    private static String expectedMonitorResponse;
    
    private static TestingServer server;
    
    @LocalServerPort
    private int webServicePort;
    
    @Autowired
    private ApplicationContext context;
    
    @Autowired
    private RestTemplateBuilder restTemplateBuilder;
    
    @Autowired
    private StatsService statsService;
    
    private JWTRestTemplate jwtRestTemplate;
    private DatawaveUserDetails defaultUserDetails;
    private MockRestServiceServer mockMonitorServer;
    private TestHelper th;
    
    @BeforeAll
    public static void setupZK() throws Exception {
        //@formatter:off
        int retryCounter = 10;
        boolean started = false;
        while (!started && retryCounter > 0) {
            retryCounter--;
            try {
                server = new TestingServer(true);
                started = true;
            } catch (Exception ex) {
                if (server != null) {
                    server.stop();
                }
            }
        }
        if (!started) {
            server = new TestingServer(true);
        }
        expectedMonitorResponse = new String(Files.readAllBytes(Paths.get(
            StatsServiceTest.class.getClassLoader().getResource("accumulo-monitor-stats.xml").toURI())));
        //@formatter:on
    }
    
    @AfterAll
    public static void tearDownZK() throws Exception {
        server.stop();
    }
    
    @BeforeEach
    public void setup() {
        // REST api user must have Administrator role
        defaultUserDetails = TestHelper.userDetails(Collections.singleton("Administrator"), null);
        jwtRestTemplate = restTemplateBuilder.build(JWTRestTemplate.class);
        
        th = new TestHelper(jwtRestTemplate, defaultUserDetails, webServicePort, "/accumulo/v1");
        
        setupMockMonitorServer();
    }
    
    @Test
    public void verifyAutoConfig() {
        assertTrue(context.containsBean("statsService"), "statsService bean not found");
        assertTrue(context.containsBean("statsController"), "statsController bean not found");
        
        assertFalse(context.containsBean("auditServiceConfiguration"), "auditServiceConfiguration bean should not have been found");
        assertFalse(context.containsBean("auditServiceInstanceProvider"), "auditServiceInstanceProvider bean should not have been found");
        assertFalse(context.containsBean("auditLookupSecurityMarking"), "auditLookupSecurityMarking bean should not have been found");
        assertFalse(context.containsBean("lookupService"), "lookupService bean should not have been found");
        assertFalse(context.containsBean("lookupController"), "lookupController bean should not have been found");
        assertFalse(context.containsBean("adminService"), "adminService bean should not have been found");
        assertFalse(context.containsBean("adminController"), "adminController bean should not have been found");
    }
    
    @Test
    public void testStatsService() {
        mockMonitorServer.expect(requestTo(EXPECTED_MONITOR_URI)).andRespond(withSuccess(expectedMonitorResponse, MediaType.APPLICATION_XML));
        
        StatsResponse response = th.assert200Status(th.createGetRequest("/stats"), StatsResponse.class);
        
        mockMonitorServer.verify();
        
        // Spot-check deserialized response...
        assertNotNull(response);
        assertNotNull(response.getServers());
        assertNotNull(response.getTables());
        assertNotNull(response.getTotals());
        assertEquals(1, response.getServers().size());
        assertEquals(15, response.getTables().size());
        assertEquals("0.0", response.getTotals().getDiskrate().toString());
        assertEquals("0.7999999999525093", response.getTotals().getQueryrate().toString());
        assertEquals("36977.0", response.getTotals().getNumentries().toString());
        assertEquals("2.783203125000025E-13", response.getTotals().getIngestrate().toString());
    }
    
    /**
     * Mocks the {@link StatsService#restTemplate} field in order to control the response from {@link #EXPECTED_MONITOR_URI}
     */
    private void setupMockMonitorServer() {
        // Here we're mocking the restTemplate field within the StatsService instance
        //@formatter:off
        RestTemplate monitorRestTemplate = (RestTemplate)
                new DirectFieldAccessor(statsService).getPropertyValue("restTemplate");
        //@formatter:on
        mockMonitorServer = MockRestServiceServer.createServer(monitorRestTemplate);
    }
    
    @Configuration
    @Profile("StatsServiceTest")
    @ComponentScan(basePackages = "datawave.microservice")
    public static class TestConfig {
        /**
         * This eager-loaded bean should get injected into StatsService, thus overriding the lazy-loaded "warehouse" Instance in
         * {@link datawave.microservice.accumulo.config.AccumuloConfiguration}
         * <p>
         * Also, we use the bean's Instance ID to initialize the monitor data that we need in ZK (our {@link TestingServer})
         * 
         * @return "warehouse" Instance for mocked ZK server
         */
        @Bean
        @Qualifier("warehouse")
        public AccumuloClient warehouseClient() throws Exception {
            Properties testProperties = new Properties();
            testProperties.setProperty(ClientProperty.INSTANCE_ZOOKEEPERS.getKey(), String.format("localhost:%d", server.getPort()));
            final AccumuloClient accumuloClient = new InMemoryAccumuloClient("root", new InMemoryInstance("testInstance")) {
                @Override
                public Properties properties() {
                    return testProperties;
                }
            };
            //@formatter:off
            try (CuratorFramework curator = CuratorFrameworkFactory.newClient(
                    String.format("localhost:%d", server.getPort()), new RetryOneTime(500))) {
                curator.start();
                curator.create().creatingParentContainersIfNeeded()
                    .forPath(String.format(ZK_MONITOR_PATH, accumuloClient.instanceOperations().getInstanceId()), ZK_MONITOR_DATA.getBytes());
            }
            //@formatter:on
            
            return accumuloClient;
        }
    }
}
