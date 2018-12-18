package datawave.microservice.audit.health.rabbit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.http.client.domain.BindingInfo;
import com.rabbitmq.http.client.domain.ExchangeInfo;
import com.rabbitmq.http.client.domain.NodeInfo;
import com.rabbitmq.http.client.domain.QueueInfo;
import datawave.microservice.audit.health.rabbit.config.RabbitHealthProperties;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.client.ExpectedCount;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withServerError;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ContextConfiguration(classes = RabbitHealthCheckerTest.RabbitHealthCheckerTestConfiguration.class)
@ActiveProfiles({"RabbitHealthTest", "rabbit-config"})
public class RabbitHealthCheckerTest {
    
    private static final String NODES_URL = "http://localhost:15672/api/nodes/";
    private static final String EXCHANGES_URL = "http://localhost:15672/api/exchanges/";
    private static final String QUEUES_URL = "http://localhost:15672/api/queues/";
    private static final String BINDINGS_URL = "http://localhost:15672/api/bindings/";
    
    private static final String QUEUE_URL = "http://localhost:15672/api/queues/%2F/";
    private static final String EXCHANGE_URL = "http://localhost:15672/api/exchanges/%2F/";
    private static final String BINDING_URL = "http://localhost:15672/api/bindings/%2F/";
    
    private ObjectMapper mapper = new ObjectMapper();
    
    private MockRestServiceServer mockServer;
    private RabbitHealthChecker healthChecker;
    
    @Autowired
    private RabbitHealthProperties rabbitHealthProperties;
    
    @Before
    public void beforeTest() {
        healthChecker = new RabbitHealthChecker(rabbitHealthProperties, "localhost", "guest", "guest");
        RestTemplate restTemplate = (RestTemplate) new DirectFieldAccessor(new DirectFieldAccessor(healthChecker).getPropertyValue("rabbitClient"))
                        .getPropertyValue("rt");
        mockServer = MockRestServiceServer.createServer(restTemplate);
    }
    
    @Test
    public void everythingHealthyTest() throws Exception {
        // runHealthCheck() calls
        mockServer.expect(requestTo(NODES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyNodeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(EXCHANGES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyExchangeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyQueueInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(BINDINGS_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyBindingInfo()), MediaType.APPLICATION_JSON));
        
        // health() calls
        mockServer.expect(requestTo(QUEUE_URL + "audit.log")).andRespond(
                        withSuccess(mapper.writeValueAsString(createAuditQueue("audit.log", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andRespond(withSuccess(
                        mapper.writeValueAsString(createAuditDeadLetterQueue("audit.log.dlq", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        
        healthChecker.runHealthCheck();
        
        Assert.assertTrue(healthChecker.isHealthy());
        Assert.assertEquals(Status.UP, healthChecker.health().getStatus());
        
        mockServer.verify();
        
        Assert.assertEquals(rabbitHealthProperties.getHealthyPollIntervalMillis(), healthChecker.pollIntervalMillis());
        Assert.assertEquals(0, healthChecker.getOutageStats().size());
    }
    
    @Test
    public void everythingUnhealthyTest() throws Exception {
        // runHealthCheck() calls
        mockServer.expect(requestTo(NODES_URL)).andRespond(withServerError());
        mockServer.expect(requestTo(EXCHANGES_URL)).andRespond(withServerError());
        mockServer.expect(requestTo(QUEUES_URL)).andRespond(withServerError());
        mockServer.expect(requestTo(BINDINGS_URL)).andRespond(withServerError());
        
        // health() calls
        mockServer.expect(requestTo(QUEUE_URL + "audit.log")).andRespond(withServerError());
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andRespond(withServerError());
        
        healthChecker.runHealthCheck();
        
        Assert.assertFalse(healthChecker.isHealthy());
        Assert.assertEquals(RabbitHealthChecker.RABBITMQ_UNHEALTHY, healthChecker.health().getStatus());
        
        mockServer.verify();
        
        Assert.assertEquals(rabbitHealthProperties.getUnhealthyPollIntervalMillis(), healthChecker.pollIntervalMillis());
        Assert.assertEquals(0, healthChecker.getOutageStats().size());
    }
    
    @Test
    public void missingNodeAtInitTest() throws Exception {
        // runHealthCheck() calls
        mockServer.expect(requestTo(NODES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getMissingNodeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(EXCHANGES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyExchangeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyQueueInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(BINDINGS_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyBindingInfo()), MediaType.APPLICATION_JSON));
        
        // health() calls
        mockServer.expect(requestTo(QUEUE_URL + "audit.log")).andRespond(
                        withSuccess(mapper.writeValueAsString(createAuditQueue("audit.log", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andRespond(withSuccess(
                        mapper.writeValueAsString(createAuditDeadLetterQueue("audit.log.dlq", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        
        healthChecker.runHealthCheck();
        
        Assert.assertFalse(healthChecker.isHealthy());
        Assert.assertEquals(RabbitHealthChecker.RABBITMQ_UNHEALTHY, healthChecker.health().getStatus());
        
        mockServer.verify();
        
        Assert.assertEquals(rabbitHealthProperties.getUnhealthyPollIntervalMillis(), healthChecker.pollIntervalMillis());
        Assert.assertEquals(0, healthChecker.getOutageStats().size());
    }
    
    @Test
    public void missingNodeAfterHealthyTest() throws Exception {
        goodHealthCheck();
        
        // missing runHealthCheck() calls
        mockServer.expect(ExpectedCount.times(4), requestTo(NODES_URL))
                        .andRespond(withSuccess(mapper.writeValueAsString(getMissingNodeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(ExpectedCount.times(4), requestTo(EXCHANGES_URL))
                        .andRespond(withSuccess(mapper.writeValueAsString(getHealthyExchangeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(ExpectedCount.times(4), requestTo(QUEUES_URL))
                        .andRespond(withSuccess(mapper.writeValueAsString(getHealthyQueueInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(ExpectedCount.times(4), requestTo(BINDINGS_URL))
                        .andRespond(withSuccess(mapper.writeValueAsString(getHealthyBindingInfo()), MediaType.APPLICATION_JSON));
        
        // missing health() calls
        mockServer.expect(requestTo(QUEUE_URL + "audit.log")).andRespond(
                        withSuccess(mapper.writeValueAsString(createAuditQueue("audit.log", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andRespond(withSuccess(
                        mapper.writeValueAsString(createAuditDeadLetterQueue("audit.log.dlq", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        
        // unhealthy call
        healthChecker.runHealthCheck();
        Assert.assertTrue(healthChecker.isHealthy());
        
        healthChecker.runHealthCheck();
        Assert.assertTrue(healthChecker.isHealthy());
        
        healthChecker.runHealthCheck();
        Assert.assertTrue(healthChecker.isHealthy());
        
        healthChecker.runHealthCheck();
        
        // ends up unhealthy with an outage stat
        Assert.assertFalse(healthChecker.isHealthy());
        Assert.assertEquals(RabbitHealthChecker.RABBITMQ_UNHEALTHY, healthChecker.health().getStatus());
        Assert.assertEquals(rabbitHealthProperties.getUnhealthyPollIntervalMillis(), healthChecker.pollIntervalMillis());
        Assert.assertEquals(1, healthChecker.getOutageStats().size());
        
        Assert.assertEquals("current", healthChecker.getOutageStats().get(0).get("stopDate"));
        Assert.assertEquals(1, healthChecker.getOutageStats().get(0).get("numNodesMissing"));
        
        mockServer.verify();
    }
    
    @Test
    @DirtiesContext
    public void missingNodeAfterHealthyNeverFailTest() throws Exception {
        rabbitHealthProperties.getCluster().setFailIfNodeMissing(false);
        
        goodHealthCheck();
        
        // missing runHealthCheck() calls
        mockServer.expect(ExpectedCount.times(4), requestTo(NODES_URL))
                        .andRespond(withSuccess(mapper.writeValueAsString(getMissingNodeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(ExpectedCount.times(4), requestTo(EXCHANGES_URL))
                        .andRespond(withSuccess(mapper.writeValueAsString(getHealthyExchangeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(ExpectedCount.times(4), requestTo(QUEUES_URL))
                        .andRespond(withSuccess(mapper.writeValueAsString(getHealthyQueueInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(ExpectedCount.times(4), requestTo(BINDINGS_URL))
                        .andRespond(withSuccess(mapper.writeValueAsString(getHealthyBindingInfo()), MediaType.APPLICATION_JSON));
        
        // missing health() calls
        mockServer.expect(requestTo(QUEUE_URL + "audit.log")).andRespond(
                        withSuccess(mapper.writeValueAsString(createAuditQueue("audit.log", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andRespond(withSuccess(
                        mapper.writeValueAsString(createAuditDeadLetterQueue("audit.log.dlq", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        
        // unhealthy call
        healthChecker.runHealthCheck();
        Assert.assertTrue(healthChecker.isHealthy());
        
        healthChecker.runHealthCheck();
        Assert.assertTrue(healthChecker.isHealthy());
        
        healthChecker.runHealthCheck();
        Assert.assertTrue(healthChecker.isHealthy());
        
        healthChecker.runHealthCheck();
        
        // ends up unhealthy with an outage stat
        Assert.assertTrue(healthChecker.isHealthy());
        Assert.assertEquals(Status.UP, healthChecker.health().getStatus());
        Assert.assertEquals(rabbitHealthProperties.getHealthyPollIntervalMillis(), healthChecker.pollIntervalMillis());
        Assert.assertEquals(0, healthChecker.getOutageStats().size());
        
        mockServer.verify();
    }
    
    @Test
    public void stoppedNodeAtInitTest() throws Exception {
        // runHealthCheck() calls
        mockServer.expect(requestTo(NODES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getStoppedNodeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(EXCHANGES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyExchangeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyQueueInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(BINDINGS_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyBindingInfo()), MediaType.APPLICATION_JSON));
        
        // health() calls
        mockServer.expect(requestTo(QUEUE_URL + "audit.log")).andRespond(
                        withSuccess(mapper.writeValueAsString(createAuditQueue("audit.log", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andRespond(withSuccess(
                        mapper.writeValueAsString(createAuditDeadLetterQueue("audit.log.dlq", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        
        healthChecker.runHealthCheck();
        
        Assert.assertFalse(healthChecker.isHealthy());
        Assert.assertEquals(RabbitHealthChecker.RABBITMQ_UNHEALTHY, healthChecker.health().getStatus());
        
        mockServer.verify();
        
        Assert.assertEquals(rabbitHealthProperties.getUnhealthyPollIntervalMillis(), healthChecker.pollIntervalMillis());
        Assert.assertEquals(0, healthChecker.getOutageStats().size());
    }
    
    @Test
    public void stoppedNodeAfterHealthyTest() throws Exception {
        goodHealthCheck();
        
        // missing runHealthCheck() calls
        mockServer.expect(ExpectedCount.times(4), requestTo(NODES_URL))
                        .andRespond(withSuccess(mapper.writeValueAsString(getStoppedNodeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(ExpectedCount.times(4), requestTo(EXCHANGES_URL))
                        .andRespond(withSuccess(mapper.writeValueAsString(getHealthyExchangeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(ExpectedCount.times(4), requestTo(QUEUES_URL))
                        .andRespond(withSuccess(mapper.writeValueAsString(getHealthyQueueInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(ExpectedCount.times(4), requestTo(BINDINGS_URL))
                        .andRespond(withSuccess(mapper.writeValueAsString(getHealthyBindingInfo()), MediaType.APPLICATION_JSON));
        
        // missing health() calls
        mockServer.expect(requestTo(QUEUE_URL + "audit.log")).andRespond(
                        withSuccess(mapper.writeValueAsString(createAuditQueue("audit.log", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andRespond(withSuccess(
                        mapper.writeValueAsString(createAuditDeadLetterQueue("audit.log.dlq", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        
        // unhealthy call
        healthChecker.runHealthCheck();
        Assert.assertTrue(healthChecker.isHealthy());
        
        healthChecker.runHealthCheck();
        Assert.assertTrue(healthChecker.isHealthy());
        
        healthChecker.runHealthCheck();
        Assert.assertTrue(healthChecker.isHealthy());
        
        healthChecker.runHealthCheck();
        
        // ends up unhealthy with an outage stat
        Assert.assertFalse(healthChecker.isHealthy());
        Assert.assertEquals(RabbitHealthChecker.RABBITMQ_UNHEALTHY, healthChecker.health().getStatus());
        Assert.assertEquals(rabbitHealthProperties.getUnhealthyPollIntervalMillis(), healthChecker.pollIntervalMillis());
        Assert.assertEquals(1, healthChecker.getOutageStats().size());
        
        Assert.assertEquals("current", healthChecker.getOutageStats().get(0).get("stopDate"));
        Assert.assertEquals(1, healthChecker.getOutageStats().get(0).get("numNodesMissing"));
        
        mockServer.verify();
    }
    
    @Test
    @DirtiesContext
    public void stoppedNodeAfterHealthyNeverFailTest() throws Exception {
        rabbitHealthProperties.getCluster().setFailIfNodeMissing(false);
        
        goodHealthCheck();
        
        // missing runHealthCheck() calls
        mockServer.expect(ExpectedCount.times(4), requestTo(NODES_URL))
                        .andRespond(withSuccess(mapper.writeValueAsString(getStoppedNodeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(ExpectedCount.times(4), requestTo(EXCHANGES_URL))
                        .andRespond(withSuccess(mapper.writeValueAsString(getHealthyExchangeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(ExpectedCount.times(4), requestTo(QUEUES_URL))
                        .andRespond(withSuccess(mapper.writeValueAsString(getHealthyQueueInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(ExpectedCount.times(4), requestTo(BINDINGS_URL))
                        .andRespond(withSuccess(mapper.writeValueAsString(getHealthyBindingInfo()), MediaType.APPLICATION_JSON));
        
        // missing health() calls
        mockServer.expect(requestTo(QUEUE_URL + "audit.log")).andRespond(
                        withSuccess(mapper.writeValueAsString(createAuditQueue("audit.log", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andRespond(withSuccess(
                        mapper.writeValueAsString(createAuditDeadLetterQueue("audit.log.dlq", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        
        // unhealthy call
        healthChecker.runHealthCheck();
        Assert.assertTrue(healthChecker.isHealthy());
        
        healthChecker.runHealthCheck();
        Assert.assertTrue(healthChecker.isHealthy());
        
        healthChecker.runHealthCheck();
        Assert.assertTrue(healthChecker.isHealthy());
        
        healthChecker.runHealthCheck();
        
        // ends up unhealthy with an outage stat
        Assert.assertTrue(healthChecker.isHealthy());
        Assert.assertEquals(Status.UP, healthChecker.health().getStatus());
        Assert.assertEquals(rabbitHealthProperties.getHealthyPollIntervalMillis(), healthChecker.pollIntervalMillis());
        Assert.assertEquals(0, healthChecker.getOutageStats().size());
        
        mockServer.verify();
    }
    
    @Test
    @DirtiesContext
    public void missingQueueAtInitTest() throws Exception {
        // runHealthCheck() calls
        mockServer.expect(requestTo(NODES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyNodeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(EXCHANGES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyExchangeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getMissingQueueInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(BINDINGS_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyBindingInfo()), MediaType.APPLICATION_JSON));
        
        // health() calls
        mockServer.expect(requestTo(QUEUE_URL + "audit.log")).andRespond(
                        withSuccess(mapper.writeValueAsString(createAuditQueue("audit.log", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andRespond(withSuccess(
                        mapper.writeValueAsString(createAuditDeadLetterQueue("audit.log.dlq", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        
        healthChecker.runHealthCheck();
        
        Assert.assertFalse(healthChecker.isHealthy());
        Assert.assertEquals(RabbitHealthChecker.RABBITMQ_UNHEALTHY, healthChecker.health().getStatus());
        
        mockServer.verify();
        mockServer.reset();
        
        Assert.assertEquals(rabbitHealthProperties.getUnhealthyPollIntervalMillis(), healthChecker.pollIntervalMillis());
        Assert.assertEquals(0, healthChecker.getOutageStats().size());
        
        // test recovery
        // recovery turned off
        rabbitHealthProperties.setAttemptRecovery(false);
        healthChecker.recover();
        Assert.assertFalse(healthChecker.isHealthy());
        
        // recovery turned on
        rabbitHealthProperties.setAttemptRecovery(true);
        
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andExpect(method(HttpMethod.PUT)).andRespond(withSuccess());
        
        healthChecker.recover();
        
        mockServer.verify();
        mockServer.reset();
        
        // finally run a health check and return the correct configuration to show that our status changes
        goodHealthCheck();
    }
    
    @Test
    @DirtiesContext
    public void missingQueueAfterHealthyTest() throws Exception {
        goodHealthCheck();
        
        // missing runHealthCheck() calls
        mockServer.expect(requestTo(NODES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyNodeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(EXCHANGES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyExchangeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getMissingQueueInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(BINDINGS_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyBindingInfo()), MediaType.APPLICATION_JSON));
        
        // missing health() calls
        mockServer.expect(requestTo(QUEUE_URL + "audit.log")).andRespond(
                        withSuccess(mapper.writeValueAsString(createAuditQueue("audit.log", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andRespond(withSuccess(
                        mapper.writeValueAsString(createAuditDeadLetterQueue("audit.log.dlq", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        
        // unhealthy call
        healthChecker.runHealthCheck();
        
        // ends up unhealthy with an outage stat
        Assert.assertFalse(healthChecker.isHealthy());
        Assert.assertEquals(RabbitHealthChecker.RABBITMQ_UNHEALTHY, healthChecker.health().getStatus());
        Assert.assertEquals(rabbitHealthProperties.getUnhealthyPollIntervalMillis(), healthChecker.pollIntervalMillis());
        Assert.assertEquals(1, healthChecker.getOutageStats().size());
        
        Assert.assertEquals("current", healthChecker.getOutageStats().get(0).get("stopDate"));
        Assert.assertEquals("[audit.log.dlq]", healthChecker.getOutageStats().get(0).get("missingQueues").toString());
        
        mockServer.verify();
        mockServer.reset();
        
        // test recovery
        // recovery turned off
        rabbitHealthProperties.setAttemptRecovery(false);
        healthChecker.recover();
        Assert.assertFalse(healthChecker.isHealthy());
        
        // recovery turned on
        rabbitHealthProperties.setAttemptRecovery(true);
        
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andExpect(method(HttpMethod.PUT)).andRespond(withSuccess());
        
        healthChecker.recover();
        
        mockServer.verify();
        mockServer.reset();
        
        // finally run a health check and return the correct configuration to show that our status changes
        goodHealthCheck(1);
    }
    
    @Test
    @DirtiesContext
    public void fixableInvalidQueueAtInitTest() throws Exception {
        // runHealthCheck() calls
        mockServer.expect(requestTo(NODES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyNodeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(EXCHANGES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyExchangeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getFixableInvalidQueueInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(BINDINGS_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyBindingInfo()), MediaType.APPLICATION_JSON));
        
        // health() calls
        mockServer.expect(requestTo(QUEUE_URL + "audit.log")).andRespond(
                        withSuccess(mapper.writeValueAsString(createAuditQueue("audit.log", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andRespond(withSuccess(
                        mapper.writeValueAsString(createAuditDeadLetterQueue("audit.log.dlq", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        
        healthChecker.runHealthCheck();
        
        Assert.assertFalse(healthChecker.isHealthy());
        Assert.assertEquals(RabbitHealthChecker.RABBITMQ_UNHEALTHY, healthChecker.health().getStatus());
        
        mockServer.verify();
        mockServer.reset();
        
        Assert.assertEquals(rabbitHealthProperties.getUnhealthyPollIntervalMillis(), healthChecker.pollIntervalMillis());
        Assert.assertEquals(0, healthChecker.getOutageStats().size());
        
        // test recovery
        // recovery turned off
        rabbitHealthProperties.setAttemptRecovery(false);
        healthChecker.recover();
        Assert.assertFalse(healthChecker.isHealthy());
        
        // recovery turned on
        rabbitHealthProperties.setAttemptRecovery(true);
        
        mockServer.expect(requestTo(QUEUE_URL + "audit.log")).andExpect(method(HttpMethod.GET))
                        .andRespond(withSuccess(mapper.writeValueAsString(getFixableInvalidQueueInfo()[0]), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUE_URL + "audit.log")).andExpect(method(HttpMethod.DELETE)).andRespond(withSuccess());
        mockServer.expect(requestTo(QUEUE_URL + "audit.log")).andExpect(method(HttpMethod.PUT)).andRespond(withSuccess());
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andExpect(method(HttpMethod.GET))
                        .andRespond(withSuccess(mapper.writeValueAsString(getFixableInvalidQueueInfo()[0]), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andExpect(method(HttpMethod.DELETE)).andRespond(withSuccess());
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andExpect(method(HttpMethod.PUT)).andRespond(withSuccess());
        
        healthChecker.recover();
        
        mockServer.verify();
        mockServer.reset();
        
        // finally run a health check and return the correct configuration to show that our status changes
        goodHealthCheck();
    }
    
    @Test
    @DirtiesContext
    public void fixableInvalidQueueAfterHealthyTest() throws Exception {
        goodHealthCheck();
        
        // missing runHealthCheck() calls
        mockServer.expect(requestTo(NODES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyNodeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(EXCHANGES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyExchangeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getFixableInvalidQueueInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(BINDINGS_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyBindingInfo()), MediaType.APPLICATION_JSON));
        
        // missing health() calls
        mockServer.expect(requestTo(QUEUE_URL + "audit.log")).andRespond(
                        withSuccess(mapper.writeValueAsString(createAuditQueue("audit.log", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andRespond(withSuccess(
                        mapper.writeValueAsString(createAuditDeadLetterQueue("audit.log.dlq", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        
        // unhealthy call
        healthChecker.runHealthCheck();
        
        // ends up unhealthy with an outage stat
        Assert.assertFalse(healthChecker.isHealthy());
        Assert.assertEquals(RabbitHealthChecker.RABBITMQ_UNHEALTHY, healthChecker.health().getStatus());
        Assert.assertEquals(rabbitHealthProperties.getUnhealthyPollIntervalMillis(), healthChecker.pollIntervalMillis());
        Assert.assertEquals(1, healthChecker.getOutageStats().size());
        
        Assert.assertEquals("current", healthChecker.getOutageStats().get(0).get("stopDate"));
        Assert.assertEquals("[audit.log.dlq, audit.log]", healthChecker.getOutageStats().get(0).get("invalidQueues").toString());
        
        mockServer.verify();
        mockServer.reset();
        
        // test recovery
        // recovery turned off
        rabbitHealthProperties.setAttemptRecovery(false);
        healthChecker.recover();
        Assert.assertFalse(healthChecker.isHealthy());
        
        // recovery turned on
        rabbitHealthProperties.setAttemptRecovery(true);
        
        mockServer.expect(requestTo(QUEUE_URL + "audit.log")).andExpect(method(HttpMethod.GET))
                        .andRespond(withSuccess(mapper.writeValueAsString(getFixableInvalidQueueInfo()[0]), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUE_URL + "audit.log")).andExpect(method(HttpMethod.DELETE)).andRespond(withSuccess());
        mockServer.expect(requestTo(QUEUE_URL + "audit.log")).andExpect(method(HttpMethod.PUT)).andRespond(withSuccess());
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andExpect(method(HttpMethod.GET))
                        .andRespond(withSuccess(mapper.writeValueAsString(getFixableInvalidQueueInfo()[0]), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andExpect(method(HttpMethod.DELETE)).andRespond(withSuccess());
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andExpect(method(HttpMethod.PUT)).andRespond(withSuccess());
        
        healthChecker.recover();
        
        mockServer.verify();
        mockServer.reset();
        
        // finally run a health check and return the correct configuration to show that our status changes
        goodHealthCheck(1);
    }
    
    @Test
    @DirtiesContext
    public void unfixableInvalidQueueAtInitTest() throws Exception {
        // runHealthCheck() calls
        mockServer.expect(requestTo(NODES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyNodeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(EXCHANGES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyExchangeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getUnfixableInvalidQueueInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(BINDINGS_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyBindingInfo()), MediaType.APPLICATION_JSON));
        
        // health() calls
        mockServer.expect(requestTo(QUEUE_URL + "audit.log")).andRespond(
                        withSuccess(mapper.writeValueAsString(createAuditQueue("audit.log", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andRespond(withSuccess(
                        mapper.writeValueAsString(createAuditDeadLetterQueue("audit.log.dlq", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        
        healthChecker.runHealthCheck();
        
        Assert.assertFalse(healthChecker.isHealthy());
        Assert.assertEquals(RabbitHealthChecker.RABBITMQ_UNHEALTHY, healthChecker.health().getStatus());
        
        mockServer.verify();
        mockServer.reset();
        
        Assert.assertEquals(rabbitHealthProperties.getUnhealthyPollIntervalMillis(), healthChecker.pollIntervalMillis());
        Assert.assertEquals(0, healthChecker.getOutageStats().size());
        
        // test recovery
        // recovery turned off
        rabbitHealthProperties.setAttemptRecovery(false);
        healthChecker.recover();
        Assert.assertFalse(healthChecker.isHealthy());
        
        // recovery turned on
        rabbitHealthProperties.setAttemptRecovery(true);
        
        mockServer.expect(requestTo(QUEUE_URL + "audit.log")).andExpect(method(HttpMethod.GET))
                        .andRespond(withSuccess(mapper.writeValueAsString(getUnfixableInvalidQueueInfo()[0]), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andExpect(method(HttpMethod.GET))
                        .andRespond(withSuccess(mapper.writeValueAsString(getUnfixableInvalidQueueInfo()[0]), MediaType.APPLICATION_JSON));
        
        healthChecker.recover();
        
        mockServer.verify();
        mockServer.reset();
        
        // finally run a health check and return the correct configuration to show that our status changes
        goodHealthCheck();
    }
    
    @Test
    @DirtiesContext
    public void unfixableInvalidQueueAfterHealthyTest() throws Exception {
        goodHealthCheck();
        
        // missing runHealthCheck() calls
        mockServer.expect(requestTo(NODES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyNodeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(EXCHANGES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyExchangeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getUnfixableInvalidQueueInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(BINDINGS_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyBindingInfo()), MediaType.APPLICATION_JSON));
        
        // missing health() calls
        mockServer.expect(requestTo(QUEUE_URL + "audit.log")).andRespond(
                        withSuccess(mapper.writeValueAsString(createAuditQueue("audit.log", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andRespond(withSuccess(
                        mapper.writeValueAsString(createAuditDeadLetterQueue("audit.log.dlq", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        
        // unhealthy call
        healthChecker.runHealthCheck();
        
        // ends up unhealthy with an outage stat
        Assert.assertFalse(healthChecker.isHealthy());
        Assert.assertEquals(RabbitHealthChecker.RABBITMQ_UNHEALTHY, healthChecker.health().getStatus());
        Assert.assertEquals(rabbitHealthProperties.getUnhealthyPollIntervalMillis(), healthChecker.pollIntervalMillis());
        Assert.assertEquals(1, healthChecker.getOutageStats().size());
        
        Assert.assertEquals("current", healthChecker.getOutageStats().get(0).get("stopDate"));
        Assert.assertEquals("[audit.log.dlq, audit.log]", healthChecker.getOutageStats().get(0).get("invalidQueues").toString());
        
        mockServer.verify();
        mockServer.reset();
        
        // test recovery
        // recovery turned off
        rabbitHealthProperties.setAttemptRecovery(false);
        healthChecker.recover();
        Assert.assertFalse(healthChecker.isHealthy());
        
        // recovery turned on
        rabbitHealthProperties.setAttemptRecovery(true);
        
        mockServer.expect(requestTo(QUEUE_URL + "audit.log")).andExpect(method(HttpMethod.GET))
                        .andRespond(withSuccess(mapper.writeValueAsString(getUnfixableInvalidQueueInfo()[0]), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andExpect(method(HttpMethod.GET))
                        .andRespond(withSuccess(mapper.writeValueAsString(getUnfixableInvalidQueueInfo()[0]), MediaType.APPLICATION_JSON));
        
        healthChecker.recover();
        
        mockServer.verify();
        mockServer.reset();
        
        // finally run a health check and return the correct configuration to show that our status changes
        goodHealthCheck(1);
    }
    
    @Test
    @DirtiesContext
    public void missingExchangeAtInitTest() throws Exception {
        // runHealthCheck() calls
        mockServer.expect(requestTo(NODES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyNodeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(EXCHANGES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getMissingExchangeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyQueueInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(BINDINGS_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyBindingInfo()), MediaType.APPLICATION_JSON));
        
        // health() calls
        mockServer.expect(requestTo(QUEUE_URL + "audit.log")).andRespond(
                        withSuccess(mapper.writeValueAsString(createAuditQueue("audit.log", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andRespond(withSuccess(
                        mapper.writeValueAsString(createAuditDeadLetterQueue("audit.log.dlq", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        
        healthChecker.runHealthCheck();
        
        Assert.assertFalse(healthChecker.isHealthy());
        Assert.assertEquals(RabbitHealthChecker.RABBITMQ_UNHEALTHY, healthChecker.health().getStatus());
        
        mockServer.verify();
        mockServer.reset();
        
        Assert.assertEquals(rabbitHealthProperties.getUnhealthyPollIntervalMillis(), healthChecker.pollIntervalMillis());
        Assert.assertEquals(0, healthChecker.getOutageStats().size());
        
        // test recovery
        // recovery turned off
        rabbitHealthProperties.setAttemptRecovery(false);
        healthChecker.recover();
        Assert.assertFalse(healthChecker.isHealthy());
        
        // recovery turned on
        rabbitHealthProperties.setAttemptRecovery(true);
        
        mockServer.expect(requestTo(EXCHANGE_URL + "audit")).andExpect(method(HttpMethod.PUT)).andRespond(withSuccess());
        
        healthChecker.recover();
        
        mockServer.verify();
        mockServer.reset();
        
        // finally run a health check and return the correct configuration to show that our status changes
        goodHealthCheck();
    }
    
    @Test
    @DirtiesContext
    public void missingExchangeAfterHealthyTest() throws Exception {
        goodHealthCheck();
        
        // missing runHealthCheck() calls
        mockServer.expect(requestTo(NODES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyNodeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(EXCHANGES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getMissingExchangeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyQueueInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(BINDINGS_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyBindingInfo()), MediaType.APPLICATION_JSON));
        
        // missing health() calls
        mockServer.expect(requestTo(QUEUE_URL + "audit.log")).andRespond(
                        withSuccess(mapper.writeValueAsString(createAuditQueue("audit.log", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andRespond(withSuccess(
                        mapper.writeValueAsString(createAuditDeadLetterQueue("audit.log.dlq", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        
        // unhealthy call
        healthChecker.runHealthCheck();
        
        // ends up unhealthy with an outage stat
        Assert.assertFalse(healthChecker.isHealthy());
        Assert.assertEquals(RabbitHealthChecker.RABBITMQ_UNHEALTHY, healthChecker.health().getStatus());
        Assert.assertEquals(rabbitHealthProperties.getUnhealthyPollIntervalMillis(), healthChecker.pollIntervalMillis());
        Assert.assertEquals(1, healthChecker.getOutageStats().size());
        
        Assert.assertEquals("current", healthChecker.getOutageStats().get(0).get("stopDate"));
        Assert.assertEquals("[audit]", healthChecker.getOutageStats().get(0).get("missingExchanges").toString());
        
        mockServer.verify();
        mockServer.reset();
        
        // test recovery
        // recovery turned off
        rabbitHealthProperties.setAttemptRecovery(false);
        healthChecker.recover();
        Assert.assertFalse(healthChecker.isHealthy());
        
        // recovery turned on
        rabbitHealthProperties.setAttemptRecovery(true);
        
        mockServer.expect(requestTo(EXCHANGE_URL + "audit")).andExpect(method(HttpMethod.PUT)).andRespond(withSuccess());
        
        healthChecker.recover();
        
        mockServer.verify();
        mockServer.reset();
        
        // finally run a health check and return the correct configuration to show that our status changes
        goodHealthCheck(1);
    }
    
    @Test
    @DirtiesContext
    public void invalidExchangeAtInitTest() throws Exception {
        // runHealthCheck() calls
        mockServer.expect(requestTo(NODES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyNodeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(EXCHANGES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getInvalidExchangeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyQueueInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(BINDINGS_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyBindingInfo()), MediaType.APPLICATION_JSON));
        
        // health() calls
        mockServer.expect(requestTo(QUEUE_URL + "audit.log")).andRespond(
                        withSuccess(mapper.writeValueAsString(createAuditQueue("audit.log", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andRespond(withSuccess(
                        mapper.writeValueAsString(createAuditDeadLetterQueue("audit.log.dlq", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        
        healthChecker.runHealthCheck();
        
        Assert.assertFalse(healthChecker.isHealthy());
        Assert.assertEquals(RabbitHealthChecker.RABBITMQ_UNHEALTHY, healthChecker.health().getStatus());
        
        mockServer.verify();
        mockServer.reset();
        
        Assert.assertEquals(rabbitHealthProperties.getUnhealthyPollIntervalMillis(), healthChecker.pollIntervalMillis());
        Assert.assertEquals(0, healthChecker.getOutageStats().size());
        
        // test recovery
        // recovery turned off
        rabbitHealthProperties.setAttemptRecovery(false);
        healthChecker.recover();
        Assert.assertFalse(healthChecker.isHealthy());
        
        // recovery turned on
        rabbitHealthProperties.setAttemptRecovery(true);
        
        mockServer.expect(requestTo(EXCHANGE_URL + "audit")).andExpect(method(HttpMethod.DELETE)).andRespond(withSuccess());
        mockServer.expect(requestTo(EXCHANGE_URL + "audit")).andExpect(method(HttpMethod.PUT)).andRespond(withSuccess());
        mockServer.expect(requestTo(EXCHANGE_URL + "DLX")).andExpect(method(HttpMethod.DELETE)).andRespond(withSuccess());
        mockServer.expect(requestTo(EXCHANGE_URL + "DLX")).andExpect(method(HttpMethod.PUT)).andRespond(withSuccess());
        
        healthChecker.recover();
        
        mockServer.verify();
        mockServer.reset();
        
        // finally run a health check and return the correct configuration to show that our status changes
        goodHealthCheck();
    }
    
    @Test
    @DirtiesContext
    public void invalidExchangeAfterHealthyTest() throws Exception {
        goodHealthCheck();
        
        // missing runHealthCheck() calls
        mockServer.expect(requestTo(NODES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyNodeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(EXCHANGES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getInvalidExchangeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyQueueInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(BINDINGS_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyBindingInfo()), MediaType.APPLICATION_JSON));
        
        // missing health() calls
        mockServer.expect(requestTo(QUEUE_URL + "audit.log")).andRespond(
                        withSuccess(mapper.writeValueAsString(createAuditQueue("audit.log", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andRespond(withSuccess(
                        mapper.writeValueAsString(createAuditDeadLetterQueue("audit.log.dlq", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        
        // unhealthy call
        healthChecker.runHealthCheck();
        
        // ends up unhealthy with an outage stat
        Assert.assertFalse(healthChecker.isHealthy());
        Assert.assertEquals(RabbitHealthChecker.RABBITMQ_UNHEALTHY, healthChecker.health().getStatus());
        Assert.assertEquals(rabbitHealthProperties.getUnhealthyPollIntervalMillis(), healthChecker.pollIntervalMillis());
        Assert.assertEquals(1, healthChecker.getOutageStats().size());
        
        Assert.assertEquals("current", healthChecker.getOutageStats().get(0).get("stopDate"));
        Assert.assertEquals("[DLX, audit]", healthChecker.getOutageStats().get(0).get("invalidExchanges").toString());
        
        mockServer.verify();
        mockServer.reset();
        
        // test recovery
        // recovery turned off
        rabbitHealthProperties.setAttemptRecovery(false);
        healthChecker.recover();
        Assert.assertFalse(healthChecker.isHealthy());
        
        // recovery turned on
        rabbitHealthProperties.setAttemptRecovery(true);
        
        mockServer.expect(requestTo(EXCHANGE_URL + "audit")).andExpect(method(HttpMethod.DELETE)).andRespond(withSuccess());
        mockServer.expect(requestTo(EXCHANGE_URL + "audit")).andExpect(method(HttpMethod.PUT)).andRespond(withSuccess());
        mockServer.expect(requestTo(EXCHANGE_URL + "DLX")).andExpect(method(HttpMethod.DELETE)).andRespond(withSuccess());
        mockServer.expect(requestTo(EXCHANGE_URL + "DLX")).andExpect(method(HttpMethod.PUT)).andRespond(withSuccess());
        
        healthChecker.recover();
        
        mockServer.verify();
        mockServer.reset();
        
        // finally run a health check and return the correct configuration to show that our status changes
        goodHealthCheck(1);
    }
    
    @Test
    @DirtiesContext
    public void missingBindingAtInitTest() throws Exception {
        // runHealthCheck() calls
        mockServer.expect(requestTo(NODES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyNodeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(EXCHANGES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyExchangeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyQueueInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(BINDINGS_URL)).andRespond(withSuccess(mapper.writeValueAsString(getMissingBindingInfo()), MediaType.APPLICATION_JSON));
        
        // health() calls
        mockServer.expect(requestTo(QUEUE_URL + "audit.log")).andRespond(
                        withSuccess(mapper.writeValueAsString(createAuditQueue("audit.log", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andRespond(withSuccess(
                        mapper.writeValueAsString(createAuditDeadLetterQueue("audit.log.dlq", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        
        healthChecker.runHealthCheck();
        
        Assert.assertFalse(healthChecker.isHealthy());
        Assert.assertEquals(RabbitHealthChecker.RABBITMQ_UNHEALTHY, healthChecker.health().getStatus());
        
        mockServer.verify();
        mockServer.reset();
        
        Assert.assertEquals(rabbitHealthProperties.getUnhealthyPollIntervalMillis(), healthChecker.pollIntervalMillis());
        Assert.assertEquals(0, healthChecker.getOutageStats().size());
        
        // test recovery
        // recovery turned off
        rabbitHealthProperties.setAttemptRecovery(false);
        healthChecker.recover();
        Assert.assertFalse(healthChecker.isHealthy());
        
        // recovery turned on
        rabbitHealthProperties.setAttemptRecovery(true);
        
        mockServer.expect(requestTo(BINDING_URL + "e/audit/q/audit.log")).andExpect(method(HttpMethod.POST)).andRespond(withSuccess());
        
        healthChecker.recover();
        
        mockServer.verify();
        mockServer.reset();
        
        // finally run a health check and return the correct configuration to show that our status changes
        goodHealthCheck();
    }
    
    @Test
    @DirtiesContext
    public void missingBindingAfterHealthyTest() throws Exception {
        goodHealthCheck();
        
        // missing runHealthCheck() calls
        mockServer.expect(requestTo(NODES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyNodeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(EXCHANGES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyExchangeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyQueueInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(BINDINGS_URL)).andRespond(withSuccess(mapper.writeValueAsString(getMissingBindingInfo()), MediaType.APPLICATION_JSON));
        
        // missing health() calls
        mockServer.expect(requestTo(QUEUE_URL + "audit.log")).andRespond(
                        withSuccess(mapper.writeValueAsString(createAuditQueue("audit.log", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andRespond(withSuccess(
                        mapper.writeValueAsString(createAuditDeadLetterQueue("audit.log.dlq", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        
        // unhealthy call
        healthChecker.runHealthCheck();
        
        // ends up unhealthy with an outage stat
        Assert.assertFalse(healthChecker.isHealthy());
        Assert.assertEquals(RabbitHealthChecker.RABBITMQ_UNHEALTHY, healthChecker.health().getStatus());
        Assert.assertEquals(rabbitHealthProperties.getUnhealthyPollIntervalMillis(), healthChecker.pollIntervalMillis());
        Assert.assertEquals(1, healthChecker.getOutageStats().size());
        
        Assert.assertEquals("current", healthChecker.getOutageStats().get(0).get("stopDate"));
        Assert.assertEquals("{audit=[audit.log]}", healthChecker.getOutageStats().get(0).get("missingBindings").toString());
        
        mockServer.verify();
        mockServer.reset();
        
        // test recovery
        // recovery turned off
        rabbitHealthProperties.setAttemptRecovery(false);
        healthChecker.recover();
        Assert.assertFalse(healthChecker.isHealthy());
        
        // recovery turned on
        rabbitHealthProperties.setAttemptRecovery(true);
        
        mockServer.expect(requestTo(BINDING_URL + "e/audit/q/audit.log")).andExpect(method(HttpMethod.POST)).andRespond(withSuccess());
        
        healthChecker.recover();
        
        mockServer.verify();
        mockServer.reset();
        
        // finally run a health check and return the correct configuration to show that our status changes
        goodHealthCheck(1);
    }
    
    @Test
    @DirtiesContext
    public void invalidBindingAtInitTest() throws Exception {
        // runHealthCheck() calls
        mockServer.expect(requestTo(NODES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyNodeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(EXCHANGES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyExchangeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyQueueInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(BINDINGS_URL)).andRespond(withSuccess(mapper.writeValueAsString(getInvalidBindingInfo()), MediaType.APPLICATION_JSON));
        
        // health() calls
        mockServer.expect(requestTo(QUEUE_URL + "audit.log")).andRespond(
                        withSuccess(mapper.writeValueAsString(createAuditQueue("audit.log", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andRespond(withSuccess(
                        mapper.writeValueAsString(createAuditDeadLetterQueue("audit.log.dlq", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        
        healthChecker.runHealthCheck();
        
        Assert.assertFalse(healthChecker.isHealthy());
        Assert.assertEquals(RabbitHealthChecker.RABBITMQ_UNHEALTHY, healthChecker.health().getStatus());
        
        mockServer.verify();
        mockServer.reset();
        
        Assert.assertEquals(rabbitHealthProperties.getUnhealthyPollIntervalMillis(), healthChecker.pollIntervalMillis());
        Assert.assertEquals(0, healthChecker.getOutageStats().size());
        
        // test recovery
        // recovery turned off
        rabbitHealthProperties.setAttemptRecovery(false);
        healthChecker.recover();
        Assert.assertFalse(healthChecker.isHealthy());
        
        // recovery turned on
        rabbitHealthProperties.setAttemptRecovery(true);
        
        mockServer.expect(requestTo(BINDING_URL + "e/audit/e/audit.log/lalaland")).andExpect(method(HttpMethod.DELETE)).andRespond(withSuccess());
        mockServer.expect(requestTo(BINDING_URL + "e/audit/q/audit.log")).andExpect(method(HttpMethod.POST)).andRespond(withSuccess());
        mockServer.expect(requestTo(BINDING_URL + "e/DLX/q/audit.log.dlq/say.what")).andExpect(method(HttpMethod.DELETE)).andRespond(withSuccess());
        mockServer.expect(requestTo(BINDING_URL + "e/DLX/q/audit.log.dlq")).andExpect(method(HttpMethod.POST)).andRespond(withSuccess());
        
        healthChecker.recover();
        
        mockServer.verify();
        mockServer.reset();
        
        // finally run a health check and return the correct configuration to show that our status changes
        goodHealthCheck();
    }
    
    @Test
    @DirtiesContext
    public void invalidBindingAfterHealthyTest() throws Exception {
        goodHealthCheck();
        
        // missing runHealthCheck() calls
        mockServer.expect(requestTo(NODES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyNodeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(EXCHANGES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyExchangeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyQueueInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(BINDINGS_URL)).andRespond(withSuccess(mapper.writeValueAsString(getInvalidBindingInfo()), MediaType.APPLICATION_JSON));
        
        // missing health() calls
        mockServer.expect(requestTo(QUEUE_URL + "audit.log")).andRespond(
                        withSuccess(mapper.writeValueAsString(createAuditQueue("audit.log", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andRespond(withSuccess(
                        mapper.writeValueAsString(createAuditDeadLetterQueue("audit.log.dlq", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        
        // unhealthy call
        healthChecker.runHealthCheck();
        
        // ends up unhealthy with an outage stat
        Assert.assertFalse(healthChecker.isHealthy());
        Assert.assertEquals(RabbitHealthChecker.RABBITMQ_UNHEALTHY, healthChecker.health().getStatus());
        Assert.assertEquals(rabbitHealthProperties.getUnhealthyPollIntervalMillis(), healthChecker.pollIntervalMillis());
        Assert.assertEquals(1, healthChecker.getOutageStats().size());
        
        Assert.assertEquals("current", healthChecker.getOutageStats().get(0).get("stopDate"));
        Assert.assertEquals("{DLX=[audit.log.dlq], audit=[audit.log]}", healthChecker.getOutageStats().get(0).get("invalidBindings").toString());
        
        mockServer.verify();
        mockServer.reset();
        
        // test recovery
        // recovery turned off
        rabbitHealthProperties.setAttemptRecovery(false);
        healthChecker.recover();
        Assert.assertFalse(healthChecker.isHealthy());
        
        // recovery turned on
        rabbitHealthProperties.setAttemptRecovery(true);
        
        mockServer.expect(requestTo(BINDING_URL + "e/audit/e/audit.log/lalaland")).andExpect(method(HttpMethod.DELETE)).andRespond(withSuccess());
        mockServer.expect(requestTo(BINDING_URL + "e/audit/q/audit.log")).andExpect(method(HttpMethod.POST)).andRespond(withSuccess());
        mockServer.expect(requestTo(BINDING_URL + "e/DLX/q/audit.log.dlq/say.what")).andExpect(method(HttpMethod.DELETE)).andRespond(withSuccess());
        mockServer.expect(requestTo(BINDING_URL + "e/DLX/q/audit.log.dlq")).andExpect(method(HttpMethod.POST)).andRespond(withSuccess());
        
        healthChecker.recover();
        
        mockServer.verify();
        mockServer.reset();
        
        // finally run a health check and return the correct configuration to show that our status changes
        goodHealthCheck(1);
    }
    
    private void goodHealthCheck() throws Exception {
        goodHealthCheck(0);
    }
    
    private void goodHealthCheck(int numOutages) throws Exception {
        // healthy runHealthCheck() calls
        mockServer.expect(requestTo(NODES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyNodeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(EXCHANGES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyExchangeInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUES_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyQueueInfo()), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(BINDINGS_URL)).andRespond(withSuccess(mapper.writeValueAsString(getHealthyBindingInfo()), MediaType.APPLICATION_JSON));
        
        // healthy health() calls
        mockServer.expect(requestTo(QUEUE_URL + "audit.log")).andRespond(
                        withSuccess(mapper.writeValueAsString(createAuditQueue("audit.log", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        mockServer.expect(requestTo(QUEUE_URL + "audit.log.dlq")).andRespond(withSuccess(
                        mapper.writeValueAsString(createAuditDeadLetterQueue("audit.log.dlq", true, false, false, 0, 0)), MediaType.APPLICATION_JSON));
        
        // healthy call
        healthChecker.runHealthCheck();
        
        // starts out healthy
        Assert.assertTrue(healthChecker.isHealthy());
        Assert.assertEquals(Status.UP, healthChecker.health().getStatus());
        Assert.assertEquals(rabbitHealthProperties.getHealthyPollIntervalMillis(), healthChecker.pollIntervalMillis());
        Assert.assertEquals(numOutages, healthChecker.getOutageStats().size());
        
        mockServer.verify();
        mockServer.reset();
    }
    
    private NodeInfo[] getHealthyNodeInfo() {
        return new NodeInfo[] {createNode("rabbit1@rabbit1", true), createNode("rabbit2@rabbit2", true), createNode("rabbit3@rabbit3", true)};
    }
    
    private NodeInfo[] getMissingNodeInfo() {
        return new NodeInfo[] {createNode("rabbit1@rabbit1", true), createNode("rabbit2@rabbit2", true)};
    }
    
    private NodeInfo[] getStoppedNodeInfo() {
        return new NodeInfo[] {createNode("rabbit1@rabbit1", false), createNode("rabbit2@rabbit2", true), createNode("rabbit3@rabbit3", true)};
    }
    
    private NodeInfo createNode(String name, boolean running) {
        NodeInfo node = new NodeInfo();
        node.setName(name);
        node.setRunning(running);
        node.setType("disc");
        return node;
    }
    
    private ExchangeInfo[] getHealthyExchangeInfo() {
        return new ExchangeInfo[] {createExchange("audit", "topic", true, false, false), createExchange("DLX", "direct", true, false, false)};
    }
    
    private ExchangeInfo[] getMissingExchangeInfo() {
        return new ExchangeInfo[] {createExchange("DLX", "direct", true, false, false)};
    }
    
    private ExchangeInfo[] getInvalidExchangeInfo() {
        return new ExchangeInfo[] {createExchange("audit", "fanout", false, true, false), createExchange("DLX", "topic", true, false, true)};
    }
    
    private ExchangeInfo createExchange(String name, String type, boolean durable, boolean autoDelete, boolean internal) {
        ExchangeInfo exchange = new ExchangeInfo();
        exchange.setName(name);
        exchange.setType(type);
        exchange.setDurable(durable);
        exchange.setAutoDelete(autoDelete);
        exchange.setInternal(internal);
        return exchange;
    }
    
    private QueueInfo[] getHealthyQueueInfo() {
        return new QueueInfo[] {createAuditQueue("audit.log", true, false, false, 0, 0), createAuditDeadLetterQueue("audit.log.dlq", true, false, false, 0, 0)};
    }
    
    private QueueInfo[] getMissingQueueInfo() {
        return new QueueInfo[] {createAuditQueue("audit.log", true, false, false, 0, 0)};
    }
    
    private QueueInfo[] getFixableInvalidQueueInfo() {
        Map<String,Object> dlqArgs = new HashMap<>();
        dlqArgs.put("x-dead-letter-exchange", "BOGUS");
        dlqArgs.put("x-dead-letter-routing-key", "audit.log");
        return new QueueInfo[] {createQueue("audit.log", false, true, false, dlqArgs, 0, 0),
                createAuditDeadLetterQueue("audit.log.dlq", true, false, true, 0, 0)};
    }
    
    private QueueInfo[] getUnfixableInvalidQueueInfo() {
        Map<String,Object> dlqArgs = new HashMap<>();
        dlqArgs.put("x-dead-letter-exchange", "BOGUS");
        dlqArgs.put("x-dead-letter-routing-key", "audit.log");
        return new QueueInfo[] {createQueue("audit.log", false, true, false, dlqArgs, 7, 5),
                createAuditDeadLetterQueue("audit.log.dlq", true, false, true, 0, 0)};
    }
    
    private QueueInfo createAuditQueue(String name, boolean durable, boolean exclusive, boolean autoDelete, long unackedMessages, long readyMessages) {
        Map<String,Object> dlqArgs = new HashMap<>();
        dlqArgs.put("x-dead-letter-exchange", "DLX");
        dlqArgs.put("x-dead-letter-routing-key", name);
        return createQueue(name, durable, exclusive, autoDelete, dlqArgs, unackedMessages, readyMessages);
    }
    
    private QueueInfo createExtraArgsAuditQueue(String name, boolean durable, boolean exclusive, boolean autoDelete, long unackedMessages, long readyMessages) {
        Map<String,Object> dlqArgs = new HashMap<>();
        dlqArgs.put("x-dead-letter-exchange", "DLX");
        dlqArgs.put("x-dead-letter-routing-key", name);
        dlqArgs.put("extraArg", "dontCare");
        return createQueue(name, durable, exclusive, autoDelete, dlqArgs, unackedMessages, readyMessages);
    }
    
    private QueueInfo createAuditDeadLetterQueue(String name, boolean durable, boolean exclusive, boolean autoDelete, long unackedMessages,
                    long readyMessages) {
        return createQueue(name, durable, exclusive, autoDelete, null, unackedMessages, readyMessages);
    }
    
    private QueueInfo createQueue(String name, boolean durable, boolean exclusive, boolean autoDelete, Map<String,Object> arguments, long unackedMessages,
                    long readyMessages) {
        QueueInfo queue = new QueueInfo();
        queue.setName(name);
        queue.setDurable(durable);
        queue.setExclusive(exclusive);
        queue.setAutoDelete(autoDelete);
        if (arguments != null)
            queue.setArguments(arguments);
        queue.setTotalMessages(unackedMessages + readyMessages);
        queue.setMessagesUnacknowledged(unackedMessages);
        queue.setMessagesReady(readyMessages);
        return queue;
    }
    
    private BindingInfo createBinding(String destination, String destinationType, String source, String routingKey, Map<String,Object> arguments) {
        BindingInfo binding = new BindingInfo();
        binding.setDestination(destination);
        binding.setDestinationType(destinationType);
        binding.setSource(source);
        binding.setRoutingKey(routingKey);
        if (arguments != null)
            binding.setArguments(arguments);
        return binding;
    }
    
    private BindingInfo[] getHealthyBindingInfo() {
        return new BindingInfo[] {createBinding("audit.log", "queue", "audit", "#", null), createBinding("audit.log.dlq", "queue", "DLX", "audit.log", null)};
    }
    
    private BindingInfo[] getMissingBindingInfo() {
        return new BindingInfo[] {createBinding("audit.log.dlq", "queue", "DLX", "audit.log", null)};
    }
    
    private BindingInfo[] getInvalidBindingInfo() {
        return new BindingInfo[] {createBinding("audit.log", "exchange", "audit", "lalaland", null),
                createBinding("audit.log.dlq", "queue", "DLX", "say.what", null)};
    }
    
    @Configuration
    @Profile("RabbitHealthTest")
    @EnableConfigurationProperties(RabbitHealthProperties.class)
    public static class RabbitHealthCheckerTestConfiguration {}
}
