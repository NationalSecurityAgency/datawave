package datawave.microservice.audit.replay;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withServerError;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import datawave.microservice.audit.AuditServiceProvider;
import datawave.microservice.audit.TestUtils;
import datawave.microservice.audit.config.AuditServiceConfiguration;
import datawave.microservice.authorization.user.DatawaveUserDetails;

/**
 * Tests {@link ReplayClient} and {@link ReplayClient.Request} functionality and ensures that audit {@code audit.enabled=true})
 * <p>
 * Utilizes mocked audit server to verify that expected REST calls are made
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest
@ContextConfiguration(classes = ReplayClientTest.TestConfiguration.class)
@ActiveProfiles({"ReplayClientTest", "audit-enabled"})
public class ReplayClientTest {
    
    private static final String EXPECTED_REPLAY_URI = "http://localhost:11111/audit/v1/replay";
    
    @Autowired
    private ReplayClient replayClient;
    
    @Autowired
    private ApplicationContext context;
    
    private MockRestServiceServer mockServer;
    private DatawaveUserDetails defaultUserDetails;
    
    @BeforeEach
    public void setup() throws Exception {
        defaultUserDetails = TestUtils.userDetails(Collections.singleton("AuthorizedUser"), Arrays.asList("A", "B", "C", "D", "E", "F", "G", "H", "I"));
        setupMockAuditServer();
    }
    
    @Test
    public void verifyAutoConfig() {
        assertEquals(1, context.getBeanNamesForType(ReplayClient.class).length, "One ReplayClient bean should have been found");
        assertEquals(1, context.getBeanNamesForType(AuditServiceConfiguration.class).length, "One AuditServiceConfiguration bean should have been found");
        assertEquals(1, context.getBeanNamesForType(AuditServiceProvider.class).length, "One AuditServiceProvider bean should have been found");
    }
    
    @Test
    public void testCreateURISuccess() {
        
        //@formatter:off
        final ReplayClient.Request replayRequest = new ReplayClient.Request.Builder()
                .withDatawaveUserDetails(defaultUserDetails)
                .withPathUri("hdfs://some-path/")
                .withSendRate(100l)
                .withReplayUnfinishedFiles(true)
                .build();

        mockServer.expect(requestTo(EXPECTED_REPLAY_URI + "/create"))
                .andExpect(content().formData(replayRequest.paramMap))
                .andRespond(withSuccess());

        replayClient.create(replayRequest);
        mockServer.verify();
        //@formatter:on
    }
    
    @Test
    public void testCreateMissingParam() {
        
        //@formatter:off
        final ReplayClient.Request replayRequest = new ReplayClient.Request.Builder()
                .withDatawaveUserDetails(defaultUserDetails)
                .withSendRate(100l)
                .withReplayUnfinishedFiles(true)
                .build();

        mockServer.expect(requestTo(EXPECTED_REPLAY_URI + "/create"))
                .andExpect(content().formData(replayRequest.paramMap))
                .andRespond(withSuccess());

        assertThrows(NullPointerException.class, () -> {
            replayClient.create(replayRequest);
            mockServer.verify();
        });
        //@formatter:on
    }
    
    @Test
    public void testCreateURIServerError() {
        //@formatter:off
        final ReplayClient.Request replayRequest = new ReplayClient.Request.Builder()
                .withDatawaveUserDetails(defaultUserDetails)
                .withPathUri("hdfs://some-path/")
                .withSendRate(100l)
                .withReplayUnfinishedFiles(true)
                .build();

        mockServer.expect(requestTo(EXPECTED_REPLAY_URI + "/create"))
                .andExpect(content().formData(replayRequest.paramMap))
                .andRespond(withServerError());

        assertThrows(HttpServerErrorException.InternalServerError.class, () ->replayClient.create(replayRequest));
        mockServer.verify();

        //@formatter:on
    }
    
    @Test
    public void testCreateAndStartURISuccess() {
        
        //@formatter:off
        final ReplayClient.Request replayRequest = new ReplayClient.Request.Builder()
                .withDatawaveUserDetails(defaultUserDetails)
                .withPathUri("hdfs://some-path/")
                .withSendRate(100l)
                .withReplayUnfinishedFiles(true)
                .build();

        mockServer.expect(requestTo(EXPECTED_REPLAY_URI + "/createAndStart"))
                .andExpect(content().formData(replayRequest.paramMap))
                .andRespond(withSuccess());

        replayClient.createAndStart(replayRequest);
        mockServer.verify();
        //@formatter:on
    }
    
    @Test
    public void testCreateAndStartMissingParam() {
        
        //@formatter:off
        final ReplayClient.Request replayRequest = new ReplayClient.Request.Builder()
                .withDatawaveUserDetails(defaultUserDetails)
                .withSendRate(100l)
                .withReplayUnfinishedFiles(true)
                .build();

        mockServer.expect(requestTo(EXPECTED_REPLAY_URI + "/createAndStart"))
                .andExpect(content().formData(replayRequest.paramMap))
                .andRespond(withSuccess());

        assertThrows(NullPointerException.class, () -> {
            replayClient.createAndStart(replayRequest);
            mockServer.verify();
        });
        //@formatter:on
    }
    
    @Test
    public void testStartURISuccess() {
        String id = "some-id";
        
        //@formatter:off
        final ReplayClient.Request replayRequest = new ReplayClient.Request.Builder()
                .withDatawaveUserDetails(defaultUserDetails)
                .withId("some-id")
                .build();

        mockServer.expect(requestTo(EXPECTED_REPLAY_URI + "/" + id + "/start"))
                .andRespond(withSuccess());

        replayClient.start(replayRequest);
        mockServer.verify();
        //@formatter:on
    }
    
    @Test
    public void testStartMissingParam() {
        String id = "some-id";
        
        //@formatter:off
        final ReplayClient.Request replayRequest = new ReplayClient.Request.Builder()
                .withDatawaveUserDetails(defaultUserDetails)
                .build();

        mockServer.expect(requestTo(EXPECTED_REPLAY_URI + "/" + id + "/start"))
                .andRespond(withSuccess());

        assertThrows(NullPointerException.class, () -> {
            replayClient.start(replayRequest);
            mockServer.verify();
        });
        //@formatter:on
    }
    
    @Test
    public void testStartAllURISuccess() {
        
        //@formatter:off
        final ReplayClient.Request replayRequest = new ReplayClient.Request.Builder()
                .withDatawaveUserDetails(defaultUserDetails)
                .build();

        mockServer.expect(requestTo(EXPECTED_REPLAY_URI + "/startAll"))
                .andRespond(withSuccess());

        replayClient.startAll(replayRequest);
        mockServer.verify();
        //@formatter:on
    }
    
    @Test
    public void testStatusURISuccess() {
        String id = "some-id";
        
        //@formatter:off
        final ReplayClient.Request replayRequest = new ReplayClient.Request.Builder()
                .withDatawaveUserDetails(defaultUserDetails)
                .withId("some-id")
                .build();

        mockServer.expect(requestTo(EXPECTED_REPLAY_URI + "/" + id + "/status"))
                .andRespond(withSuccess());

        replayClient.status(replayRequest);
        mockServer.verify();
        //@formatter:on
    }
    
    @Test
    public void testStatusMissingParam() {
        String id = "some-id";
        
        //@formatter:off
        final ReplayClient.Request replayRequest = new ReplayClient.Request.Builder()
                .withDatawaveUserDetails(defaultUserDetails)
                .build();

        mockServer.expect(requestTo(EXPECTED_REPLAY_URI + "/" + id + "/status"))
                .andRespond(withSuccess());

        assertThrows(NullPointerException.class, () -> {
            replayClient.status(replayRequest);
            mockServer.verify();
        });
        //@formatter:on
    }
    
    @Test
    public void testStatusAllURISuccess() {
        
        //@formatter:off
        final ReplayClient.Request replayRequest = new ReplayClient.Request.Builder()
                .withDatawaveUserDetails(defaultUserDetails)
                .build();

        mockServer.expect(requestTo(EXPECTED_REPLAY_URI + "/statusAll"))
                .andRespond(withSuccess());

        replayClient.statusAll(replayRequest);
        mockServer.verify();
        //@formatter:on
    }
    
    @Test
    public void testUpdateURISuccess() {
        String id = "some-id";
        
        //@formatter:off
        final ReplayClient.Request replayRequest = new ReplayClient.Request.Builder()
                .withDatawaveUserDetails(defaultUserDetails)
                .withId("some-id")
                .withSendRate(100l)
                .build();

        mockServer.expect(requestTo(EXPECTED_REPLAY_URI + "/" + id + "/update"))
                .andExpect(content().formData(replayRequest.paramMap))
                .andRespond(withSuccess());

        replayClient.update(replayRequest);
        mockServer.verify();
        //@formatter:on
    }
    
    @Test
    public void testUpdateMissingParam1() {
        String id = "some-id";
        
        //@formatter:off
        final ReplayClient.Request replayRequest = new ReplayClient.Request.Builder()
                .withDatawaveUserDetails(defaultUserDetails)
                .withId(id)
                .build();

        mockServer.expect(requestTo(EXPECTED_REPLAY_URI + "/" + id + "/update"))
                .andRespond(withSuccess());

        assertThrows(NullPointerException.class, () -> {
            replayClient.update(replayRequest);
            mockServer.verify();
        });
        //@formatter:on
    }
    
    @Test
    public void testUpdateMissingParam2() {
        String id = "some-id";
        
        //@formatter:off
        final ReplayClient.Request replayRequest = new ReplayClient.Request.Builder()
                .withDatawaveUserDetails(defaultUserDetails)
                .withSendRate(100l)
                .build();

        mockServer.expect(requestTo(EXPECTED_REPLAY_URI + "/" + id + "/update"))
                .andExpect(content().formData(replayRequest.paramMap))
                .andRespond(withSuccess());

        assertThrows(NullPointerException.class, () -> {
            replayClient.update(replayRequest);
            mockServer.verify();
        });

        //@formatter:on
    }
    
    @Test
    public void testUpdateAllURISuccess() {
        
        //@formatter:off
        final ReplayClient.Request replayRequest = new ReplayClient.Request.Builder()
                .withDatawaveUserDetails(defaultUserDetails)
                .withSendRate(100l)
                .build();

        mockServer.expect(requestTo(EXPECTED_REPLAY_URI + "/updateAll"))
                .andExpect(content().formData(replayRequest.paramMap))
                .andRespond(withSuccess());

        replayClient.updateAll(replayRequest);
        mockServer.verify();
        //@formatter:on
    }
    
    @Test
    public void testUpdateAllMissingParam() {
        
        //@formatter:off
        final ReplayClient.Request replayRequest = new ReplayClient.Request.Builder()
                .withDatawaveUserDetails(defaultUserDetails)
                .build();

        mockServer.expect(requestTo(EXPECTED_REPLAY_URI + "/updateAll"))
                .andExpect(content().formData(replayRequest.paramMap))
                .andRespond(withSuccess());

        assertThrows(NullPointerException.class, () -> {
                    replayClient.updateAll(replayRequest);
                    mockServer.verify();
        });
            //@formatter:on
    }
    
    @Test
    public void testStopURISuccess() {
        String id = "some-id";
        
        //@formatter:off
        final ReplayClient.Request replayRequest = new ReplayClient.Request.Builder()
                .withDatawaveUserDetails(defaultUserDetails)
                .withId("some-id")
                .build();

        mockServer.expect(requestTo(EXPECTED_REPLAY_URI + "/" + id + "/stop"))
                .andRespond(withSuccess());

        replayClient.stop(replayRequest);
        mockServer.verify();
        //@formatter:on
    }
    
    @Test
    public void testStopMissingParam() {
        String id = "some-id";
        
        //@formatter:off
        final ReplayClient.Request replayRequest = new ReplayClient.Request.Builder()
                .withDatawaveUserDetails(defaultUserDetails)
                .build();

        mockServer.expect(requestTo(EXPECTED_REPLAY_URI + "/" + id + "/stop"))
                .andRespond(withSuccess());

        assertThrows(NullPointerException.class, () -> {
            replayClient.stop(replayRequest);
            mockServer.verify();
        });
        //@formatter:on
    }
    
    @Test
    public void testStopAllURISuccess() {
        
        //@formatter:off
        final ReplayClient.Request replayRequest = new ReplayClient.Request.Builder()
                .withDatawaveUserDetails(defaultUserDetails)
                .build();

        mockServer.expect(requestTo(EXPECTED_REPLAY_URI + "/stopAll"))
                .andRespond(withSuccess());

        replayClient.stopAll(replayRequest);
        mockServer.verify();
        //@formatter:on
    }
    
    @Test
    public void testResumeURISuccess() {
        String id = "some-id";
        
        //@formatter:off
        final ReplayClient.Request replayRequest = new ReplayClient.Request.Builder()
                .withDatawaveUserDetails(defaultUserDetails)
                .withId("some-id")
                .build();

        mockServer.expect(requestTo(EXPECTED_REPLAY_URI + "/" + id + "/resume"))
                .andRespond(withSuccess());

        replayClient.resume(replayRequest);
        mockServer.verify();
        //@formatter:on
    }
    
    @Test
    public void testResumeMissingParam() {
        String id = "some-id";
        
        //@formatter:off
        final ReplayClient.Request replayRequest = new ReplayClient.Request.Builder()
                .withDatawaveUserDetails(defaultUserDetails)
                .build();

        mockServer.expect(requestTo(EXPECTED_REPLAY_URI + "/" + id + "/resume"))
                .andRespond(withSuccess());

        assertThrows(NullPointerException.class, () -> {
            replayClient.resume(replayRequest);
            mockServer.verify();
        });
        //@formatter:on
    }
    
    @Test
    public void testResumeAllURISuccess() {
        
        //@formatter:off
        final ReplayClient.Request replayRequest = new ReplayClient.Request.Builder()
                .withDatawaveUserDetails(defaultUserDetails)
                .build();

        mockServer.expect(requestTo(EXPECTED_REPLAY_URI + "/resumeAll"))
                .andRespond(withSuccess());

        replayClient.resumeAll(replayRequest);
        mockServer.verify();
        //@formatter:on
    }
    
    @Test
    public void testDeleteURISuccess() {
        String id = "some-id";
        
        //@formatter:off
        final ReplayClient.Request replayRequest = new ReplayClient.Request.Builder()
                .withDatawaveUserDetails(defaultUserDetails)
                .withId("some-id")
                .build();

        mockServer.expect(requestTo(EXPECTED_REPLAY_URI + "/" + id + "/delete"))
                .andRespond(withSuccess());

        replayClient.delete(replayRequest);
        mockServer.verify();
        //@formatter:on
    }
    
    @Test
    public void testDeleteMissingParam() {
        String id = "some-id";
        
        //@formatter:off
        final ReplayClient.Request replayRequest = new ReplayClient.Request.Builder()
                .withDatawaveUserDetails(defaultUserDetails)
                .build();

        mockServer.expect(requestTo(EXPECTED_REPLAY_URI + "/" + id + "/delete"))
                .andRespond(withSuccess());

        assertThrows(NullPointerException.class, () -> {
            replayClient.delete(replayRequest);
            mockServer.verify();
        });
        //@formatter:on
    }
    
    @Test
    public void testDeleteAllURISuccess() {
        
        //@formatter:off
        final ReplayClient.Request replayRequest = new ReplayClient.Request.Builder()
                .withDatawaveUserDetails(defaultUserDetails)
                .build();

        mockServer.expect(requestTo(EXPECTED_REPLAY_URI + "/deleteAll"))
                .andRespond(withSuccess());

        replayClient.deleteAll(replayRequest);
        mockServer.verify();
        //@formatter:on
    }
    
    /**
     * Mocks the ReplayClient jwtRestTemplate field within the internal ReplayClient
     */
    private void setupMockAuditServer() {
        RestTemplate replayRestTemplate = (RestTemplate) new DirectFieldAccessor(replayClient).getPropertyValue("jwtRestTemplate");
        mockServer = MockRestServiceServer.createServer(replayRestTemplate);
    }
    
    @Configuration
    @Profile("ReplayClientTest")
    @ComponentScan(basePackages = "datawave.microservice")
    public static class TestConfiguration {}
    
    @SpringBootApplication(scanBasePackages = "datawave.microservice")
    public static class TestApplication {
        public static void main(String[] args) {
            SpringApplication.run(ReplayClientTest.TestApplication.class, args);
        }
    }
}
