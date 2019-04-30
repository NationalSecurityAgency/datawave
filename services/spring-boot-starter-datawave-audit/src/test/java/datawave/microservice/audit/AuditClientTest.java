package datawave.microservice.audit;

import datawave.marking.ColumnVisibilitySecurityMarking;
import datawave.marking.SecurityMarking;
import datawave.microservice.audit.config.AuditServiceConfiguration;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.webservice.common.audit.Auditor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withServerError;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

/**
 * Tests {@link AuditClient} and {@link AuditClient.Request} functionality and ensures that audit {@code audit.enabled=true})
 * <p>
 * Utilizes mocked audit server to verify that expected REST calls are made
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@ContextConfiguration(classes = AuditClientTest.TestConfiguration.class)
@ActiveProfiles({"AuditClientTest", "audit-enabled"})
public class AuditClientTest {
    
    private static final String AUDIT_BASE_URI = "http://localhost:11111/audit/v1/audit";
    
    @Autowired
    private AuditClient auditClient;
    
    @Autowired
    private SecurityMarking auditTestSecurityMarking;
    
    @Autowired
    private ApplicationContext context;
    
    private MockRestServiceServer mockServer;
    private ProxiedUserDetails defaultUserDetails;
    
    @Before
    public void setup() throws Exception {
        defaultUserDetails = TestUtils.userDetails(Collections.singleton("AuthorizedUser"), Arrays.asList("A", "B", "C", "D", "E", "F", "G", "H", "I"));
        setupMockAuditServer();
    }
    
    @Test
    public void verifyAutoConfig() {
        assertEquals("One AuditClient bean should have been found", 1, context.getBeanNamesForType(AuditClient.class).length);
        assertEquals("One AuditServiceConfiguration bean should have been found", 1, context.getBeanNamesForType(AuditServiceConfiguration.class).length);
        assertEquals("One AuditServiceProvider bean should have been found", 1, context.getBeanNamesForType(AuditServiceProvider.class).length);
    }
    
    @Test
    public void testAuditURISuccess() throws Exception {
        
        MultiValueMap<String,String> parameters = new LinkedMultiValueMap<>();
        parameters.add("paramFoo", "paramFooValue");
        parameters.add("paramBar", "paramBarValue");
        
        //@formatter:off
        final AuditClient.Request auditRequest = new AuditClient.Request.Builder()
                .withParams(parameters)
                .withQueryExpression("FIELD:VALUE1 OR FIELD:VALUE2")
                .withProxiedUserDetails(defaultUserDetails)
                .withMarking(auditTestSecurityMarking)
                .withAuditType(Auditor.AuditType.LOCALONLY)
                .withQueryLogic("QueryLogic")
                .withParams(parameters)
                .build();

        String expectedAuditUri = AUDIT_BASE_URI +
            "?paramFoo=paramFooValue&paramBar=paramBarValue&query=FIELD:VALUE1%20OR%20FIELD:VALUE2&auths=[A,%20B,%20C,%20D,%20E,%20F,%20G,%20H,%20I]&auditUserDN=userdn%3Cissuerdn%3E&auditType=LOCALONLY&auditColumnVisibility=BAR%7CFOO&logicClass=QueryLogic";

        //@formatter:on
        
        mockServer.expect(requestTo(expectedAuditUri)).andRespond(withSuccess());
        auditClient.submit(auditRequest);
        mockServer.verify();
    }
    
    @Test(expected = RuntimeException.class)
    public void testAuditURIServerError() throws Exception {
        
        MultiValueMap<String,String> parameters = new LinkedMultiValueMap<>();
        parameters.add("paramFoo", "paramFooValue");
        parameters.add("paramBar", "paramBarValue");
        
        //@formatter:off
        final AuditClient.Request auditRequest = new AuditClient.Request.Builder()
                .withParams(parameters)
                .withQueryExpression("FIELD:VALUE1 OR FIELD:VALUE2")
                .withProxiedUserDetails(defaultUserDetails)
                .withMarking(auditTestSecurityMarking)
                .withAuditType(Auditor.AuditType.LOCALONLY)
                .withQueryLogic("QueryLogic")
                .withParams(parameters)
                .build();

        String expectedAuditUri = AUDIT_BASE_URI +
                "?paramFoo=paramFooValue&paramBar=paramBarValue&query=FIELD:VALUE1%20OR%20FIELD:VALUE2&auths=[A,%20B,%20C,%20D,%20E,%20F,%20G,%20H,%20I]&auditUserDN=userdn%3Cissuerdn%3E&auditType=LOCALONLY&auditColumnVisibility=BAR%7CFOO&logicClass=QueryLogic";

        //@formatter:on
        
        mockServer.expect(requestTo(expectedAuditUri)).andRespond(withServerError());
        auditClient.submit(auditRequest);
        mockServer.verify();
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testBuildMissingAuditParams1() throws Exception {
        
        // No AuditType specified this time
        
        //@formatter:off
        final AuditClient.Request auditRequest = new AuditClient.Request.Builder()
                .withQueryExpression("FIELD:VALUE1 OR FIELD:VALUE2")
                .withProxiedUserDetails(defaultUserDetails)
                .withMarking(auditTestSecurityMarking)
                .withQueryLogic("QueryLogic")
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildMissingAuditParams2() throws Exception {

        // No query specified this time

        //@formatter:off
        final AuditClient.Request auditRequest = new AuditClient.Request.Builder()
                .withAuditType(Auditor.AuditType.PASSIVE)
                .withProxiedUserDetails(defaultUserDetails)
                .withMarking(auditTestSecurityMarking)
                .withQueryLogic("QueryLogic")
                .build();
    }
    
    /**
     * Mocks the AuditClient jwtRestTemplate field within the internal AuditClient
     */
    private void setupMockAuditServer() {
        RestTemplate auditorRestTemplate = (RestTemplate) new DirectFieldAccessor(auditClient).getPropertyValue("jwtRestTemplate");
        mockServer = MockRestServiceServer.createServer(auditorRestTemplate);
    }
    
    @Configuration
    @Profile("AuditClientTest")
    @ComponentScan(basePackages = "datawave.microservice")
    public static class TestConfiguration {
        
        @Bean
        public SecurityMarking auditTestSecurityMarking() {
            ColumnVisibilitySecurityMarking auditCVSM = new ColumnVisibilitySecurityMarking();
            auditCVSM.setColumnVisibility("BAR|FOO");
            return auditCVSM;
        }
        
    }
    
    @SpringBootApplication(scanBasePackages = "datawave.microservice")
    public static class TestApplication {
        public static void main(String[] args) {
            SpringApplication.run(AuditClientTest.TestApplication.class, args);
        }
    }
}
