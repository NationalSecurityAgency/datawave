package datawave.microservice.audit;

import datawave.marking.ColumnVisibilitySecurityMarking;
import datawave.marking.SecurityMarking;
import datawave.microservice.audit.config.AuditServiceConfiguration;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
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
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
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
    
    private static final String EXPECTED_AUDIT_URI = "http://localhost:11111/audit/v1/audit";
    
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    
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
        parameters.add(TestAuditParameters.TEST_PARAM_1, "tp1Value");
        parameters.add(TestAuditParameters.TEST_PARAM_2, "tp2Value");
        
        //@formatter:off
        final AuditClient.Request auditRequest = new AuditClient.Request.Builder()
                .withParams(parameters)
                .withQueryExpression("FIELD:VALUE1 OR FIELD:VALUE2")
                .withProxiedUserDetails(defaultUserDetails)
                .withMarking(auditTestSecurityMarking)
                .withAuditType(Auditor.AuditType.LOCALONLY)
                .withQueryLogic("QueryLogic")
                .build();

        mockServer.expect(requestTo(EXPECTED_AUDIT_URI))
                .andExpect(content().formData(auditRequest.paramMap))
                .andRespond(withSuccess());

        auditClient.submit(auditRequest);
        mockServer.verify();

        //@formatter:on
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testMissingTestParam2() throws Exception {
        
        MultiValueMap<String,String> parameters = new LinkedMultiValueMap<>();
        parameters.add(TestAuditParameters.TEST_PARAM_1, "tp1Value");
        
        // Missing TestAuditParameters.TEST_PARAM_2 this time
        
        //@formatter:off
        final AuditClient.Request auditRequest = new AuditClient.Request.Builder()
                .withParams(parameters)
                .withQueryExpression("FIELD:VALUE1 OR FIELD:VALUE2")
                .withProxiedUserDetails(defaultUserDetails)
                .withMarking(auditTestSecurityMarking)
                .withAuditType(Auditor.AuditType.LOCALONLY)
                .withQueryLogic("QueryLogic")
                .build();
        //@formatter:on
        AuditClient.validate(auditRequest, new TestAuditParameters());
    }
    
    @Test
    public void testAuditURIServerError() {
        expectedException.expect(HttpServerErrorException.class);
        expectedException.expect(new TestUtils.StatusMatcher(500));
        
        MultiValueMap<String,String> parameters = new LinkedMultiValueMap<>();
        parameters.add("paramFoo", "paramFooValue");
        parameters.add("paramBar", "paramBarValue");
        
        //@formatter:off
        final AuditClient.Request auditRequest = new AuditClient.Request.Builder()
                .withQueryExpression("FIELD:VALUE1 OR FIELD:VALUE2")
                .withProxiedUserDetails(defaultUserDetails)
                .withMarking(auditTestSecurityMarking)
                .withAuditType(Auditor.AuditType.LOCALONLY)
                .withQueryLogic("QueryLogic")
                .withParams(parameters)
                .build();

        mockServer.expect(requestTo(EXPECTED_AUDIT_URI))
                .andExpect(content().formData(auditRequest.paramMap))
                .andRespond(withServerError());

        auditClient.submit(auditRequest);
        mockServer.verify();

        //@formatter:on
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
        //@formatter:on
        AuditClient.validate(auditRequest, new AuditParameters());
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
        //@formatter:on
        AuditClient.validate(auditRequest, new AuditParameters());
    }
    
    /**
     * Mocks the AuditClient jwtRestTemplate field within the internal AuditClient
     */
    private void setupMockAuditServer() {
        RestTemplate auditorRestTemplate = (RestTemplate) new DirectFieldAccessor(auditClient).getPropertyValue("jwtRestTemplate");
        mockServer = MockRestServiceServer.createServer(auditorRestTemplate);
    }
    
    public static class TestAuditParameters extends AuditParameters {
        public static final String TEST_PARAM_1 = "PARAM1";
        public static final String TEST_PARAM_2 = "PARAM2";
        private static final List<String> REQUIRED_TEST_PARAMS = Arrays.asList(TEST_PARAM_1, TEST_PARAM_2);
        
        @Override
        public void validate(Map<String,List<String>> parameters) throws IllegalArgumentException {
            super.validate(parameters);
            for (String param : REQUIRED_TEST_PARAMS) {
                List<String> values = parameters.get(param);
                if (null == values) {
                    throw new IllegalArgumentException("Required parameter " + param + " not found");
                }
            }
        }
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
