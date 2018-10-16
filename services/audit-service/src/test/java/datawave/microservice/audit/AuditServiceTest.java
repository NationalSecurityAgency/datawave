package datawave.microservice.audit;

//import datawave.common.test.integration.IntegrationTest;
import datawave.microservice.audit.common.AuditMessage;
import datawave.microservice.audit.config.AuditServiceConfig;
import datawave.microservice.audit.health.HealthChecker;
import datawave.microservice.authorization.jwt.JWTRestTemplate;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor.AuditType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.Message;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static datawave.security.authorization.DatawaveUser.UserType.USER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

//@Category(IntegrationTest.class)
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(classes = AuditServiceTest.AuditServiceTestConfiguration.class)
@ActiveProfiles({"AuditServiceTest"})
public class AuditServiceTest {
    @LocalServerPort
    private int webServicePort;
    
    @Autowired
    private RestTemplateBuilder restTemplateBuilder;
    
    private JWTRestTemplate jwtRestTemplate;
    
    @Autowired
    private MessageCollector messageCollector;
    
    @Autowired
    private AuditServiceConfig.AuditSourceBinding auditSourceBinding;
    
    private SubjectIssuerDNPair DN;
    private String userDN = "userDn";
    private String query = "some query";
    private String authorizations = "AUTH1,AUTH2";
    private AuditType auditType = AuditType.ACTIVE;
    
    private static Boolean isHealthy = Boolean.TRUE;
    
    @Before
    public void setup() {
        isHealthy = true;
        jwtRestTemplate = restTemplateBuilder.build(JWTRestTemplate.class);
        DN = SubjectIssuerDNPair.of(userDN, "issuerDn");
    }
    
    @Test(expected = HttpServerErrorException.class)
    public void testMissingUserDN() {
        Collection<String> roles = Collections.singleton("AuthorizedUser");
        DatawaveUser uathDWUser = new DatawaveUser(DN, USER, null, roles, null, System.currentTimeMillis());
        ProxiedUserDetails authUser = new ProxiedUserDetails(Collections.singleton(uathDWUser), uathDWUser.getCreationTime());
        
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/audit/v1/audit")
                        .queryParam(AuditParameters.QUERY_STRING, query).queryParam(AuditParameters.QUERY_AUTHORIZATIONS, authorizations)
                        .queryParam(AuditParameters.QUERY_AUDIT_TYPE, auditType).queryParam(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, "ALL").build();
        
        try {
            jwtRestTemplate.exchange(authUser, HttpMethod.POST, uri, String.class);
        } finally {
            assertTrue(messageCollector.forChannel(auditSourceBinding.auditSource()).isEmpty());
        }
    }
    
    @Test(expected = HttpServerErrorException.class)
    public void testMissingQuery() {
        Collection<String> roles = Collections.singleton("AuthorizedUser");
        DatawaveUser uathDWUser = new DatawaveUser(DN, USER, null, roles, null, System.currentTimeMillis());
        ProxiedUserDetails authUser = new ProxiedUserDetails(Collections.singleton(uathDWUser), uathDWUser.getCreationTime());
        
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/audit/v1/audit")
                        .queryParam(AuditParameters.USER_DN, userDN).queryParam(AuditParameters.QUERY_AUTHORIZATIONS, authorizations)
                        .queryParam(AuditParameters.QUERY_AUDIT_TYPE, auditType).queryParam(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, "ALL").build();
        
        try {
            jwtRestTemplate.exchange(authUser, HttpMethod.POST, uri, String.class);
        } finally {
            assertTrue(messageCollector.forChannel(auditSourceBinding.auditSource()).isEmpty());
        }
    }
    
    @Test(expected = HttpServerErrorException.class)
    public void testMissingAuths() {
        Collection<String> roles = Collections.singleton("AuthorizedUser");
        DatawaveUser uathDWUser = new DatawaveUser(DN, USER, null, roles, null, System.currentTimeMillis());
        ProxiedUserDetails authUser = new ProxiedUserDetails(Collections.singleton(uathDWUser), uathDWUser.getCreationTime());
        
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/audit/v1/audit")
                        .queryParam(AuditParameters.USER_DN, userDN).queryParam(AuditParameters.QUERY_STRING, query)
                        .queryParam(AuditParameters.QUERY_AUDIT_TYPE, auditType).queryParam(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, "ALL").build();
        
        try {
            jwtRestTemplate.exchange(authUser, HttpMethod.POST, uri, String.class);
        } finally {
            assertTrue(messageCollector.forChannel(auditSourceBinding.auditSource()).isEmpty());
        }
    }
    
    @Test(expected = HttpClientErrorException.class)
    public void testUnauthorizedRole() {
        Collection<String> roles = Collections.singleton("UnauthorizedUser");
        DatawaveUser uathDWUser = new DatawaveUser(DN, USER, null, roles, null, System.currentTimeMillis());
        ProxiedUserDetails authUser = new ProxiedUserDetails(Collections.singleton(uathDWUser), uathDWUser.getCreationTime());
        
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/audit/v1/audit")
                        .queryParam(AuditParameters.USER_DN, userDN).queryParam(AuditParameters.QUERY_STRING, query)
                        .queryParam(AuditParameters.QUERY_AUTHORIZATIONS, authorizations).queryParam(AuditParameters.QUERY_AUDIT_TYPE, auditType)
                        .queryParam(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, "ALL").build();
        
        try {
            jwtRestTemplate.exchange(authUser, HttpMethod.POST, uri, String.class);
        } finally {
            assertTrue(messageCollector.forChannel(auditSourceBinding.auditSource()).isEmpty());
        }
    }
    
    @Test
    public void testAuditMessaging() {
        Collection<String> roles = Collections.singleton("AuthorizedUser");
        DatawaveUser uathDWUser = new DatawaveUser(DN, USER, null, roles, null, System.currentTimeMillis());
        ProxiedUserDetails authUser = new ProxiedUserDetails(Collections.singleton(uathDWUser), uathDWUser.getCreationTime());
        
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/audit/v1/audit")
                        .queryParam(AuditParameters.USER_DN, userDN).queryParam(AuditParameters.QUERY_STRING, query)
                        .queryParam(AuditParameters.QUERY_AUTHORIZATIONS, authorizations).queryParam(AuditParameters.QUERY_AUDIT_TYPE, auditType)
                        .queryParam(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, "ALL").build();
        
        ResponseEntity<String> response = jwtRestTemplate.exchange(authUser, HttpMethod.POST, uri, String.class);
        assertEquals(response.getStatusCode().value(), HttpStatus.OK.value());
        
        @SuppressWarnings("unchecked")
        Message<AuditMessage> msg = (Message<AuditMessage>) messageCollector.forChannel(auditSourceBinding.auditSource()).poll();
        assertNotNull(msg);
        Map<String,String> received = msg.getPayload().getAuditParameters();
        Map<String,String> expected = uri.getQueryParams().toSingleValueMap();
        
        for (String param : expected.keySet()) {
            assertEquals(expected.get(param), received.get(param));
            received.remove(param);
        }
        
        assertNotNull(received.remove(AuditParameters.QUERY_DATE));
        assertNotNull(received.remove(AuditParameters.AUDIT_ID));
        assertEquals(0, received.size());
    }
    
    @Test(expected = HttpServerErrorException.class)
    public void testUnhealthy() {
        isHealthy = false;
        Collection<String> roles = Collections.singleton("AuthorizedUser");
        DatawaveUser uathDWUser = new DatawaveUser(DN, USER, null, roles, null, System.currentTimeMillis());
        ProxiedUserDetails authUser = new ProxiedUserDetails(Collections.singleton(uathDWUser), uathDWUser.getCreationTime());
        
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/audit/v1/audit")
                        .queryParam(AuditParameters.USER_DN, userDN).queryParam(AuditParameters.QUERY_STRING, query)
                        .queryParam(AuditParameters.QUERY_AUTHORIZATIONS, authorizations).queryParam(AuditParameters.QUERY_AUDIT_TYPE, auditType)
                        .queryParam(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, "ALL").build();
        
        jwtRestTemplate.exchange(authUser, HttpMethod.POST, uri, String.class);
    }
    
    @Test
    public void testAuditMessagingWithAuditId() {
        Collection<String> roles = Collections.singleton("AuthorizedUser");
        DatawaveUser uathDWUser = new DatawaveUser(DN, USER, null, roles, null, System.currentTimeMillis());
        ProxiedUserDetails authUser = new ProxiedUserDetails(Collections.singleton(uathDWUser), uathDWUser.getCreationTime());
        String auditId = UUID.randomUUID().toString();
        
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/audit/v1/audit")
                        .queryParam(AuditParameters.USER_DN, userDN).queryParam(AuditParameters.QUERY_STRING, query)
                        .queryParam(AuditParameters.QUERY_AUTHORIZATIONS, authorizations).queryParam(AuditParameters.QUERY_AUDIT_TYPE, auditType)
                        .queryParam(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, "ALL").queryParam(AuditParameters.AUDIT_ID, auditId).build();
        
        ResponseEntity<String> response = jwtRestTemplate.exchange(authUser, HttpMethod.POST, uri, String.class);
        assertEquals(response.getStatusCode().value(), HttpStatus.OK.value());
        
        @SuppressWarnings("unchecked")
        Message<AuditMessage> msg = (Message<AuditMessage>) messageCollector.forChannel(auditSourceBinding.auditSource()).poll();
        assertNotNull(msg);
        Map<String,String> received = msg.getPayload().getAuditParameters();
        Map<String,String> expected = uri.getQueryParams().toSingleValueMap();
        
        for (String param : expected.keySet()) {
            assertEquals(expected.get(param), received.get(param));
            received.remove(param);
        }
        
        assertNotNull(received.remove(AuditParameters.QUERY_DATE));
        assertEquals(0, received.size());
    }
    
    @Configuration
    @Profile("AuditServiceTest")
    @ComponentScan(basePackages = "datawave.microservice")
    public static class AuditServiceTestConfiguration {
        @Bean
        public HealthChecker healthChecker() {
            return new TestHealthChecker();
        }
    }
    
    private static class TestHealthChecker implements HealthChecker {
        
        @Override
        public long pollIntervalMillis() {
            return 0;
        }
        
        @Override
        public void recover() {
            // do nothing
        }
        
        @Override
        public void runHealthCheck() {
            // do nothing
        }
        
        @Override
        public boolean isHealthy() {
            return isHealthy;
        }
        
        @Override
        public List<Map<String,Object>> getOutageStats() {
            return null;
        }
    }
}
