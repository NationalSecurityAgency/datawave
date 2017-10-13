package datawave.microservice.audit;

import datawave.common.test.integration.IntegrationTest;
import datawave.microservice.audit.common.AuditParameters;
import datawave.microservice.audit.common.Auditor;
import datawave.microservice.audit.config.AuditServiceConfig;
import datawave.microservice.authorization.jwt.JWTRestTemplate;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.embedded.LocalServerPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.Message;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static datawave.security.authorization.DatawaveUser.UserType.USER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
@RunWith(SpringRunner.class)
@SpringBootTest(properties = {"spring.main.banner-mode=off"}, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
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
    private Auditor.AuditType auditType = Auditor.AuditType.ACTIVE;
    
    @Before
    public void setup() {
        jwtRestTemplate = restTemplateBuilder.build(JWTRestTemplate.class);
        DN = SubjectIssuerDNPair.of(userDN, "issuerDn");
    }
    
    @Test(expected = HttpServerErrorException.class)
    public void testMissingUserDN() {
        Collection<String> roles = Collections.singleton("AuthorizedUser");
        DatawaveUser uathDWUser = new DatawaveUser(DN, USER, null, roles, null, System.currentTimeMillis());
        ProxiedUserDetails authUser = new ProxiedUserDetails(Collections.singleton(uathDWUser), uathDWUser.getCreationTime());
        
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/datawave/audit")
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
        
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/datawave/audit")
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
        
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/datawave/audit")
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
        
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/datawave/audit")
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
        
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/datawave/audit")
                        .queryParam(AuditParameters.USER_DN, userDN).queryParam(AuditParameters.QUERY_STRING, query)
                        .queryParam(AuditParameters.QUERY_AUTHORIZATIONS, authorizations).queryParam(AuditParameters.QUERY_AUDIT_TYPE, auditType)
                        .queryParam(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, "ALL").build();
        
        ResponseEntity<String> response = jwtRestTemplate.exchange(authUser, HttpMethod.POST, uri, String.class);
        assertEquals(response.getStatusCode().value(), HttpStatus.OK.value());
        
        Map<String,Object> received = ((Message<Map<String,Object>>) messageCollector.forChannel(auditSourceBinding.auditSource()).poll()).getPayload();
        Map<String,String> expected = uri.getQueryParams().toSingleValueMap();
        
        for (String param : expected.keySet()) {
            assertEquals(expected.get(param), received.get(param));
            received.remove(param);
        }
        
        assertNotNull(received.remove(AuditParameters.QUERY_DATE));
        assertEquals(0, received.size());
    }
}
