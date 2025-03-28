package datawave.microservice.audit;

import static datawave.security.authorization.DatawaveUser.UserType.USER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.Message;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;

import datawave.microservice.audit.auditors.file.FileAuditor;
import datawave.microservice.audit.auditors.file.config.FileAuditProperties;
import datawave.microservice.audit.common.AuditMessage;
import datawave.microservice.audit.common.AuditMessageSupplier;
import datawave.microservice.audit.config.AuditProperties;
import datawave.microservice.audit.health.HealthChecker;
import datawave.microservice.authorization.jwt.JWTRestTemplate;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;
import datawave.webservice.common.audit.Auditor.AuditType;

@ExtendWith(SpringExtension.class)
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
    public List<AuditMessage> auditMessages;
    
    @Autowired
    private AuditProperties auditProperties;
    
    @Autowired
    private FileAuditProperties fileAuditProperties;
    
    private SubjectIssuerDNPair DN;
    private String userDN = "userDn";
    private String query = "some query";
    private String authorizations = "AUTH1,AUTH2";
    private AuditType auditType = AuditType.ACTIVE;
    
    private static Boolean isHealthy = Boolean.TRUE;
    private static Boolean isFileAuditEnabled = Boolean.TRUE;
    
    @BeforeEach
    public void setup() {
        isHealthy = true;
        isFileAuditEnabled = true;
        jwtRestTemplate = restTemplateBuilder.build(JWTRestTemplate.class);
        DN = SubjectIssuerDNPair.of(userDN, "issuerDn");
        auditMessages.clear();
    }
    
    @Test
    public void testMissingUserDN() {
        Collection<String> roles = Collections.singleton("AuthorizedUser");
        DatawaveUser uathDWUser = new DatawaveUser(DN, USER, null, roles, null, System.currentTimeMillis());
        DatawaveUserDetails authUser = new DatawaveUserDetails(Collections.singleton(uathDWUser), uathDWUser.getCreationTime());
        
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/audit/v1/audit").build();
        
        MultiValueMap<String,String> map = new LinkedMultiValueMap<>();
        map.add(AuditParameters.QUERY_STRING, query);
        map.add(AuditParameters.QUERY_AUTHORIZATIONS, authorizations);
        map.add(AuditParameters.QUERY_AUDIT_TYPE, auditType.name());
        map.add(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, "ALL");
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        
        try {
            assertThrows(HttpServerErrorException.class, () -> jwtRestTemplate.exchange(requestEntity, String.class));
        } finally {
            assertTrue(auditMessages.isEmpty());
        }
    }
    
    @Test
    public void testMissingQuery() {
        Collection<String> roles = Collections.singleton("AuthorizedUser");
        DatawaveUser uathDWUser = new DatawaveUser(DN, USER, null, roles, null, System.currentTimeMillis());
        DatawaveUserDetails authUser = new DatawaveUserDetails(Collections.singleton(uathDWUser), uathDWUser.getCreationTime());
        
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/audit/v1/audit").build();
        
        MultiValueMap<String,String> map = new LinkedMultiValueMap<>();
        map.add(AuditParameters.USER_DN, userDN);
        map.add(AuditParameters.QUERY_AUTHORIZATIONS, authorizations);
        map.add(AuditParameters.QUERY_AUDIT_TYPE, auditType.name());
        map.add(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, "ALL");
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        
        try {
            assertThrows(HttpServerErrorException.class, () -> jwtRestTemplate.exchange(requestEntity, String.class));
        } finally {
            assertTrue(auditMessages.isEmpty());
        }
    }
    
    @Test
    public void testMissingAuths() {
        Collection<String> roles = Collections.singleton("AuthorizedUser");
        DatawaveUser uathDWUser = new DatawaveUser(DN, USER, null, roles, null, System.currentTimeMillis());
        DatawaveUserDetails authUser = new DatawaveUserDetails(Collections.singleton(uathDWUser), uathDWUser.getCreationTime());
        
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/audit/v1/audit").build();
        
        MultiValueMap<String,String> map = new LinkedMultiValueMap<>();
        map.add(AuditParameters.USER_DN, userDN);
        map.add(AuditParameters.QUERY_STRING, query);
        map.add(AuditParameters.QUERY_AUDIT_TYPE, auditType.name());
        map.add(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, "ALL");
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        
        try {
            assertThrows(HttpServerErrorException.class, () -> jwtRestTemplate.exchange(requestEntity, String.class));
        } finally {
            assertTrue(auditMessages.isEmpty());
        }
    }
    
    @Test
    public void testUnauthorizedRole() {
        Collection<String> roles = Collections.singleton("UnauthorizedUser");
        DatawaveUser uathDWUser = new DatawaveUser(DN, USER, null, roles, null, System.currentTimeMillis());
        DatawaveUserDetails authUser = new DatawaveUserDetails(Collections.singleton(uathDWUser), uathDWUser.getCreationTime());
        
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/audit/v1/audit").build();
        
        MultiValueMap<String,String> map = new LinkedMultiValueMap<>();
        map.add(AuditParameters.USER_DN, userDN);
        map.add(AuditParameters.QUERY_STRING, query);
        map.add(AuditParameters.QUERY_AUTHORIZATIONS, authorizations);
        map.add(AuditParameters.QUERY_AUDIT_TYPE, auditType.name());
        map.add(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, "ALL");
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        
        try {
            assertThrows(HttpClientErrorException.class, () -> jwtRestTemplate.exchange(requestEntity, String.class));
        } finally {
            assertTrue(auditMessages.isEmpty());
        }
    }
    
    @Test
    public void testAuditMessaging() {
        Collection<String> roles = Collections.singleton("AuthorizedUser");
        DatawaveUser uathDWUser = new DatawaveUser(DN, USER, null, roles, null, System.currentTimeMillis());
        DatawaveUserDetails authUser = new DatawaveUserDetails(Collections.singleton(uathDWUser), uathDWUser.getCreationTime());
        
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/audit/v1/audit").build();
        
        MultiValueMap<String,String> map = new LinkedMultiValueMap<>();
        map.add(AuditParameters.USER_DN, userDN);
        map.add(AuditParameters.QUERY_STRING, query);
        map.add(AuditParameters.QUERY_AUTHORIZATIONS, authorizations);
        map.add(AuditParameters.QUERY_AUDIT_TYPE, auditType.name());
        map.add(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, "ALL");
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        
        ResponseEntity<String> response = jwtRestTemplate.exchange(requestEntity, String.class);
        assertEquals(response.getStatusCode().value(), HttpStatus.OK.value());
        
        Map<String,String> received = auditMessages.remove(0).getAuditParameters();
        
        for (String param : map.keySet()) {
            assertEquals(Objects.requireNonNull(map.get(param)).get(0), received.get(param));
            received.remove(param);
        }
        
        assertNotNull(received.remove(AuditParameters.QUERY_DATE));
        assertNotNull(received.remove(AuditParameters.AUDIT_ID));
        assertEquals(0, received.size());
    }
    
    @DirtiesContext
    @Test
    public void testUnhealthyFileAuditDisabled() {
        isFileAuditEnabled = false;
        
        AuditProperties.Retry retry = new AuditProperties.Retry();
        retry.setMaxAttempts(1);
        retry.setBackoffIntervalMillis(0);
        retry.setFailTimeoutMillis(0);
        auditProperties.setRetry(retry);
        
        isHealthy = false;
        Collection<String> roles = Collections.singleton("AuthorizedUser");
        DatawaveUser uathDWUser = new DatawaveUser(DN, USER, null, roles, null, System.currentTimeMillis());
        DatawaveUserDetails authUser = new DatawaveUserDetails(Collections.singleton(uathDWUser), uathDWUser.getCreationTime());
        
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/audit/v1/audit").build();
        
        MultiValueMap<String,String> map = new LinkedMultiValueMap<>();
        map.add(AuditParameters.USER_DN, userDN);
        map.add(AuditParameters.QUERY_STRING, query);
        map.add(AuditParameters.QUERY_AUTHORIZATIONS, authorizations);
        map.add(AuditParameters.QUERY_AUDIT_TYPE, auditType.name());
        map.add(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, "ALL");
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        
        assertThrows(HttpServerErrorException.class, () -> jwtRestTemplate.exchange(requestEntity, String.class));
    }
    
    @DirtiesContext
    @Test
    public void testUnhealthyFileAuditEnabled() throws Exception {
        
        AuditProperties.Retry retry = new AuditProperties.Retry();
        retry.setMaxAttempts(1);
        retry.setBackoffIntervalMillis(0);
        retry.setFailTimeoutMillis(0);
        auditProperties.setRetry(retry);
        
        isHealthy = false;
        Collection<String> roles = Collections.singleton("AuthorizedUser");
        DatawaveUser uathDWUser = new DatawaveUser(DN, USER, null, roles, null, System.currentTimeMillis());
        DatawaveUserDetails authUser = new DatawaveUserDetails(Collections.singleton(uathDWUser), uathDWUser.getCreationTime());
        
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/audit/v1/audit").build();
        
        MultiValueMap<String,String> map = new LinkedMultiValueMap<>();
        map.add(AuditParameters.USER_DN, userDN);
        map.add(AuditParameters.QUERY_STRING, query);
        map.add(AuditParameters.QUERY_AUTHORIZATIONS, authorizations);
        map.add(AuditParameters.QUERY_AUDIT_TYPE, auditType.name());
        map.add(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, "ALL");
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        
        ResponseEntity<String> response = jwtRestTemplate.exchange(requestEntity, String.class);
        assertEquals(response.getStatusCode().value(), HttpStatus.OK.value());
        
        assertTrue(auditMessages.isEmpty());
        
        List<File> files = Arrays
                        .stream(Objects.requireNonNull(
                                        new File(new File(new URI(fileAuditProperties.getPathUri())), fileAuditProperties.getSubPath()).listFiles()))
                        .filter(f -> f.getName().endsWith(".json")).collect(Collectors.toList());
        assertEquals(1, files.size());
        
        BufferedReader reader = new BufferedReader(new FileReader(files.get(0)));
        List<String> lines = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null)
            lines.add(line);
        reader.close();
        
        assertEquals(1, lines.size());
        
        HashMap<String,String> auditParamsMap = new ObjectMapper().readValue(lines.get(0), new TypeReference<HashMap<String,String>>() {});
        for (String param : map.keySet()) {
            assertEquals(Objects.requireNonNull(map.get(param)).get(0), auditParamsMap.get(param));
            auditParamsMap.remove(param);
        }
        
        assertNotNull(auditParamsMap.remove(AuditParameters.QUERY_DATE));
        assertNotNull(auditParamsMap.remove(AuditParameters.AUDIT_ID));
        assertEquals(0, auditParamsMap.size());
    }
    
    @DirtiesContext
    @Test
    public void testRetryMaxAttemptsFileAuditDisabled() {
        isFileAuditEnabled = false;
        
        int maxAttempts = 2;
        long backoffIntervalMillis = 50L;
        
        AuditProperties.Retry retry = new AuditProperties.Retry();
        retry.setMaxAttempts(maxAttempts);
        retry.setBackoffIntervalMillis(backoffIntervalMillis);
        retry.setFailTimeoutMillis(Long.MAX_VALUE);
        auditProperties.setRetry(retry);
        
        isHealthy = false;
        Collection<String> roles = Collections.singleton("AuthorizedUser");
        DatawaveUser uathDWUser = new DatawaveUser(DN, USER, null, roles, null, System.currentTimeMillis());
        DatawaveUserDetails authUser = new DatawaveUserDetails(Collections.singleton(uathDWUser), uathDWUser.getCreationTime());
        
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/audit/v1/audit").build();
        
        MultiValueMap<String,String> map = new LinkedMultiValueMap<>();
        map.add(AuditParameters.USER_DN, userDN);
        map.add(AuditParameters.QUERY_STRING, query);
        map.add(AuditParameters.QUERY_AUTHORIZATIONS, authorizations);
        map.add(AuditParameters.QUERY_AUDIT_TYPE, auditType.name());
        map.add(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, "ALL");
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        
        long startTimeMillis = System.currentTimeMillis();
        assertThrows(HttpServerErrorException.class, () -> jwtRestTemplate.exchange(requestEntity, String.class));
        long stopTimeMillis = System.currentTimeMillis();
        
        assertTrue((stopTimeMillis - startTimeMillis) >= ((maxAttempts - 1) * backoffIntervalMillis));
    }
    
    @DirtiesContext
    @Test
    public void testRetryMaxAttemptsFileAuditEnabled() throws Exception {
        int maxAttempts = 2;
        long backoffIntervalMillis = 50L;
        
        AuditProperties.Retry retry = new AuditProperties.Retry();
        retry.setMaxAttempts(maxAttempts);
        retry.setBackoffIntervalMillis(backoffIntervalMillis);
        retry.setFailTimeoutMillis(Long.MAX_VALUE);
        auditProperties.setRetry(retry);
        
        isHealthy = false;
        Collection<String> roles = Collections.singleton("AuthorizedUser");
        DatawaveUser uathDWUser = new DatawaveUser(DN, USER, null, roles, null, System.currentTimeMillis());
        DatawaveUserDetails authUser = new DatawaveUserDetails(Collections.singleton(uathDWUser), uathDWUser.getCreationTime());
        
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/audit/v1/audit").build();
        
        MultiValueMap<String,String> map = new LinkedMultiValueMap<>();
        map.add(AuditParameters.USER_DN, userDN);
        map.add(AuditParameters.QUERY_STRING, query);
        map.add(AuditParameters.QUERY_AUTHORIZATIONS, authorizations);
        map.add(AuditParameters.QUERY_AUDIT_TYPE, auditType.name());
        map.add(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, "ALL");
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        
        long startTimeMillis = System.currentTimeMillis();
        ResponseEntity<String> response = jwtRestTemplate.exchange(requestEntity, String.class);
        long stopTimeMillis = System.currentTimeMillis();
        
        assertEquals(response.getStatusCode().value(), HttpStatus.OK.value());
        assertTrue((stopTimeMillis - startTimeMillis) >= ((maxAttempts - 1) * backoffIntervalMillis));
        
        assertTrue(auditMessages.isEmpty());
        
        List<File> files = Arrays
                        .stream(Objects.requireNonNull(
                                        new File(new File(new URI(fileAuditProperties.getPathUri())), fileAuditProperties.getSubPath()).listFiles()))
                        .filter(f -> f.getName().endsWith(".json")).collect(Collectors.toList());
        assertEquals(1, files.size());
        
        BufferedReader reader = new BufferedReader(new FileReader(files.get(0)));
        List<String> lines = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null)
            lines.add(line);
        reader.close();
        
        assertEquals(1, lines.size());
        
        HashMap<String,String> auditParamsMap = new ObjectMapper().readValue(lines.get(0), new TypeReference<HashMap<String,String>>() {});
        for (String param : map.keySet()) {
            assertEquals(Objects.requireNonNull(map.get(param)).get(0), auditParamsMap.get(param));
            auditParamsMap.remove(param);
        }
        
        assertNotNull(auditParamsMap.remove(AuditParameters.QUERY_DATE));
        assertNotNull(auditParamsMap.remove(AuditParameters.AUDIT_ID));
        assertEquals(0, auditParamsMap.size());
    }
    
    @DirtiesContext
    @Test
    public void testRetryFailTimeoutFileAuditDisabled() {
        isFileAuditEnabled = false;
        
        long failTimeoutMillis = 50L;
        
        AuditProperties.Retry retry = new AuditProperties.Retry();
        retry.setMaxAttempts(Integer.MAX_VALUE);
        retry.setBackoffIntervalMillis(0L);
        retry.setFailTimeoutMillis(failTimeoutMillis);
        auditProperties.setRetry(retry);
        
        isHealthy = false;
        Collection<String> roles = Collections.singleton("AuthorizedUser");
        DatawaveUser uathDWUser = new DatawaveUser(DN, USER, null, roles, null, System.currentTimeMillis());
        DatawaveUserDetails authUser = new DatawaveUserDetails(Collections.singleton(uathDWUser), uathDWUser.getCreationTime());
        
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/audit/v1/audit").build();
        
        MultiValueMap<String,String> map = new LinkedMultiValueMap<>();
        map.add(AuditParameters.USER_DN, userDN);
        map.add(AuditParameters.QUERY_STRING, query);
        map.add(AuditParameters.QUERY_AUTHORIZATIONS, authorizations);
        map.add(AuditParameters.QUERY_AUDIT_TYPE, auditType.name());
        map.add(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, "ALL");
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        
        long startTimeMillis = System.currentTimeMillis();
        assertThrows(HttpServerErrorException.class, () -> jwtRestTemplate.exchange(requestEntity, String.class));
        long stopTimeMillis = System.currentTimeMillis();
        
        assertTrue((stopTimeMillis - startTimeMillis) >= failTimeoutMillis);
    }
    
    @DirtiesContext
    @Test
    public void testRetryFailTimeoutFileAuditEnabled() throws Exception {
        long failTimeoutMillis = 50L;
        
        AuditProperties.Retry retry = new AuditProperties.Retry();
        retry.setMaxAttempts(Integer.MAX_VALUE);
        retry.setBackoffIntervalMillis(0L);
        retry.setFailTimeoutMillis(failTimeoutMillis);
        auditProperties.setRetry(retry);
        
        isHealthy = false;
        Collection<String> roles = Collections.singleton("AuthorizedUser");
        DatawaveUser uathDWUser = new DatawaveUser(DN, USER, null, roles, null, System.currentTimeMillis());
        DatawaveUserDetails authUser = new DatawaveUserDetails(Collections.singleton(uathDWUser), uathDWUser.getCreationTime());
        
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/audit/v1/audit").build();
        
        MultiValueMap<String,String> map = new LinkedMultiValueMap<>();
        map.add(AuditParameters.USER_DN, userDN);
        map.add(AuditParameters.QUERY_STRING, query);
        map.add(AuditParameters.QUERY_AUTHORIZATIONS, authorizations);
        map.add(AuditParameters.QUERY_AUDIT_TYPE, auditType.name());
        map.add(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, "ALL");
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        
        long startTimeMillis = System.currentTimeMillis();
        ResponseEntity<String> response = jwtRestTemplate.exchange(requestEntity, String.class);
        long stopTimeMillis = System.currentTimeMillis();
        
        assertEquals(response.getStatusCode().value(), HttpStatus.OK.value());
        assertTrue((stopTimeMillis - startTimeMillis) >= failTimeoutMillis);
        
        assertTrue(auditMessages.isEmpty());
        
        List<File> files = Arrays
                        .stream(Objects.requireNonNull(
                                        new File(new File(new URI(fileAuditProperties.getPathUri())), fileAuditProperties.getSubPath()).listFiles()))
                        .filter(f -> f.getName().endsWith(".json")).collect(Collectors.toList());
        assertEquals(1, files.size());
        
        BufferedReader reader = new BufferedReader(new FileReader(files.get(0)));
        List<String> lines = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null)
            lines.add(line);
        reader.close();
        
        assertEquals(1, lines.size());
        
        HashMap<String,String> auditParamsMap = new ObjectMapper().readValue(lines.get(0), new TypeReference<HashMap<String,String>>() {});
        for (String param : map.keySet()) {
            assertEquals(Objects.requireNonNull(map.get(param)).get(0), auditParamsMap.get(param));
            auditParamsMap.remove(param);
        }
        
        assertNotNull(auditParamsMap.remove(AuditParameters.QUERY_DATE));
        assertNotNull(auditParamsMap.remove(AuditParameters.AUDIT_ID));
        assertEquals(0, auditParamsMap.size());
    }
    
    @Test
    public void testAuditMessagingWithAuditId() {
        Collection<String> roles = Collections.singleton("AuthorizedUser");
        DatawaveUser uathDWUser = new DatawaveUser(DN, USER, null, roles, null, System.currentTimeMillis());
        DatawaveUserDetails authUser = new DatawaveUserDetails(Collections.singleton(uathDWUser), uathDWUser.getCreationTime());
        String auditId = UUID.randomUUID().toString();
        
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/audit/v1/audit").build();
        
        MultiValueMap<String,String> map = new LinkedMultiValueMap<>();
        map.add(AuditParameters.USER_DN, userDN);
        map.add(AuditParameters.QUERY_STRING, query);
        map.add(AuditParameters.QUERY_AUTHORIZATIONS, authorizations);
        map.add(AuditParameters.QUERY_AUDIT_TYPE, auditType.name());
        map.add(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, "ALL");
        map.add(AuditParameters.AUDIT_ID, auditId);
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        
        ResponseEntity<String> response = jwtRestTemplate.exchange(requestEntity, String.class);
        assertEquals(response.getStatusCode().value(), HttpStatus.OK.value());
        
        Map<String,String> received = auditMessages.remove(0).getAuditParameters();
        
        for (String param : map.keySet()) {
            assertEquals(Objects.requireNonNull(map.get(param)).get(0), received.get(param));
            received.remove(param);
        }
        
        assertNotNull(received.remove(AuditParameters.QUERY_DATE));
        assertEquals(0, received.size());
    }
    
    @Test
    @DirtiesContext
    public void testFileAuditMessaging() throws Exception {
        AuditProperties.Retry retry = new AuditProperties.Retry();
        retry.setMaxAttempts(1);
        retry.setBackoffIntervalMillis(0);
        retry.setFailTimeoutMillis(0);
        auditProperties.setRetry(retry);
        
        auditProperties.setConfirmAckEnabled(true);
        auditProperties.setConfirmAckTimeoutMillis(0);
        
        Collection<String> roles = Collections.singleton("AuthorizedUser");
        DatawaveUser uathDWUser = new DatawaveUser(DN, USER, null, roles, null, System.currentTimeMillis());
        DatawaveUserDetails authUser = new DatawaveUserDetails(Collections.singleton(uathDWUser), uathDWUser.getCreationTime());
        
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/audit/v1/audit").build();
        
        MultiValueMap<String,String> map = new LinkedMultiValueMap<>();
        map.add(AuditParameters.USER_DN, userDN);
        map.add(AuditParameters.QUERY_STRING, query);
        map.add(AuditParameters.QUERY_AUTHORIZATIONS, authorizations);
        map.add(AuditParameters.QUERY_AUDIT_TYPE, auditType.name());
        map.add(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, "ALL");
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        
        ResponseEntity<String> response = jwtRestTemplate.exchange(requestEntity, String.class);
        assertEquals(response.getStatusCode().value(), HttpStatus.OK.value());
        
        assertFalse(auditMessages.isEmpty());
        
        List<File> files = Arrays
                        .stream(Objects.requireNonNull(
                                        new File(new File(new URI(fileAuditProperties.getPathUri())), fileAuditProperties.getSubPath()).listFiles()))
                        .filter(f -> f.getName().endsWith(".json")).collect(Collectors.toList());
        assertEquals(1, files.size());
        
        BufferedReader reader = new BufferedReader(new FileReader(files.get(0)));
        List<String> lines = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null)
            lines.add(line);
        reader.close();
        
        assertEquals(1, lines.size());
        
        HashMap<String,String> auditParamsMap = new ObjectMapper().readValue(lines.get(0), new TypeReference<HashMap<String,String>>() {});
        for (String param : map.keySet()) {
            assertEquals(Objects.requireNonNull(map.get(param)).get(0), auditParamsMap.get(param));
            auditParamsMap.remove(param);
        }
        
        assertNotNull(auditParamsMap.remove(AuditParameters.QUERY_DATE));
        assertNotNull(auditParamsMap.remove(AuditParameters.AUDIT_ID));
        assertEquals(0, auditParamsMap.size());
    }
    
    @Configuration
    @Profile("AuditServiceTest")
    @ComponentScan(basePackages = "datawave.microservice")
    public static class AuditServiceTestConfiguration {
        @Bean
        public HealthChecker healthChecker() {
            return new TestHealthChecker();
        }
        
        @Bean("fileAuditProperties")
        public FileAuditProperties fileAuditProperties() {
            FileAuditProperties fileAuditProperties = new FileAuditProperties();
            @SuppressWarnings("UnstableApiUsage")
            File tempDir = Files.createTempDir();
            tempDir.deleteOnExit();
            fileAuditProperties.setPathUri(tempDir.toURI().toString());
            fileAuditProperties.setSubPath("audit");
            return fileAuditProperties;
        }
        
        @Bean(name = "fileAuditor")
        public Auditor fileAuditor(AuditProperties auditProperties, @Qualifier("fileAuditProperties") FileAuditProperties fileAuditProperties)
                        throws Exception {
            List<String> fsConfigResources = (fileAuditProperties.getFsConfigResources() != null) ? fileAuditProperties.getFsConfigResources()
                            : auditProperties.getFsConfigResources();
            
            String subPath = fileAuditProperties.getSubPath();
            if (subPath == null && fileAuditProperties.getSubPathEnvVar() != null)
                subPath = System.getenv(fileAuditProperties.getSubPathEnvVar());
            
            // @formatter:off
            return new TestFileAuditor.Builder()
                    .setPath(fileAuditProperties.getPathUri())
                    .setSubPath(subPath)
                    .setFsConfigResources(fsConfigResources)
                    .setMaxFileAgeSeconds(fileAuditProperties.getMaxFileAgeSeconds())
                    .setMaxFileLengthMB(fileAuditProperties.getMaxFileLengthMB())
                    .setPrefix(fileAuditProperties.getPrefix())
                    .build();
            // @formatter:on
        }
        
        @Bean
        public List<AuditMessage> auditMessages() {
            return new ArrayList<>();
        }
        
        @Primary
        @Bean
        public AuditMessageSupplier testAuditSource(List<AuditMessage> auditMessages) {
            return new AuditMessageSupplier() {
                @Override
                public boolean send(Message<AuditMessage> auditMessage) {
                    auditMessages.add(auditMessage.getPayload());
                    return true;
                }
            };
        }
    }
    
    public static class TestHealthChecker implements HealthChecker {
        
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
    
    private static class TestFileAuditor extends FileAuditor {
        
        protected TestFileAuditor(Builder builder) throws URISyntaxException, IOException {
            super(builder);
        }
        
        @Override
        public void audit(AuditParameters auditParameters) throws Exception {
            if (isFileAuditEnabled)
                super.audit(auditParameters);
            else
                throw new RuntimeException("Filesystem audits disabled");
        }
        
        public static class Builder extends FileAuditor.Builder<Builder> {
            @Override
            public FileAuditor build() throws IOException, URISyntaxException {
                return new TestFileAuditor(this);
            }
        }
    }
}
