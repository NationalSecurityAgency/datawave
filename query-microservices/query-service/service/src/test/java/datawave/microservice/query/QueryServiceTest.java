package datawave.microservice.query;

import datawave.marking.ColumnVisibilitySecurityMarking;
import datawave.microservice.authorization.jwt.JWTRestTemplate;
import datawave.microservice.authorization.service.RemoteAuthorizationServiceUserDetailsService;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpMethod;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Collection;
import java.util.Collections;

import static datawave.security.authorization.DatawaveUser.UserType.USER;
import static org.springframework.test.util.AssertionErrors.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"QueryServiceTest", RemoteAuthorizationServiceUserDetailsService.ACTIVATION_PROFILE})
public class QueryServiceTest {
    
    @LocalServerPort
    private int webServicePort;
    
    @Autowired
    private RestTemplateBuilder restTemplateBuilder;
    
    private JWTRestTemplate jwtRestTemplate;
    
    private SubjectIssuerDNPair DN;
    private String userDN = "userDn";
    
    @Before
    public void setup() {
        jwtRestTemplate = restTemplateBuilder.build(JWTRestTemplate.class);
        DN = SubjectIssuerDNPair.of(userDN, "issuerDn");
    }
    
    @Test
    public void testDefineQuery() {
        Collection<String> roles = Collections.singleton("AuthorizedUser");
        Collection<String> auths = Collections.singleton("ALL");
        DatawaveUser uathDWUser = new DatawaveUser(DN, USER, auths, roles, null, System.currentTimeMillis());
        ProxiedUserDetails authUser = new ProxiedUserDetails(Collections.singleton(uathDWUser), uathDWUser.getCreationTime());
        
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/query/v1/EventQuery/define")
                        .build();
        
        MultiValueMap<String,String> map = new LinkedMultiValueMap<>();
        map.set(DefaultQueryParameters.QUERY_STRING, "FIELD:SOME_VALUE");
        map.set(DefaultQueryParameters.QUERY_NAME, "The Greatest Query in the World - Tribute");
        map.set(DefaultQueryParameters.QUERY_PERSISTENCE, "PERSISTENT");
        map.set(DefaultQueryParameters.QUERY_AUTHORIZATIONS, "ALL");
        map.set(DefaultQueryParameters.QUERY_EXPIRATION, "20500101 000000.000");
        map.set(DefaultQueryParameters.QUERY_BEGIN, "20000101 000000.000");
        map.set(DefaultQueryParameters.QUERY_END, "20500101 000000.000");
        map.set(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, "ALL");
        
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        
        try {
            ResponseEntity resp = jwtRestTemplate.exchange(requestEntity, String.class);
            
            System.out.println("done!");
        } finally {
            assertTrue("", true);
        }
    }
    
    @Test
    public void testCreateQuery() {
        Collection<String> roles = Collections.singleton("AuthorizedUser");
        Collection<String> auths = Collections.singleton("ALL");
        DatawaveUser uathDWUser = new DatawaveUser(DN, USER, auths, roles, null, System.currentTimeMillis());
        ProxiedUserDetails authUser = new ProxiedUserDetails(Collections.singleton(uathDWUser), uathDWUser.getCreationTime());
        
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/query/v1/EventQuery/create")
                        .build();
        
        MultiValueMap<String,String> map = new LinkedMultiValueMap<>();
        map.set(DefaultQueryParameters.QUERY_STRING, "FIELD:SOME_VALUE");
        map.set(DefaultQueryParameters.QUERY_NAME, "The Greatest Query in the World - Tribute");
        map.set(DefaultQueryParameters.QUERY_PERSISTENCE, "PERSISTENT");
        map.set(DefaultQueryParameters.QUERY_AUTHORIZATIONS, "ALL");
        map.set(DefaultQueryParameters.QUERY_EXPIRATION, "20500101 000000.000");
        map.set(DefaultQueryParameters.QUERY_BEGIN, "20000101 000000.000");
        map.set(DefaultQueryParameters.QUERY_END, "20500101 000000.000");
        map.set(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, "ALL");
        
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        
        try {
            ResponseEntity resp = jwtRestTemplate.exchange(requestEntity, String.class);
            
            System.out.println("done!");
        } finally {
            assertTrue("", true);
        }
    }
    
    @Configuration
    @Profile("QueryServiceTest")
    @ComponentScan(basePackages = "datawave.microservice")
    public static class QueryServiceTestConfiguration {
        
    }
}
