package datawave.microservice.authorization;

import static datawave.security.authorization.DatawaveUser.UserType.USER;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.UUID;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.cache.CacheType;
import org.springframework.boot.test.autoconfigure.core.AutoConfigureCache;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.cache.CacheManager;
import org.springframework.cloud.autoconfigure.RefreshAutoConfiguration;
import org.springframework.cloud.bus.BusProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.cached.CacheInspector;
import datawave.security.authorization.CachedDatawaveUserService;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.JWTTokenHandler;
import datawave.security.authorization.SubjectIssuerDNPair;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"AuthorizationServiceV1HttpTest", "http"})
public class AuthorizeHttpTest {
    
    private static final SubjectIssuerDNPair ALLOWED_CALLER = SubjectIssuerDNPair.of("cn=test.testcorp.com, ou=microservices, ou=development, o=testcorp, c=us",
                    "cn=testcorp ca, ou=security, o=testcorp, c=us");
    private static final SubjectIssuerDNPair NOT_ALOWED_CALLER = SubjectIssuerDNPair.of(
                    "cn=notallowedcaller.testcorp.com, ou=microservices, ou=development, o=testcorp, c=us", "cn=testcorp ca, ou=security, o=testcorp, c=us");
    
    @LocalServerPort
    private int webServicePort;
    
    @Autowired
    private RestTemplateBuilder restTemplateBuilder;
    
    @Autowired
    private CacheManager cacheManager;
    
    @Autowired
    private JWTTokenHandler jwtTokenHandler;
    
    private AuthorizationTestUtils testUtils;
    
    private DatawaveUserDetails allowedCaller;
    private DatawaveUserDetails notAllowedCaller;
    private RestTemplate restTemplate;
    
    @BeforeEach
    public void setup() {
        cacheManager.getCacheNames().forEach(name -> cacheManager.getCache(name).clear());
        restTemplate = restTemplateBuilder.build(RestTemplate.class);
        testUtils = new AuthorizationTestUtils(jwtTokenHandler, restTemplate, "http", webServicePort);
        
        DatawaveUser allowedDWUser = new DatawaveUser(ALLOWED_CALLER, USER, null, null, null, null, System.currentTimeMillis());
        allowedCaller = new DatawaveUserDetails(Collections.singleton(allowedDWUser), allowedDWUser.getCreationTime());
        
        DatawaveUser notAllowedDWUser = new DatawaveUser(NOT_ALOWED_CALLER, USER, null, null, null, null, System.currentTimeMillis());
        notAllowedCaller = new DatawaveUserDetails(Collections.singleton(notAllowedDWUser), notAllowedDWUser.getCreationTime());
    }
    
    @Test
    public void testAuthorizeNotAllowedCallerTrustedHeader() throws Exception {
        // Use trusted header to authenticate to ProxiedEntityX509Filter
        testUtils.testAuthorizeMethodFailure(notAllowedCaller, "/authorization/v1/authorize", true, false);
        testUtils.testAuthorizeMethodFailure(notAllowedCaller, "/authorization/v2/authorize", true, false);
    }
    
    @Test
    public void testAuthorizeJWTTrustedHeader() throws Exception {
        // Use JWT to authenticate to JWTAuthenticationFilter
        // Since user is already authenticated, ProxiedEntityX509Filter does not
        // authenticate and trustedHeaders are ignored
        testUtils.testAuthorizeMethodSuccess(allowedCaller, "/authorization/v1/authorize", true, true);
        testUtils.testAuthorizeMethodSuccess(allowedCaller, "/authorization/v2/authorize", true, true);
        
        // Use JWT to authenticate to JWTAuthenticationFilter
        // Since user is already authenticated, ProxiedEntityX509Filter does not
        // authenticate and trustedHeaders are ignored
        // allowedCaller is not enforced when accessing using JWT
        testUtils.testAuthorizeMethodSuccess(notAllowedCaller, "/authorization/v1/authorize", true, true);
        testUtils.testAuthorizeMethodSuccess(notAllowedCaller, "/authorization/v2/authorize", true, true);
    }
    
    @Test
    public void testAuthorizeJWT() throws Exception {
        // Use JWT to authenticate to JWTAuthenticationFilter
        testUtils.testAuthorizeMethodSuccess(allowedCaller, "/authorization/v1/authorize", false, true);
        testUtils.testAuthorizeMethodSuccess(allowedCaller, "/authorization/v2/authorize", false, true);
        
        // Use JWT to authenticate to JWTAuthenticationFilter
        // allowedCaller is not enforced when accessing using JWT
        testUtils.testAuthorizeMethodSuccess(notAllowedCaller, "/authorization/v1/authorize", false, true);
        testUtils.testAuthorizeMethodSuccess(notAllowedCaller, "/authorization/v2/authorize", false, true);
    }
    
    @Test
    public void testAuthorizeAllowedCallerTrustedHeader() throws Exception {
        // Use trusted header to authenticate to ProxiedEntityX509Filter
        testUtils.testAuthorizeMethodSuccess(allowedCaller, "/authorization/v1/authorize", true, false);
        testUtils.testAuthorizeMethodSuccess(allowedCaller, "/authorization/v2/authorize", true, false);
    }
    
    @Test
    public void testAuthorizeNoPrincipalChangedCheck() throws Exception {
        // Checking for setCheckForPrincipalChanges(false) in ProxiedEntityX509Filter()
        // If user is authenticated using JWT, then ProxiedEntityX509Filter should not be used
        // If setCheckForPrincipalChanges(true) and we checked for a changed principal, then the trusted header
        // user would be authenticated and notAllowedCaller would be checked against the allowedCallers list
        // allowedCaller is not enforced when accessing using JWT
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("http").host("localhost").port(webServicePort).path("/authorization/v1/authorize")
                        .build();
        RequestEntity requestEntity = testUtils.createRequestEntity(notAllowedCaller, allowedCaller, HttpMethod.GET, uri);
        ResponseEntity<String> responseEntity = restTemplate.exchange(requestEntity, String.class);
        assertEquals(HttpStatus.OK, responseEntity.getStatusCode(), "Authorized request to " + uri + " did not return a 200.");
    }
    
    @ImportAutoConfiguration({RefreshAutoConfiguration.class})
    @AutoConfigureCache(cacheProvider = CacheType.HAZELCAST)
    @ComponentScan(basePackages = "datawave.microservice")
    @Profile("AuthorizationServiceV1HttpTest")
    @Configuration
    public static class AuthorizationServiceTestConfiguration {
        @Bean
        public CachedDatawaveUserService cachedDatawaveUserService(CacheManager cacheManager,
                        @Qualifier("cacheInspectorFactory") Function<CacheManager,CacheInspector> cacheInspectorFactory) {
            return new AuthorizationTestUserService(Collections.emptyMap(), true);
        }
        
        @Bean
        public HazelcastInstance testHazelcastInstance() {
            Config config = new Config();
            config.setClusterName(UUID.randomUUID().toString());
            config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
            return Hazelcast.newHazelcastInstance(config);
        }
        
        @Bean
        public BusProperties busProperties() {
            return new BusProperties();
        }
    }
}
