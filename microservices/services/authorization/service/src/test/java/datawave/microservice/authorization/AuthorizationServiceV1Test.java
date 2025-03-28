package datawave.microservice.authorization;

import static datawave.security.authorization.DatawaveUser.UserType.USER;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Function;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
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
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.collect.Multimap;
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
@ActiveProfiles({"AuthorizationServiceV1Test"})
public class AuthorizationServiceV1Test {
    
    private static final SubjectIssuerDNPair ALLOWED_ADMIN_CALLER = SubjectIssuerDNPair
                    .of("cn=test.testcorp.com, ou=microservices, ou=development, o=testcorp, c=us", "cn=testcorp ca, ou=security, o=testcorp, c=us");
    private static final SubjectIssuerDNPair ALLOWED_NONADMIN_CALLER = SubjectIssuerDNPair
                    .of("cn=test2.testcorp.com, ou=microservices, ou=development, o=testcorp, c=us", "cn=testcorp ca, ou=security, o=testcorp, c=us");
    
    @LocalServerPort
    private int webServicePort;
    
    @Autowired
    private RestTemplateBuilder restTemplateBuilder;
    
    @Autowired
    private CacheManager cacheManager;
    
    @Autowired
    private JWTTokenHandler jwtTokenHandler;
    
    private AuthorizationTestUtils testUtils;
    
    private RestTemplate restTemplate;
    
    private static DatawaveUserDetails allowedAdminCaller;
    private static DatawaveUserDetails allowedNonAdminCaller;
    
    @BeforeAll
    public static void classSetup() {
        Collection<String> roles = Collections.singleton("Administrator");
        DatawaveUser allowedAdminDWUser = new DatawaveUser(ALLOWED_ADMIN_CALLER, USER, null, null, roles, null, System.currentTimeMillis());
        allowedAdminCaller = new DatawaveUserDetails(Collections.singleton(allowedAdminDWUser), allowedAdminDWUser.getCreationTime());
        
        DatawaveUser allowedNonAdminDWUser = new DatawaveUser(ALLOWED_NONADMIN_CALLER, USER, null, null, null, null, System.currentTimeMillis());
        allowedNonAdminCaller = new DatawaveUserDetails(Collections.singleton(allowedNonAdminDWUser), allowedNonAdminDWUser.getCreationTime());
    }
    
    @BeforeEach
    public void setup() {
        cacheManager.getCacheNames().forEach(name -> cacheManager.getCache(name).clear());
        restTemplate = restTemplateBuilder.build(RestTemplate.class);
        testUtils = new AuthorizationTestUtils(jwtTokenHandler, restTemplate, "https", webServicePort);
    }
    
    @Test
    public void testAdminMethodSecurityNonAdminCaller() throws Exception {
        
        // the call is being authenticated using a JWT of the provided user. The roles are encapsulated in the JWT
        testUtils.testAdminMethodFailure(allowedNonAdminCaller, "/authorization/v1/admin/evictAll", null);
        testUtils.testAdminMethodFailure(allowedNonAdminCaller, "/authorization/v1/admin/evictUser", "username=ignored");
        testUtils.testAdminMethodFailure(allowedNonAdminCaller, "/authorization/v1/admin/evictUsersMatching", "substring=ignored");
        testUtils.testAdminMethodFailure(allowedNonAdminCaller, "/authorization/v1/admin/listUsers", null);
        testUtils.testAdminMethodFailure(allowedNonAdminCaller, "/authorization/v1/admin/listUser", "username=ignored");
        testUtils.testAdminMethodFailure(allowedNonAdminCaller, "/authorization/v1/admin/listUsersMatching", "substring=ignore");
    }
    
    @Test
    public void testAdminMethodSecurityAdminCaller() throws Exception {
        
        // the call is being authenticated using a JWT of the provided user. The roles are encapsulated in the JWT
        testUtils.testAdminMethodSuccess(allowedAdminCaller, "/authorization/v1/admin/evictAll", null);
        testUtils.testAdminMethodSuccess(allowedAdminCaller, "/authorization/v1/admin/evictUser", "username=ignored");
        testUtils.testAdminMethodSuccess(allowedAdminCaller, "/authorization/v1/admin/evictUsersMatching", "substring=ignored");
        testUtils.testAdminMethodSuccess(allowedAdminCaller, "/authorization/v1/admin/listUsers", null);
        testUtils.testAdminMethodSuccess(allowedAdminCaller, "/authorization/v1/admin/listUser", "username=ignored");
        testUtils.testAdminMethodSuccess(allowedAdminCaller, "/authorization/v1/admin/listUsersMatching", "substring=ignore");
    }
    
    @Test
    public void testV1SerializationSuccessWhenCallingV1() {
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/authorization/v1/admin/listUser")
                        .query("username=ignored").build();
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new GuavaModule());
        // the call is being authenticated using a JWT of the provided user. The roles are encapsulated in the JWT
        RequestEntity requestEntity = testUtils.createRequestEntity(null, allowedAdminCaller, HttpMethod.GET, uri);
        ResponseEntity<Object> r = restTemplate.exchange(requestEntity, Object.class);
        objectMapper.convertValue(r.getBody(), DatawaveUserTestV1.class);
    }
    
    @Test
    public void testV1SerializationFailureWhenCallingV2() {
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/authorization/v2/admin/listUser")
                        .query("username=ignored").build();
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new GuavaModule());
        // the call is being authenticated using a JWT of the provided user. The roles are encapsulated in the JWT
        RequestEntity requestEntity = testUtils.createRequestEntity(null, allowedAdminCaller, HttpMethod.GET, uri);
        ResponseEntity<Object> r = restTemplate.exchange(requestEntity, Object.class);
        assertThrows(IllegalArgumentException.class, () -> {
            objectMapper.convertValue(r.getBody(), DatawaveUserTestV1.class);
        });
    }
    
    @ImportAutoConfiguration({RefreshAutoConfiguration.class})
    @AutoConfigureCache(cacheProvider = CacheType.HAZELCAST)
    @ComponentScan(basePackages = "datawave.microservice")
    @Profile("AuthorizationServiceV1Test")
    @Configuration
    public static class AuthorizationServiceTestConfiguration {
        @Bean
        public CachedDatawaveUserService cachedDatawaveUserService(CacheManager cacheManager,
                        @Qualifier("cacheInspectorFactory") Function<CacheManager,CacheInspector> cacheInspectorFactory) {
            return new AuthorizationTestUserService(Collections.EMPTY_MAP, true);
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
    
    // Since DatawaveUserV1 extends DatawaveUser (and therefore has the email and login members),
    // we use this class as a stand-in for DatawaveUser as it existed before V2
    private static class DatawaveUserTestV1 {
        private SubjectIssuerDNPair dn;
        private String name;
        private DatawaveUser.UserType userType;
        private Collection<String> auths;
        private Collection<String> roles;
        private Multimap<String,String> roleToAuthMapping;
        private long creationTime;
        private long expirationTime;
        
        public SubjectIssuerDNPair getDn() {
            return dn;
        }
        
        public String getName() {
            return name;
        }
        
        public DatawaveUser.UserType getUserType() {
            return userType;
        }
        
        public Collection<String> getAuths() {
            return auths;
        }
        
        public Collection<String> getRoles() {
            return roles;
        }
        
        public Multimap<String,String> getRoleToAuthMapping() {
            return roleToAuthMapping;
        }
        
        public long getCreationTime() {
            return creationTime;
        }
        
        public long getExpirationTime() {
            return expirationTime;
        }
    }
}
