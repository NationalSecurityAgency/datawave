package datawave.microservice.authorization;

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
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.web.client.RestTemplate;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import datawave.microservice.cached.CacheInspector;
import datawave.security.authorization.CachedDatawaveUserService;
import datawave.security.authorization.JWTTokenHandler;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"AuthorizeHttpsAllowedCallerTest", "httpsallowedcaller"})
public class AuthorizeHttpsAllowedCallerTest {
    
    @LocalServerPort
    private int webServicePort;
    
    @Autowired
    private RestTemplateBuilder restTemplateBuilder;
    
    @Autowired
    private CacheManager cacheManager;
    
    @Autowired
    private JWTTokenHandler jwtTokenHandler;
    
    private AuthorizationTestUtils testUtils;
    
    @BeforeEach
    public void setup() {
        cacheManager.getCacheNames().forEach(name -> cacheManager.getCache(name).clear());
        RestTemplate restTemplate = restTemplateBuilder.build(RestTemplate.class);
        testUtils = new AuthorizationTestUtils(jwtTokenHandler, restTemplate, "https", webServicePort);
    }
    
    @Test
    public void testAuthorizeMethodSecurityWithAllowedCaller() throws Exception {
        // X509 certificate used for identity
        // passes AllowedCallersFilter because configured certificate is in allowedCallers list
        // passes AuthorizationProxiedEntityX509Filter because configured certificate is in allowedCallers list
        testUtils.testAuthorizeMethodSuccess(null, "/authorization/v1/authorize", false, false);
        testUtils.testAuthorizeMethodSuccess(null, "/authorization/v2/authorize", false, false);
    }
    
    @ImportAutoConfiguration({RefreshAutoConfiguration.class})
    @AutoConfigureCache(cacheProvider = CacheType.HAZELCAST)
    @ComponentScan(basePackages = "datawave.microservice")
    @Profile("AuthorizeHttpsAllowedCallerTest")
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
}
