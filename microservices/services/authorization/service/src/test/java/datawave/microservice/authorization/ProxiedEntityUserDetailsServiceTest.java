package datawave.microservice.authorization;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.cache.CacheType;
import org.springframework.boot.test.autoconfigure.core.AutoConfigureCache;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.autoconfigure.RefreshAutoConfiguration;
import org.springframework.cloud.bus.BusProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import datawave.microservice.authorization.preauth.AuthorizationProxiedEntityPreauthPrincipal;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.authorization.userdetails.ProxiedEntityUserDetailsService;
import datawave.security.authorization.CachedDatawaveUserService;
import datawave.security.authorization.SubjectIssuerDNPair;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"ProxiedEntityUserDetailsServiceTest"})
public class ProxiedEntityUserDetailsServiceTest {
    
    private static final SubjectIssuerDNPair CALLER = SubjectIssuerDNPair.of("cn=test.testcorp.com, ou=microservices, ou=development, o=testcorp, c=us",
                    "cn=testcorp ca, ou=security, o=testcorp, c=us");
    private static final SubjectIssuerDNPair USER_1 = SubjectIssuerDNPair.of("cn=user1.testcorp.com, ou=microservices, ou=development, o=testcorp, c=us",
                    "cn=testcorp ca, ou=security, o=testcorp, c=us");
    private static final SubjectIssuerDNPair USER_2 = SubjectIssuerDNPair.of("cn=user2.testcorp.com, ou=microservices, ou=development, o=testcorp, c=us",
                    "cn=testcorp ca, ou=security, o=testcorp, c=us");
    private static final SubjectIssuerDNPair USER_3 = SubjectIssuerDNPair.of("cn=user3.testcorp.com, ou=microservices, ou=development, o=testcorp, c=us",
                    "cn=testcorp ca, ou=security, o=testcorp, c=us");
    
    @Autowired
    private ProxiedEntityUserDetailsService userDetailsService;
    
    @Test
    public void withProxiedUsers() {
        List<SubjectIssuerDNPair> proxiedEntities = new ArrayList<>();
        proxiedEntities.add(USER_1);
        proxiedEntities.add(USER_2);
        proxiedEntities.add(USER_3);
        AuthorizationProxiedEntityPreauthPrincipal principal = new AuthorizationProxiedEntityPreauthPrincipal(CALLER, proxiedEntities, null);
        PreAuthenticatedAuthenticationToken token = new PreAuthenticatedAuthenticationToken(principal, null);
        DatawaveUserDetails datawaveUserDetails = (DatawaveUserDetails) userDetailsService.loadUserDetails(token);
        // ProxiedEntityUserDetailsService should return a DatawaveUserDetails with all proxied users and the caller
        assertEquals(4, datawaveUserDetails.getProxiedUsers().size());
        assertEquals(USER_1, datawaveUserDetails.getProxiedUsers().stream().findFirst().get().getDn());
        assertEquals(USER_2, datawaveUserDetails.getProxiedUsers().stream().skip(1).findFirst().get().getDn());
        assertEquals(USER_3, datawaveUserDetails.getProxiedUsers().stream().skip(2).findFirst().get().getDn());
        assertEquals(CALLER, datawaveUserDetails.getProxiedUsers().stream().skip(3).findFirst().get().getDn());
    }
    
    @Test
    public void withCallerOnly() {
        List<SubjectIssuerDNPair> proxiedEntities = new ArrayList<>();
        AuthorizationProxiedEntityPreauthPrincipal principal = new AuthorizationProxiedEntityPreauthPrincipal(CALLER, proxiedEntities, null);
        PreAuthenticatedAuthenticationToken token = new PreAuthenticatedAuthenticationToken(principal, null);
        DatawaveUserDetails datawaveUserDetails = (DatawaveUserDetails) userDetailsService.loadUserDetails(token);
        // ProxiedEntityUserDetailsService should return a DatawaveUserDetails with only the caller
        assertEquals(1, datawaveUserDetails.getProxiedUsers().size());
        assertEquals(CALLER, datawaveUserDetails.getProxiedUsers().stream().findFirst().get().getDn());
    }
    
    @Test
    public void withCallerInProxiedUsers() {
        List<SubjectIssuerDNPair> proxiedEntities = new ArrayList<>();
        proxiedEntities.add(USER_1);
        proxiedEntities.add(CALLER);
        AuthorizationProxiedEntityPreauthPrincipal principal = new AuthorizationProxiedEntityPreauthPrincipal(CALLER, proxiedEntities, null);
        PreAuthenticatedAuthenticationToken token = new PreAuthenticatedAuthenticationToken(principal, null);
        DatawaveUserDetails datawaveUserDetails = (DatawaveUserDetails) userDetailsService.loadUserDetails(token);
        // ProxiedEntityUserDetailsService should return a DatawaveUserDetails with all proxied users and the caller
        assertEquals(3, datawaveUserDetails.getProxiedUsers().size());
        assertEquals(USER_1, datawaveUserDetails.getProxiedUsers().stream().findFirst().get().getDn());
        assertEquals(CALLER, datawaveUserDetails.getProxiedUsers().stream().skip(1).findFirst().get().getDn());
        assertEquals(CALLER, datawaveUserDetails.getProxiedUsers().stream().skip(2).findFirst().get().getDn());
    }
    
    @ImportAutoConfiguration({RefreshAutoConfiguration.class})
    @AutoConfigureCache(cacheProvider = CacheType.HAZELCAST)
    @ComponentScan(basePackages = "datawave.microservice")
    @Profile("ProxiedEntityUserDetailsServiceTest")
    @Configuration
    public static class ProxiedEntityUserDetailsServiceTestConfiguration {
        @Bean
        public CachedDatawaveUserService cachedDatawaveUserService() {
            return new AuthorizationTestUserService(Collections.EMPTY_MAP, true);
        }
        
        @Bean
        public HazelcastInstance testHazelcastInstance() {
            Config config = new Config();
            config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
            return Hazelcast.newHazelcastInstance(config);
        }
        
        @Bean
        public BusProperties busProperties() {
            return new BusProperties();
        }
    }
}
