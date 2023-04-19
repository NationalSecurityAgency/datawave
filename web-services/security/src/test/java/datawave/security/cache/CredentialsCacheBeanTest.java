package datawave.security.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import datawave.configuration.spring.BeanProvider;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.system.AuthorizationCache;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import org.apache.accumulo.core.client.AccumuloClient;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.security.CacheableManager;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Default;
import javax.inject.Inject;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(Arquillian.class)
public class CredentialsCacheBeanTest {
    
    private CredentialsCacheBean ccb;
    
    @Inject
    private CacheableManager<Object,Principal> authManager;
    
    private Cache<Principal,Principal> cache;
    
    @Deployment
    public static JavaArchive createDeployment() {
        System.setProperty("cdi.bean.context", "springFrameworkBeanRefContext.xml");
        // @formatter:off
        return ShrinkWrap
                .create(JavaArchive.class)
                .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi")
                .addClasses(CredentialsCacheBean.class)
                .addAsManifestResource(EmptyAsset.INSTANCE, "beans.xml");
        // @formatter:on
    }
    
    @Before
    public void setUp() {
        // With Arquillian we would normally inject this bean into the test class. However there seems to be
        // an incompatibility with Arquillian and the @Singleton annotation on the bean where any method
        // invoked on the bean throws a NullPointerException. Instead, we instantiate the bean manually and
        // force CDI field injection. This gets everything loaded as we want for testing.
        // TODO: identify and resolve the underlying issue
        ccb = new CredentialsCacheBean();
        BeanProvider.injectFields(ccb);
        
        cache = CacheBuilder.newBuilder().build();
        authManager.setCache(cache);
        
        DatawaveUser u1 = new DatawaveUser(SubjectIssuerDNPair.of("user1", "issuer1"), UserType.USER, null, null, null, -1);
        DatawaveUser u2 = new DatawaveUser(SubjectIssuerDNPair.of("user2", "issuer2"), UserType.USER, null, null, null, -1);
        DatawaveUser s1 = new DatawaveUser(SubjectIssuerDNPair.of("server1", "issuer1"), UserType.SERVER, null, null, null, -1);
        
        DatawavePrincipal dp1 = new DatawavePrincipal(Arrays.asList(u1, s1));
        DatawavePrincipal dp2 = new DatawavePrincipal(Collections.singleton(u1));
        DatawavePrincipal dp3 = new DatawavePrincipal(Arrays.asList(u2, s1));
        
        cache.put(dp1, dp1);
        cache.put(dp2, dp2);
        cache.put(dp3, dp3);
    }
    
    @After
    public void tearDown() {}
    
    @Test
    public void testFlushAll() {
        assertEquals(3, cache.size());
        
        ccb.flushAll();
        
        assertEquals(0, cache.size());
    }
    
    @Test
    public void testEvict() {
        Principal expected = cache.asMap().keySet().stream().filter(p -> p.getName().startsWith("user2")).findFirst().orElse(null);
        assertNotNull(expected);
        assertEquals(3, cache.size());
        ccb.evict("user2<issuer2>");
        assertNull(cache.getIfPresent(expected));
        assertEquals(2, cache.size());
    }
    
    @Test
    public void testListDNs() {
        ArrayList<String> expectedDns = Lists.newArrayList("user2<issuer2>", "server1<issuer1>", "user1<issuer1>");
        DnList dnList = ccb.listDNs(false);
        assertEquals(3, dnList.getDns().size());
        assertEquals(expectedDns, new ArrayList<>(dnList.getDns()));
    }
    
    @Test
    public void testListMatching() {
        ArrayList<String> expectedDns = Lists.newArrayList("server1<issuer1>", "user1<issuer1>");
        DnList dnList = ccb.listDNsMatching("issuer1");
        assertEquals(2, dnList.getDns().size());
        assertEquals(expectedDns, new ArrayList<>(dnList.getDns()));
    }
    
    @Test
    public void testList() {
        DatawaveUser u = ccb.list("user2<issuer2>");
        assertNotNull(u);
        assertEquals("user2<issuer2>", u.getName());
    }
    
    @Default
    @ApplicationScoped
    @AuthorizationCache
    private static class TestCacheableManager implements CacheableManager<Object,Principal> {
        private Cache<Principal,Principal> cache;
        
        @Override
        public void setCache(Object o) {
            // noinspection unchecked
            cache = (Cache<Principal,Principal>) o;
        }
        
        @Override
        public void flushCache() {
            cache.invalidateAll();
            cache.cleanUp();
        }
        
        @Override
        public void flushCache(Principal principal) {
            cache.invalidate(principal);
        }
        
        @Override
        public boolean containsKey(Principal principal) {
            return cache.getIfPresent(principal) != null;
        }
        
        @Override
        public Set<Principal> getCachedKeys() {
            return new HashSet<>(cache.asMap().values());
        }
    }
    
    private static class MockAccumuloConnectionFactory implements AccumuloConnectionFactory {
        @Override
        public String getConnectionUserName(String poolName) {
            return null;
        }
        
        @Override
        public AccumuloClient getClient(Priority priority, Map<String,String> trackingMap) {
            return null;
        }
        
        @Override
        public AccumuloClient getClient(String poolName, Priority priority, Map<String,String> trackingMap) {
            return null;
        }
        
        @Override
        public void returnClient(AccumuloClient client) {
            
        }
        
        @Override
        public Map<String,String> getTrackingMap(StackTraceElement[] stackTrace) {
            return null;
        }
    }
}
