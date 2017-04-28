package nsa.datawave.security.cache;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.*;

import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import nsa.datawave.security.authorization.DatawavePrincipal;
import nsa.datawave.webservice.common.cache.SharedCacheCoordinator;
import nsa.datawave.webservice.common.connection.AccumuloConnectionFactory;
import org.apache.accumulo.core.client.Connector;
import nsa.datawave.accumulo.inmemory.InMemoryInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.VersionedValue;
import org.apache.curator.test.TestingServer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZKUtil;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.jboss.security.SimplePrincipal;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

public class CredentialsCacheBeanTest extends EasyMockSupport {
    
    private static final String TABLE_NAME = "ResponsesCache";
    protected static Logger log = Logger.getLogger(CredentialsCacheBeanTest.class);
    
    private CredentialsCacheBean cache;
    private Cache<String,Principal> principalsCache;
    private SharedCacheCoordinator cacheCoordinator;
    
    private CuratorFramework curatorClient;
    private SharedCount principalsCounter;
    
    private EmbeddedCacheManager cacheManager;
    private TestingServer testZookeeper;
    
    private Connector connector;
    
    @Before
    public void setUp() throws Exception {
        Logger.getLogger(GlobalComponentRegistry.class).setLevel(Level.OFF);
        Logger.getLogger(ComponentRegistry.class).setLevel(Level.OFF);
        Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN);
        
        testZookeeper = new TestingServer();
        InMemoryInstance mockInstance = new InMemoryInstance();
        
        connector = mockInstance.getConnector("root", new PasswordToken(""));
        connector.securityOperations().changeUserAuthorizations("root", new Authorizations("Role1c", "Role2c", "Role3c"));
        connector.tableOperations().create(TABLE_NAME);
        
        cacheManager = new DefaultCacheManager(new GlobalConfigurationBuilder().globalJmxStatistics().allowDuplicateDomains(true).build());
        cacheManager.defineConfiguration(
                        "principals",
                        new ConfigurationBuilder().expiration().lifespan(24, TimeUnit.HOURS).enableReaper().wakeUpInterval(5, TimeUnit.MINUTES).persistence()
                                        .addStore(AccumuloCacheStoreConfigurationBuilder.class).instance(mockInstance).username("root").password("")
                                        .tableName(TABLE_NAME).build());
        
        principalsCache = cacheManager.getCache("principals");
        
        AccumuloConnectionFactory accumuloConFactory = createStrictMock(AccumuloConnectionFactory.class);
        
        cache = new CredentialsCacheBean();
        Whitebox.setInternalState(cache, "principalsCache", principalsCache);
        Whitebox.setInternalState(cache, AccumuloConnectionFactory.class, accumuloConFactory);
        
        cacheCoordinator = new SharedCacheCoordinator("CredentialsCacheBeanTest", testZookeeper.getConnectString(), 30, 300, 10);
        cacheCoordinator.start();
        Whitebox.setInternalState(cache, SharedCacheCoordinator.class, cacheCoordinator);
        
        curatorClient = Whitebox.getInternalState(cacheCoordinator, CuratorFramework.class);
        principalsCounter = new SharedCount(curatorClient, "/counters/flushPrincipals", 1);
        principalsCounter.start();
        
        HashMap<String,String> trackingMap = new HashMap<>();
        expect(accumuloConFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        expect(accumuloConFactory.getConnection(eq(AccumuloConnectionFactory.Priority.ADMIN), eq(trackingMap))).andReturn(connector);
        accumuloConFactory.returnConnection(connector);
        replayAll();
        
        cache.postConstruct();
        
        verifyAll();
        resetAll();
    }
    
    @After
    public void tearDown() throws Exception {
        try {
            ZKUtil.deleteRecursive(curatorClient.getZookeeperClient().getZooKeeper(), "/CredentialsCacheBeanTest/evictions");
            
            principalsCounter.close();
            cacheCoordinator.stop();
            testZookeeper.close();
            cacheManager.stop();
        } catch (Exception ex) {
            log.warn("got " + ex + ", who cares, we are tearing down");
        }
        
    }
    
    @Test
    public void testFlushPrincipals() {
        principalsCache.put("foo", new SimplePrincipal("bar"));
        assertEquals(1, principalsCache.size());
        
        cache.flushPrincipals();
        
        assertTrue(principalsCache.isEmpty());
    }
    
    @Test
    public void testFlushAuthorizationAllCache() throws Exception {
        principalsCache.put("foo", new SimplePrincipal("bar"));
        assertEquals(1, principalsCache.size());
        
        cache.flushAll();
        
        assertTrue(principalsCache.isEmpty());
        assertFalse("Accumulo backing store for principals cache is not empty.", connector.createScanner(TABLE_NAME, new Authorizations()).iterator().hasNext());
    }
    
    @Test
    public void testDistributedFlushAuthorizationAllCache() throws Exception {
        AdvancedCache<String,Principal> advancedCache = principalsCache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD);
        advancedCache.put("foo", new SimplePrincipal("bar"));
        assertEquals(1, advancedCache.size());
        
        VersionedValue<Integer> currentCount = principalsCounter.getVersionedValue();
        principalsCounter.trySetCount(currentCount, currentCount.getValue() + 1);
        for (int i = 0; i < 10; ++i) {
            if (advancedCache.isEmpty())
                break;
            Thread.sleep(100);
        }
        
        assertTrue("In-memory principals cache is not empty after flush.", advancedCache.isEmpty());
        assertTrue("Accumulo backing store for principals cache is empty but shouldn't be.", connector.createScanner(TABLE_NAME, new Authorizations())
                        .iterator().hasNext());
    }
    
    @Test
    public void testEvict() throws Exception {
        principalsCache.put("dn1", new SimplePrincipal("dn1"));
        principalsCache.put("dn2", new SimplePrincipal("dn2"));
        principalsCache.put("dn3", new SimplePrincipal("dn3"));
        principalsCache.put("dn1<idn1><dn4><idn4>", new SimplePrincipal("dn1<idn1><dn4><idn4>"));
        principalsCache.put("dn5<idn5><dn1><idn1>", new SimplePrincipal("dn5<idn5><dn1><idn1>"));
        assertEquals(5, principalsCache.size());
        
        String response = cache.evict("dn1");
        assertEquals("Evicted dn1 from the credentials cache.", response);
        assertEquals(2, principalsCache.size());
        assertNull(principalsCache.get("dn1"));
        assertNull(principalsCache.get("dn1<idn1><dn4><idn4>"));
        assertNull(principalsCache.get("dn5<idn5><dn1><idn1>"));
        
        principalsCache.clear();
    }
    
    @Test
    public void testDistributedEvict() throws Exception {
        principalsCache.put("dn1", new SimplePrincipal("dn1"));
        principalsCache.put("dn1<idn1><dn4><idn4>", new SimplePrincipal("dn1<idn1><dn4><idn4>"));
        principalsCache.put("dn5<idn5><dn1><idn1>", new SimplePrincipal("dn5<idn5><dn1><idn1>"));
        principalsCache.put("dn2", new SimplePrincipal("dn2"));
        principalsCache.put("dn3", new SimplePrincipal("dn3"));
        assertEquals(5, principalsCache.size());
        
        curatorClient.create().creatingParentsIfNeeded().forPath("/evictions/dn1/someOtherNode");
        for (int i = 0; i < 10; ++i) {
            if (principalsCache.size() == 2)
                break;
            Thread.sleep(100);
        }
        
        assertEquals(2, principalsCache.size());
        assertNull(principalsCache.get("dn1"));
        // Make sure the dn1 entries aren't in the in-memory map. Make a copy of the key set since
        // the contains method on the one returned by the cache actually checks the backing store.
        HashSet<String> keys = new HashSet<>(principalsCache.keySet());
        assertFalse(keys.contains("dn1"));
        assertFalse(keys.contains("dn1<idn1><dn4><idn4>"));
        assertFalse(keys.contains("dn5<idn5><dn1><idn1>"));
    }
    
    @Test
    public void testNumEntries() {
        assertEquals(0, principalsCache.size());
        assertEquals(0, cache.numEntries());
        
        principalsCache.put("dn1", new SimplePrincipal("dn1"));
        assertEquals(1, principalsCache.size());
        assertEquals(1, cache.numEntries());
        
        principalsCache.put("dn2", new SimplePrincipal("dn2"));
        assertEquals(2, principalsCache.size());
        assertEquals(2, cache.numEntries());
        
        principalsCache.remove("dn2");
        assertEquals(1, principalsCache.size());
        assertEquals(1, cache.numEntries());
    }
    
    @Test
    public void testListDNs() {
        String dn1 = "CN=Last First Middle sid, OU=acme";
        String dn2 = "CN=Last First Middle sid, OU=costco";
        List<String> expected = Arrays.asList(dn1, dn2);
        Collections.sort(expected);
        
        principalsCache.put(dn1, new SimplePrincipal(dn1));
        principalsCache.put(dn2, new SimplePrincipal(dn2));
        
        List<String> actual = cache.listDNs(false).getDns();
        Collections.sort(actual);
        
        assertEquals(expected, actual);
    }
    
    @Test
    public void testListDNsMatching() {
        String dn1 = "CN=Last1 First1 Middle sid, OU=acme";
        String dn2 = "CN=Last2 First2 Middle sid, OU=costco";
        
        principalsCache.put(dn1, new SimplePrincipal(dn1));
        principalsCache.put(dn2, new SimplePrincipal(dn2));
        
        List<String> actual = cache.listDNsMatching("First").getDns();
        Collections.sort(actual);
        assertEquals(Arrays.asList(dn1, dn2), actual);
        
        actual = cache.listDNsMatching("First2").getDns();
        assertEquals(Collections.singletonList(dn2), actual);
        
        actual = cache.listDNsMatching("acme").getDns();
        assertEquals(Collections.singletonList(dn1), actual);
        
        actual = cache.listDNsMatching("WontFindIt").getDns();
        assertTrue(actual.isEmpty());
    }
    
    @Test
    public void testList() {
        String dn1 = "CN=Last1 First1 Middle sid, OU=acme<CN=ca, OU=acme>";
        String dn2 = "CN=Last2 First2 Middle sid, OU=costco<CN=ca, OU=acme>";
        DatawavePrincipal cp1 = new DatawavePrincipal(dn1);
        DatawavePrincipal cp2 = new DatawavePrincipal(dn2);
        
        assertFalse(cp1 == cp2);
        
        principalsCache.put(dn1, cp1);
        principalsCache.put(dn2, cp2);
        
        assertTrue(cp1 == cache.list(dn1));
        assertTrue(cp2 == cache.list(dn2));
    }
}
