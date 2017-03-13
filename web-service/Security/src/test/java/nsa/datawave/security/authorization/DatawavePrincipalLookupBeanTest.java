package nsa.datawave.security.authorization;

import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.*;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import nsa.datawave.security.cache.CredentialsCacheBean;
import nsa.datawave.security.util.DnUtils;
import nsa.datawave.webservice.common.cache.SharedCacheCoordinator;
import nsa.datawave.webservice.common.connection.AccumuloConnectionFactory;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections.CollectionUtils;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.test.TestingServer;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.infinispan.manager.DefaultCacheManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.reflect.Whitebox;

@RunWith(EasyMockRunner.class)
public class DatawavePrincipalLookupBeanTest extends EasyMockSupport {
    private DefaultCacheManager cacheManager;
    private Cache<String,Principal> principalsCache;
    
    private AuthorizationService authorizationService;
    
    private DatawavePrincipalLookupBean datawavePrincipalLookupBean;
    private CredentialsCacheBean credentialsCacheBean;
    private SharedCacheCoordinator cacheCoordinator;
    private TestingServer testZookeeper;
    
    private Counter cacheHits;
    private Counter cacheMisses;
    
    static private Set<String> accumuloUserAuths = new HashSet<>();
    
    @Before
    public void setUp() throws Exception {
        cacheHits = new Counter();
        cacheMisses = new Counter();
        testZookeeper = new TestingServer();
        accumuloUserAuths.addAll(Arrays.asList("Role1c", "Role2c", "Role3c"));
        
        DatawavePrincipalLookupConfiguration datawavePrincipalLookupConfig = new DatawavePrincipalLookupConfiguration();
        datawavePrincipalLookupConfig.setProjectName("TESTPROJ");
        
        cacheManager = new DefaultCacheManager();
        principalsCache = cacheManager.getCache();
        
        cacheCoordinator = new SharedCacheCoordinator("CredentialsCacheBeanTest", testZookeeper.getConnectString(), 30, 300, 10);
        cacheCoordinator.start();
        
        authorizationService = createStrictMock(AuthorizationService.class);
        credentialsCacheBean = createStrictMock(CredentialsCacheBean.class);
        
        // /authorization/principalFactory
        
        datawavePrincipalLookupBean = new DatawavePrincipalLookupBean();
        
        Whitebox.setInternalState(datawavePrincipalLookupBean, Cache.class, principalsCache);
        Whitebox.setInternalState(datawavePrincipalLookupBean, AuthorizationService.class, authorizationService);
        Whitebox.setInternalState(datawavePrincipalLookupBean, DatawavePrincipalLookupConfiguration.class, datawavePrincipalLookupConfig);
        Whitebox.setInternalState(datawavePrincipalLookupBean, BasePrincipalFactory.class, new IdentityAuthTranslator());
        Whitebox.setInternalState(datawavePrincipalLookupBean, SharedCacheCoordinator.class, cacheCoordinator);
        Whitebox.setInternalState(datawavePrincipalLookupBean, CredentialsCacheBean.class, credentialsCacheBean);
        Whitebox.setInternalState(datawavePrincipalLookupBean, "cacheHits", cacheHits);
        Whitebox.setInternalState(datawavePrincipalLookupBean, "cacheMisses", cacheMisses);
        
        datawavePrincipalLookupBean.postConstruct();
    }
    
    @After
    public void tearDown() throws Exception {
        cacheCoordinator.stop();
        testZookeeper.close();
        cacheManager.stop();
    }
    
    @Test
    public void testLookupSinglePrincipal() throws Exception {
        String dn = "CN=User Joe Test testUser, OU=acme";
        String issuerDN = "CN=ca, OU=acme";
        String combinedDN = dn + "<" + issuerDN + ">";
        
        expect(authorizationService.getRoles("TESTPROJ", dn, issuerDN)).andReturn(new String[] {"Role1", "Role2", "Role3"});
        expect(credentialsCacheBean.getAccumuloUserAuths()).andReturn(accumuloUserAuths);
        replayAll();
        
        DatawavePrincipal principal = datawavePrincipalLookupBean.lookupPrincipal(combinedDN);
        
        verifyAll();
        
        Collection<? extends Collection<String>> allAuths = principal.getAuthorizations();
        assertEquals(1, allAuths.size());
        ArrayList<String> sortedAuths = new ArrayList<>(allAuths.iterator().next());
        Collections.sort(sortedAuths);
        assertEquals("testUser", principal.getShortName());
        assertEquals(combinedDN, principal.getName());
        assertEquals(dn, principal.getUserDN());
        assertArrayEquals(new String[] {dn, issuerDN}, principal.getDNs());
        assertEquals(1, principal.getUserRoles().size());
        assertEquals(new LinkedHashSet<>(Arrays.asList("acme", "Role1", "Role2", "Role3")), new LinkedHashSet<>(principal.getUserRoles().iterator().next()));
        assertEquals(Arrays.asList("Role1c", "Role2c", "Role3c"), sortedAuths);
        assertEquals(1, cacheMisses.getCount());
        assertEquals(0, cacheHits.getCount());
    }
    
    @Test
    public void testAccumuloAuthMerge() throws Exception {
        String dn = "CN=User Joe Test testUser, OU=acme";
        String issuerDN = "CN=ca, OU=acme";
        String combinedDN = dn + "<" + issuerDN + ">";
        
        // Role10 shouldn't make it into final Accumulo auths
        expect(authorizationService.getRoles("TESTPROJ", dn, issuerDN)).andReturn(new String[] {"Role1", "Role10"});
        expect(credentialsCacheBean.getAccumuloUserAuths()).andReturn(accumuloUserAuths);
        replayAll();
        
        DatawavePrincipal principal = datawavePrincipalLookupBean.lookupPrincipal(combinedDN);
        
        verifyAll();
        
        Collection<? extends Collection<String>> allAuths = principal.getAuthorizations();
        assertEquals(1, allAuths.size());
        ArrayList<String> sortedAuths = new ArrayList<>(allAuths.iterator().next());
        Collections.sort(sortedAuths);
        assertEquals("testUser", principal.getShortName());
        assertEquals(combinedDN, principal.getName());
        assertEquals(dn, principal.getUserDN());
        assertArrayEquals(new String[] {dn, issuerDN}, principal.getDNs());
        assertEquals(1, principal.getUserRoles().size());
        assertEquals(new LinkedHashSet<>(Arrays.asList("acme", "Role1", "Role10")), new LinkedHashSet<>(principal.getUserRoles().iterator().next()));
        assertEquals(Arrays.asList("Role1c"), sortedAuths);
        assertEquals(1, cacheMisses.getCount());
        assertEquals(0, cacheHits.getCount());
    }
    
    @Test
    public void testMultipleDNs() throws Exception {
        
        Logger.getRootLogger().addAppender(new ConsoleAppender());
        Logger.getRootLogger().setLevel(Level.ALL);
        
        String issuerDN = "CN=ca, OU=acme, C=US";
        String dn = "CN=User Joe Test testUser, OU=acme, C=US";
        String serverDN1 = "CN=server1.foo.bar, OU=iamnotaperson, OU=acme, C=US";
        String serverDN2 = "CN=server2.foo.bar, OU=iamnotaperson, OU=acme, C=US";
        
        String proxiedDNs = dn + "<" + issuerDN + "><" + serverDN2 + "><" + issuerDN + "><" + serverDN1 + "><" + issuerDN + ">";
        
        // Role10 shouldn't make it into final Accumulo auths
        expect(authorizationService.getRoles("TESTPROJ", dn, issuerDN)).andReturn(new String[] {"Role1", "Role2", "Role3", "Role10"});
        expect(authorizationService.getRoles("TESTPROJ", serverDN2, issuerDN)).andReturn(new String[] {"Role2", "Role10"});
        expect(authorizationService.getRoles("TESTPROJ", serverDN1, issuerDN)).andReturn(new String[] {"Role1", "Role2", "Role10"});
        expect(credentialsCacheBean.getAccumuloUserAuths()).andReturn(accumuloUserAuths).times(3);
        replayAll();
        
        DatawavePrincipal principal = datawavePrincipalLookupBean.lookupPrincipal(proxiedDNs);
        
        verifyAll();
        
        assertEquals(3, principal.getUserRoles().size());
        HashSet<String> allRoles = new HashSet<>();
        for (Collection<String> c : principal.getUserRoles())
            allRoles.addAll(c);
        ArrayList<String> sortedRoles = new ArrayList<>(allRoles);
        Collections.sort(sortedRoles);
        assertEquals(3, principal.getAuthorizations().size());
        HashSet<String> allAuths = new HashSet<>();
        for (Collection<String> c : principal.getAuthorizations())
            allAuths.addAll(c);
        ArrayList<String> sortedAuths = new ArrayList<>(allAuths);
        Collections.sort(sortedAuths);
        
        assertEquals(dn, principal.getUserDN());
        assertEquals("testUser", principal.getShortName());
        assertEquals(proxiedDNs, principal.getName());
        assertEquals(dn, principal.getUserDN());
        assertArrayEquals(new String[] {dn, issuerDN, serverDN2, issuerDN, serverDN1, issuerDN}, principal.getDNs());
        assertEquals(Arrays.asList("Role1", "Role10", "Role2", "Role3", "acme", "iamnotaperson"), sortedRoles);
        assertEquals(Arrays.asList("Role1c", "Role2c", "Role3c"), sortedAuths);
        assertNotNull(principal.getUserRoles());
        assertEquals(new ArrayList<>(Arrays.asList("Role10", "Role2", "acme")), principal.getRoleSets());
        
        assertEquals(4, principalsCache.size());
        DatawavePrincipal actualPrincipal = (DatawavePrincipal) principalsCache.get(dn + "<" + issuerDN + ">");
        assertEquals(dn + "<" + issuerDN + ">", actualPrincipal.getName());
        assertEquals(1, actualPrincipal.getAuthorizations().size());
        assertEquals(new LinkedHashSet<>(Arrays.asList("Role1c", "Role2c", "Role3c")), new LinkedHashSet<>(actualPrincipal.getAuthorizations().iterator()
                        .next()));
        assertEquals(1, actualPrincipal.getUserRoles().size());
        assertEquals(new LinkedHashSet<>(Arrays.asList("Role1", "Role10", "Role2", "Role3", "acme")), new LinkedHashSet<>(actualPrincipal.getUserRoles()
                        .iterator().next()));
        assertNotNull(actualPrincipal.getUserRoles());
        assertEquals(new ArrayList<>(Arrays.asList("Role1", "Role10", "Role2", "Role3", "acme")), actualPrincipal.getRoleSets());
        
        actualPrincipal = (DatawavePrincipal) principalsCache.get(serverDN1 + "<" + issuerDN + ">");
        assertEquals(serverDN1 + "<" + issuerDN + ">", actualPrincipal.getName());
        assertEquals(1, actualPrincipal.getAuthorizations().size());
        assertEquals(new LinkedHashSet<>(Arrays.asList("Role1c", "Role2c")), new LinkedHashSet<>(actualPrincipal.getAuthorizations().iterator().next()));
        assertEquals(1, actualPrincipal.getUserRoles().size());
        assertEquals(new LinkedHashSet<>(Arrays.asList("iamnotaperson", "Role1", "Role10", "Role2", "acme")), new LinkedHashSet<>(actualPrincipal
                        .getUserRoles().iterator().next()));
        assertNotNull(actualPrincipal.getUserRoles());
        assertEquals(new ArrayList<>(Arrays.asList("Role1", "Role10", "Role2", "acme", "iamnotaperson")), actualPrincipal.getRoleSets());
        
        actualPrincipal = (DatawavePrincipal) principalsCache.get(serverDN2 + "<" + issuerDN + ">");
        assertEquals(serverDN2 + "<" + issuerDN + ">", actualPrincipal.getName());
        assertEquals(1, actualPrincipal.getAuthorizations().size());
        assertEquals(new LinkedHashSet<>(Arrays.asList("Role2c")), new LinkedHashSet<>(actualPrincipal.getAuthorizations().iterator().next()));
        assertEquals(1, actualPrincipal.getUserRoles().size());
        assertEquals(new LinkedHashSet<>(Arrays.asList("iamnotaperson", "Role10", "Role2", "acme")), new LinkedHashSet<>(actualPrincipal.getUserRoles()
                        .iterator().next()));
        assertNotNull(actualPrincipal.getUserRoles());
        assertEquals(new ArrayList<>(Arrays.asList("Role10", "Role2", "acme", "iamnotaperson")), actualPrincipal.getRoleSets());
        
        actualPrincipal = (DatawavePrincipal) principalsCache.get(proxiedDNs);
        assertEquals(proxiedDNs, actualPrincipal.getName());
        assertArrayEquals(new String[] {dn, issuerDN, serverDN2, issuerDN, serverDN1, issuerDN}, actualPrincipal.getDNs());
        assertEquals(3, actualPrincipal.getAuthorizationsMap().size());
        assertEquals(new LinkedHashSet<>(Arrays.asList("Role1c", "Role2c", "Role3c")),
                        new LinkedHashSet<>(actualPrincipal.getAuthorizationsMap().get(dn + "<" + issuerDN + ">")));
        assertEquals(new LinkedHashSet<>(Arrays.asList("Role1c", "Role2c")),
                        new LinkedHashSet<>(actualPrincipal.getAuthorizationsMap().get(serverDN1 + "<" + issuerDN + ">")));
        assertEquals(new LinkedHashSet<>(Arrays.asList("Role2c")),
                        new LinkedHashSet<>(actualPrincipal.getAuthorizationsMap().get(serverDN2 + "<" + issuerDN + ">")));
        assertEquals(3, actualPrincipal.getUserRoles().size());
        Iterator<? extends Collection<String>> iter = actualPrincipal.getUserRoles().iterator();
        // The iteration here is not guaranteed to be in the same order since it iterates over an unordered collection
        Set<String> roles1 = Sets.newHashSet("iamnotaperson", "Role1", "Role10", "Role2", "acme");
        Set<String> roles2 = Sets.newHashSet("iamnotaperson", "Role10", "Role2", "acme");
        Set<String> roles3 = Sets.newHashSet("Role1", "Role10", "Role2", "Role3", "acme");
        Map<String,Set<String>> expected = new TreeMap<String,Set<String>>();
        expected.put("roles1", roles1);
        expected.put("roles2", roles2);
        expected.put("roles3", roles3);
        
        while (iter.hasNext()) {
            LinkedHashSet actual = new LinkedHashSet<>(iter.next());
            // now match it against the expected map and remove that item; at the end our map should be empty.
            for (Map.Entry<String,Set<String>> entry : expected.entrySet()) {
                if (CollectionUtils.isEqualCollection(entry.getValue(), actual)) {
                    expected.remove(entry.getKey());
                    break;
                }
            }
        }
        
        // We've already asserted we had 3 items in getUserRoles & we had 3 in our expected map where we removed one
        // per iteration. Our map should now be empty.
        Assert.assertTrue(expected.isEmpty());
        
        assertNotNull(actualPrincipal.getUserRoles());
        Assert.assertTrue(CollectionUtils.isEqualCollection(Lists.newArrayList("Role10", "Role2", "acme"), actualPrincipal.getRoleSets()));
        assertEquals(4, cacheMisses.getCount());
        assertEquals(0, cacheHits.getCount());
    }
    
    @Test
    public void testPrincipalNotCached() throws Exception {
        @SuppressWarnings("unchecked")
        Cache<String,Principal> mockPrincipalsCache = createStrictMock(Cache.class);
        AdvancedCache<String,Principal> mockPrincipalsAdvCache = createStrictMock(AdvancedCache.class);
        SharedCacheCoordinator mockCoordinator = createStrictMock(SharedCacheCoordinator.class);
        InterProcessLock mockLock = createStrictMock(InterProcessLock.class);
        Whitebox.setInternalState(datawavePrincipalLookupBean, Cache.class, mockPrincipalsCache);
        Whitebox.setInternalState(datawavePrincipalLookupBean, SharedCacheCoordinator.class, mockCoordinator);
        
        String dn = "CN=User Joe Test testUser, OU=acme";
        String issuerDN = "CN=ca, OU=acme";
        String combinedDN = dn + "<" + issuerDN + ">";
        
        expect(mockPrincipalsCache.get(combinedDN)).andReturn(null);
        expect(mockCoordinator.getMutex(combinedDN)).andReturn(mockLock);
        expect(mockLock.acquire(anyInt(), anyObject(TimeUnit.class))).andReturn(Boolean.TRUE);
        expect(mockPrincipalsCache.get(combinedDN)).andReturn(null);
        expect(authorizationService.getRoles("TESTPROJ", dn, issuerDN)).andReturn(new String[] {"Role1", "Role2", "Role3"});
        mockLock.release();
        expect(mockPrincipalsCache.getAdvancedCache()).andReturn(mockPrincipalsAdvCache);
        expect(mockPrincipalsAdvCache.withFlags(Flag.IGNORE_RETURN_VALUES)).andReturn(mockPrincipalsAdvCache);
        expect(mockPrincipalsAdvCache.put(eq(combinedDN), anyObject(DatawavePrincipal.class))).andReturn(null);
        expect(credentialsCacheBean.getAccumuloUserAuths()).andReturn(accumuloUserAuths);
        
        replayAll();
        
        DatawavePrincipal principal = datawavePrincipalLookupBean.lookupPrincipal(combinedDN);
        
        verifyAll();
        
        assertEquals(combinedDN, principal.getName());
        assertEquals(1, cacheMisses.getCount());
        assertEquals(0, cacheHits.getCount());
    }
    
    @Test
    public void testPrincipalAlreadyCached() throws Exception {
        @SuppressWarnings("unchecked")
        Cache<String,Principal> mockPrincipalsCache = createStrictMock(Cache.class);
        SharedCacheCoordinator mockCoordinator = createStrictMock(SharedCacheCoordinator.class);
        Whitebox.setInternalState(datawavePrincipalLookupBean, Cache.class, mockPrincipalsCache);
        Whitebox.setInternalState(datawavePrincipalLookupBean, SharedCacheCoordinator.class, mockCoordinator);
        
        String dn = "CN=User Joe Test testUser, OU=acme";
        String issuerDN = "CN=ca, OU=acme";
        String combinedDN = dn + "<" + issuerDN + ">";
        DatawavePrincipal expectedPrincipal = new DatawavePrincipal(combinedDN);
        
        expect(mockPrincipalsCache.get(combinedDN)).andReturn(expectedPrincipal);
        
        replayAll();
        
        DatawavePrincipal principal = datawavePrincipalLookupBean.lookupPrincipal(combinedDN);
        
        verifyAll();
        
        assertEquals(expectedPrincipal, principal);
        assertEquals(0, cacheMisses.getCount());
        assertEquals(1, cacheHits.getCount());
    }
    
    @Test
    public void testPrincipalCachedAfterLock() throws Exception {
        @SuppressWarnings("unchecked")
        Cache<String,Principal> mockPrincipalsCache = createStrictMock(Cache.class);
        SharedCacheCoordinator mockCoordinator = createStrictMock(SharedCacheCoordinator.class);
        InterProcessLock mockLock = createStrictMock(InterProcessLock.class);
        Whitebox.setInternalState(datawavePrincipalLookupBean, Cache.class, mockPrincipalsCache);
        Whitebox.setInternalState(datawavePrincipalLookupBean, SharedCacheCoordinator.class, mockCoordinator);
        
        String dn = "CN=User Joe Test testUser, OU=acme";
        String issuerDN = "CN=ca, OU=acme";
        String combinedDN = dn + "<" + issuerDN + ">";
        DatawavePrincipal expectedPrincipal = new DatawavePrincipal(combinedDN);
        
        expect(mockPrincipalsCache.get(combinedDN)).andReturn(null);
        expect(mockCoordinator.getMutex(combinedDN)).andReturn(mockLock);
        expect(mockLock.acquire(anyInt(), anyObject(TimeUnit.class))).andReturn(Boolean.TRUE);
        expect(mockPrincipalsCache.get(combinedDN)).andReturn(expectedPrincipal);
        mockLock.release();
        
        replayAll();
        
        DatawavePrincipal principal = datawavePrincipalLookupBean.lookupPrincipal(combinedDN);
        
        verifyAll();
        
        assertEquals(expectedPrincipal, principal);
        assertEquals(0, cacheMisses.getCount());
        assertEquals(1, cacheHits.getCount());
    }
    
    @Test
    public void testLockFailure() throws Exception {
        @SuppressWarnings("unchecked")
        Cache<String,Principal> mockPrincipalsCache = createStrictMock(Cache.class);
        SharedCacheCoordinator mockCoordinator = createStrictMock(SharedCacheCoordinator.class);
        InterProcessLock mockLock = createStrictMock(InterProcessLock.class);
        Whitebox.setInternalState(datawavePrincipalLookupBean, Cache.class, mockPrincipalsCache);
        Whitebox.setInternalState(datawavePrincipalLookupBean, SharedCacheCoordinator.class, mockCoordinator);
        
        String dn = "CN=User Joe Test testUser, OU=acme";
        String issuerDN = "CN=ca, OU=acme";
        String combinedDN = dn + "<" + issuerDN + ">";
        DatawavePrincipal expectedPrincipal = new DatawavePrincipal(combinedDN);
        RuntimeException e = new RuntimeException("failed");
        
        expect(mockPrincipalsCache.get(combinedDN)).andReturn(null);
        expect(mockCoordinator.getMutex(combinedDN)).andReturn(mockLock);
        expect(mockLock.acquire(anyInt(), anyObject(TimeUnit.class))).andThrow(e);
        expect(mockPrincipalsCache.get(combinedDN)).andReturn(expectedPrincipal);
        
        replayAll();
        
        DatawavePrincipal principal = datawavePrincipalLookupBean.lookupPrincipal(combinedDN);
        
        verifyAll();
        
        assertEquals(expectedPrincipal, principal);
        assertEquals(0, cacheMisses.getCount());
        assertEquals(1, cacheHits.getCount());
    }
    
    @Test
    public void testLockReleaseFailure() throws Exception {
        @SuppressWarnings("unchecked")
        Cache<String,Principal> mockPrincipalsCache = createStrictMock(Cache.class);
        AdvancedCache<String,Principal> mockPrincipalsAdvCache = createStrictMock(AdvancedCache.class);
        SharedCacheCoordinator mockCoordinator = createStrictMock(SharedCacheCoordinator.class);
        InterProcessLock mockLock = createStrictMock(InterProcessLock.class);
        Whitebox.setInternalState(datawavePrincipalLookupBean, Cache.class, mockPrincipalsCache);
        Whitebox.setInternalState(datawavePrincipalLookupBean, SharedCacheCoordinator.class, mockCoordinator);
        
        String dn = "CN=User Joe Test testUser, OU=acme";
        String issuerDN = "CN=ca, OU=acme";
        String combinedDN = dn + "<" + issuerDN + ">";
        RuntimeException e = new RuntimeException("failed");
        
        expect(mockPrincipalsCache.get(combinedDN)).andReturn(null);
        expect(mockCoordinator.getMutex(combinedDN)).andReturn(mockLock);
        expect(mockLock.acquire(anyInt(), anyObject(TimeUnit.class))).andReturn(Boolean.TRUE);
        expect(mockPrincipalsCache.get(combinedDN)).andReturn(null);
        expect(authorizationService.getRoles("TESTPROJ", dn, issuerDN)).andReturn(new String[] {"Role1", "Role2", "Role3"});
        mockLock.release();
        expectLastCall().andThrow(e);
        expect(mockPrincipalsCache.getAdvancedCache()).andReturn(mockPrincipalsAdvCache);
        expect(mockPrincipalsAdvCache.withFlags(Flag.IGNORE_RETURN_VALUES)).andReturn(mockPrincipalsAdvCache);
        expect(mockPrincipalsAdvCache.put(eq(combinedDN), anyObject(DatawavePrincipal.class))).andReturn(null);
        expect(credentialsCacheBean.getAccumuloUserAuths()).andReturn(accumuloUserAuths);
        
        replayAll();
        
        DatawavePrincipal principal = datawavePrincipalLookupBean.lookupPrincipal(combinedDN);
        
        verifyAll();
        
        assertEquals(combinedDN, principal.getName());
        assertEquals(1, cacheMisses.getCount());
        assertEquals(0, cacheHits.getCount());
    }
    
    private class IdentityAuthTranslator extends BasePrincipalFactory {
        private static final long serialVersionUID = 1L;
        private Logger log = Logger.getLogger(IdentityAuthTranslator.class);
        
        @Override
        public String[] remapRoles(String userName, String[] originalRoles) {
            
            String[] dns = DnUtils.splitProxiedSubjectIssuerDNs(userName);
            String subjectDN = dns[0];
            String issuerDN = null;
            if (dns.length == 2) {
                issuerDN = dns[1];
            }
            
            String[] ous = DnUtils.getOrganizationalUnits(subjectDN);
            LinkedHashSet<String> ousSet = new LinkedHashSet<>();
            for (String ou : ous)
                ousSet.add(ou);
            ous = ousSet.toArray(new String[ousSet.size()]);
            String[] remappableRoles = new String[ous.length + originalRoles.length];
            System.arraycopy(ous, 0, remappableRoles, 0, ous.length);
            System.arraycopy(originalRoles, 0, remappableRoles, ous.length, originalRoles.length);
            return remappableRoles;
        }
        
        @Override
        public String[] toAccumuloAuthorizations(String[] userRoles) {
            String[] accumuloRoles = new String[userRoles.length];
            for (int i = 0; i < userRoles.length; ++i)
                accumuloRoles[i] = userRoles[i] + 'c';
            return accumuloRoles;
        }
        
        @Override
        public void mergePrincipals(DatawavePrincipal target, DatawavePrincipal additional) {
            
            String additionalDNPair = additional.getName();
            
            List<String> tgtRoleSets = target.getRoleSets();
            List<String> addRoleSets = additional.getRoleSets();
            if (addRoleSets != null) {
                if (tgtRoleSets == null) {
                    target.setRoleSets(addRoleSets);
                } else {
                    String[] tgtRoles = tgtRoleSets.toArray(new String[0]);
                    String[] addRoles = addRoleSets.toArray(new String[0]);
                    String[] mergedRoles = mergeUserRoles(tgtRoles, addRoles);
                    target.setRoleSets(Arrays.asList(mergedRoles));
                }
                
            }
            
            Collection<? extends Collection<String>> additionalUserRoles = additional.getUserRoles();
            if (additionalUserRoles != null && !additionalUserRoles.isEmpty()) {
                target.setUserRoles(additionalDNPair, additionalUserRoles.iterator().next());
                Collection<String> rawRoles = additional.getRawRoles(additionalDNPair);
                if (rawRoles != null)
                    target.setRawRoles(additionalDNPair, rawRoles);
            }
            
            Collection<? extends Collection<String>> additionalAccumuloAuths = additional.getAuthorizations();
            if (additionalAccumuloAuths != null && !additionalAccumuloAuths.isEmpty())
                target.setAuthorizations(additionalDNPair, additionalAccumuloAuths.iterator().next());
        }
        
        private <T> LinkedHashSet<T> asSet(T... vals) {
            LinkedHashSet<T> set = new LinkedHashSet<>(Math.max(2 * vals.length, 11));
            for (T val : vals)
                set.add(val);
            return set;
        }
        
        private String[] mergeUserRoles(String[] target, String[] additional) {
            LinkedHashSet<String> s1 = asSet(target);
            LinkedHashSet<String> s2 = asSet(additional);
            
            LinkedHashSet<String> finalSet = new LinkedHashSet<>();
            finalSet.addAll(s1);
            finalSet.retainAll(s2);
            
            return finalSet.toArray(new String[finalSet.size()]);
        }
        
        @Override
        public DatawavePrincipal createPrincipal(String userName, String[] roles) {
            DatawavePrincipal principal = new DatawavePrincipal(userName);
            principal.setRawRoles(userName, Arrays.asList(roles));
            String[] newRoles = remapRoles(userName, roles);
            Arrays.sort(newRoles);
            principal.setUserRoles(userName, Arrays.asList(newRoles));
            principal.setRoleSets(Arrays.asList(newRoles));
            String[] auths = toAccumuloAuthorizations(newRoles);
            Set<String> mergedAuths = new TreeSet<>();
            Collections.addAll(mergedAuths, auths);
            mergedAuths.retainAll(accumuloUserAuths);
            principal.setAuthorizations(userName, mergedAuths);
            return principal;
        }
    }
}
