package datawave.security.user;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import javax.ejb.EJBContext;
import javax.enterprise.inject.Instance;

import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.jboss.security.CacheableManager;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import datawave.configuration.spring.SpringBean;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.CachedDatawaveUserService;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.authorization.UserOperations;
import datawave.security.cache.CredentialsCacheBean;
import datawave.security.system.AuthorizationCache;
import datawave.user.AuthorizationsListBase;
import datawave.user.DefaultAuthorizationsList;
import datawave.webservice.query.result.event.ResponseObjectFactory;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ListEffectiveAuthorizationsTest.Config.class})
public class ListEffectiveAuthorizationsTest extends EasyMockSupport {
    private static ResponseObjectFactory mockResponseObjectFactory;
    private static EJBContext mockEJBContext;
    private static CredentialsCacheBean mockCredentialsCache;
    private static CacheableManager<?,Principal> mockCacheManager;
    private static Instance<CachedDatawaveUserService> mockCachedDatawaveUserService;
    private static AccumuloConnectionFactory mockAccumuloConnectionFactory;
    private static UserOperations mockRemoteUserOperations1;

    @BeforeClass
    public static void setupStatic() {
        mockResponseObjectFactory = EasyMock.createMock(ResponseObjectFactory.class);
        mockEJBContext = EasyMock.createMock(EJBContext.class);
        mockCredentialsCache = EasyMock.createMock(CredentialsCacheBean.class);
        mockCacheManager = EasyMock.createMock(CacheableManager.class);
        mockCachedDatawaveUserService = EasyMock.createMock(Instance.class);
        mockAccumuloConnectionFactory = EasyMock.createMock(AccumuloConnectionFactory.class);
        mockRemoteUserOperations1 = EasyMock.createMock(UserOperations.class);
    }

    @Override
    public void replayAll() {
        super.replayAll();
        EasyMock.replay(mockResponseObjectFactory, mockEJBContext, mockCredentialsCache, mockCacheManager, mockCachedDatawaveUserService,
                        mockAccumuloConnectionFactory, mockRemoteUserOperations1);
    }

    @Override
    public void verifyAll() {
        super.verifyAll();
        EasyMock.verify(mockResponseObjectFactory, mockEJBContext, mockCredentialsCache, mockCacheManager, mockCachedDatawaveUserService,
                        mockAccumuloConnectionFactory, mockRemoteUserOperations1);
    }

    @Override
    public void resetAll() {
        super.resetAll();
        EasyMock.reset(mockResponseObjectFactory, mockEJBContext, mockCredentialsCache, mockCacheManager, mockCachedDatawaveUserService,
                        mockAccumuloConnectionFactory, mockRemoteUserOperations1);
    }

    @After
    public void cleanup() {
        resetAll();
    }

    @Configuration
    static class Config {
        @Bean
        public UserOperationsBean userOperationsBean() {
            return new UserOperationsBean();
        }

        @Bean("RemoteUserOperationsList")
        @SpringBean(name = "RemoteUserOperationsList")
        public List<UserOperations> RemoteUserOperationsList() {
            return List.of(mockRemoteUserOperations1);
        }

        @Bean
        public EJBContext context() {
            return mockEJBContext;
        }

        @Bean
        public ResponseObjectFactory responseObjectFactory() {
            return mockResponseObjectFactory;
        }

        @Bean
        public CredentialsCacheBean credentialsCache() {
            return new StubbedCredentialsCacheBean();
        }

        @Bean
        @AuthorizationCache
        public CacheableManager<?,Principal> authManager() {
            return mockCacheManager;
        }

        @Bean
        public Instance<CachedDatawaveUserService> cachedDatawaveUserService() {
            return mockCachedDatawaveUserService;
        }

        @Bean
        public AccumuloConnectionFactory accumuloConnectionFactory() {
            return mockAccumuloConnectionFactory;
        }
    }

    @Autowired
    private UserOperationsBean uob;

    @Before
    public void validateAutowire() {
        assertNotNull(uob);
    }

    @Test
    public void listEffectiveAuthorizationsRemoteTest() throws AuthorizationException {
        SubjectIssuerDNPair userDN = SubjectIssuerDNPair.of("userDN", "issuerDN");
        SubjectIssuerDNPair p1dn = SubjectIssuerDNPair.of("entity1UserDN", "entity1IssuerDN");
        SubjectIssuerDNPair p2dn = SubjectIssuerDNPair.of("entity2UserDN", "entity2IssuerDN");

        DatawaveUser user = new DatawaveUser(userDN, DatawaveUser.UserType.USER, Sets.newHashSet("A", "C", "D"), null, null, System.currentTimeMillis());
        DatawaveUser p1 = new DatawaveUser(p1dn, DatawaveUser.UserType.SERVER, Sets.newHashSet("A", "B", "E"), null, null, System.currentTimeMillis());
        DatawavePrincipal proxiedUserPrincipal = new DatawavePrincipal(Lists.newArrayList(user, p1));

        DatawaveUser p2 = new DatawaveUser(p2dn, DatawaveUser.UserType.SERVER, Sets.newHashSet("X", "Y", "Z"), null, null, System.currentTimeMillis());
        DatawavePrincipal remoteUserPrincipal = new DatawavePrincipal(Lists.newArrayList(user, p1, p2));

        expect(mockResponseObjectFactory.getAuthorizationsList()).andReturn(new DefaultAuthorizationsList());
        expect(mockRemoteUserOperations1.getRemoteUser(proxiedUserPrincipal)).andReturn(remoteUserPrincipal);

        replayAll();

        AuthorizationsListBase result = uob.listEffectiveAuthorizations(proxiedUserPrincipal);

        verifyAll();

        Set<String> expectedUsers = new HashSet<>();
        expectedUsers.add(userDN.subjectDN());
        expectedUsers.add(p1dn.subjectDN());

        LinkedHashMap<AuthorizationsListBase.SubjectIssuerDNPair,Set<String>> authMap = result.getAuths();
        for (AuthorizationsListBase.SubjectIssuerDNPair pair : authMap.keySet()) {
            assertTrue(expectedUsers.remove(pair.subjectDN));
        }
        assertTrue(expectedUsers.isEmpty());
    }

    @Test
    public void listEffectiveAuthorizationsRemoteMissingLocalDNTest() throws AuthorizationException {
        SubjectIssuerDNPair userDN = SubjectIssuerDNPair.of("userDN", "issuerDN");
        SubjectIssuerDNPair p1dn = SubjectIssuerDNPair.of("entity1UserDN", "entity1IssuerDN");
        SubjectIssuerDNPair p2dn = SubjectIssuerDNPair.of("entity2UserDN", "entity2IssuerDN");

        DatawaveUser user = new DatawaveUser(userDN, DatawaveUser.UserType.USER, Sets.newHashSet("A", "C", "D"), null, null, System.currentTimeMillis());
        DatawaveUser p1 = new DatawaveUser(p1dn, DatawaveUser.UserType.SERVER, Sets.newHashSet("A", "B", "E"), null, null, System.currentTimeMillis());
        DatawavePrincipal proxiedUserPrincipal = new DatawavePrincipal(Lists.newArrayList(user, p1));

        DatawaveUser p2 = new DatawaveUser(p2dn, DatawaveUser.UserType.SERVER, Sets.newHashSet("X", "Y", "Z"), null, null, System.currentTimeMillis());
        DatawavePrincipal remoteUserPrincipal = new DatawavePrincipal(Lists.newArrayList(user, p2));

        expect(mockResponseObjectFactory.getAuthorizationsList()).andReturn(new DefaultAuthorizationsList());
        expect(mockRemoteUserOperations1.getRemoteUser(proxiedUserPrincipal)).andReturn(remoteUserPrincipal);

        replayAll();

        AuthorizationsListBase result = uob.listEffectiveAuthorizations(proxiedUserPrincipal);

        verifyAll();

        Set<String> expectedUsers = new HashSet<>();
        expectedUsers.add(userDN.subjectDN());
        expectedUsers.add(p1dn.subjectDN());

        LinkedHashMap<AuthorizationsListBase.SubjectIssuerDNPair,Set<String>> authMap = result.getAuths();
        for (AuthorizationsListBase.SubjectIssuerDNPair pair : authMap.keySet()) {
            assertTrue(expectedUsers.remove(pair.subjectDN));
        }
        assertTrue(expectedUsers.isEmpty());
    }

    @Test
    public void listEffectiveAuthorizationsTest() throws AuthorizationException {
        SubjectIssuerDNPair userDN = SubjectIssuerDNPair.of("userDN", "issuerDN");
        SubjectIssuerDNPair p1dn = SubjectIssuerDNPair.of("entity1UserDN", "entity1IssuerDN");
        SubjectIssuerDNPair p2dn = SubjectIssuerDNPair.of("entity2UserDN", "entity2IssuerDN");

        DatawaveUser user = new DatawaveUser(userDN, DatawaveUser.UserType.USER, Sets.newHashSet("A", "C", "D"), null, null, System.currentTimeMillis());
        DatawaveUser p1 = new DatawaveUser(p1dn, DatawaveUser.UserType.SERVER, Sets.newHashSet("A", "B", "E"), null, null, System.currentTimeMillis());
        DatawavePrincipal proxiedUserPrincipal = new DatawavePrincipal(Lists.newArrayList(user, p1));
        DatawavePrincipal remoteUserPrincipal = new DatawavePrincipal(Lists.newArrayList(user, p1));

        expect(mockResponseObjectFactory.getAuthorizationsList()).andReturn(new DefaultAuthorizationsList());
        expect(mockRemoteUserOperations1.getRemoteUser(proxiedUserPrincipal)).andReturn(remoteUserPrincipal);

        replayAll();

        AuthorizationsListBase result = uob.listEffectiveAuthorizations(proxiedUserPrincipal);

        verifyAll();

        Set<String> expectedUsers = new HashSet<>();
        expectedUsers.add(userDN.subjectDN());
        expectedUsers.add(p1dn.subjectDN());

        LinkedHashMap<AuthorizationsListBase.SubjectIssuerDNPair,Set<String>> authMap = result.getAuths();
        for (AuthorizationsListBase.SubjectIssuerDNPair pair : authMap.keySet()) {
            assertTrue(expectedUsers.remove(pair.subjectDN));
        }
        assertTrue(expectedUsers.isEmpty());
    }

    /*
     * This unit test mimics the response returned for a filtered caller object for
     * datawave.security.authorization.remote.ConditionalRemoteUserOperations#listEffectiveAuthorizations
     */
    @Test
    public void listEffectiveAuthorizationsConditionalRemoteUserOperationsTest() throws AuthorizationException {
        SubjectIssuerDNPair userDN = SubjectIssuerDNPair.of("userDN", "issuerDN");
        SubjectIssuerDNPair filteredUserDN = SubjectIssuerDNPair.of("filteredUserDN", "filteredIssuerDN");

        DatawaveUser user = new DatawaveUser(userDN, DatawaveUser.UserType.USER, Sets.newHashSet("A", "C", "D"), null, null, System.currentTimeMillis());
        DatawaveUser filteredUser = new DatawaveUser(filteredUserDN, DatawaveUser.UserType.USER, Collections.EMPTY_LIST, null, null,
                        System.currentTimeMillis());
        DatawavePrincipal proxiedUserPrincipal = new DatawavePrincipal(Lists.newArrayList(user));
        DatawavePrincipal remoteUserPrincipal = new DatawavePrincipal(Lists.newArrayList(filteredUser));

        expect(mockResponseObjectFactory.getAuthorizationsList()).andReturn(new DefaultAuthorizationsList());
        expect(mockRemoteUserOperations1.getRemoteUser(proxiedUserPrincipal)).andReturn(remoteUserPrincipal);

        replayAll();

        AuthorizationsListBase result = uob.listEffectiveAuthorizations(proxiedUserPrincipal);

        verifyAll();

        Set<String> expectedUsers = new HashSet<>();
        expectedUsers.add(userDN.subjectDN());

        LinkedHashMap<AuthorizationsListBase.SubjectIssuerDNPair,Set<String>> authMap = result.getAuths();
        for (AuthorizationsListBase.SubjectIssuerDNPair pair : authMap.keySet()) {
            assertTrue(expectedUsers.remove(pair.subjectDN));
        }
        assertTrue(expectedUsers.isEmpty());
    }

    private static class StubbedCredentialsCacheBean extends CredentialsCacheBean {
        @Override
        protected void postConstruct() {
            // no-op to avoid unmockable expectation
        }
    }

}
