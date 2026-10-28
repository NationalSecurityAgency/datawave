package datawave.security.user;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import javax.ejb.EJBContext;
import javax.enterprise.inject.Instance;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

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
import datawave.security.cache.ElytronCacheManager;
import datawave.user.AuthorizationsListBase;
import datawave.user.DefaultAuthorizationsList;
import datawave.webservice.query.result.event.ResponseObjectFactory;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {ListEffectiveAuthorizationsTest.Config.class})
public class ListEffectiveAuthorizationsTest {
    private static ResponseObjectFactory mockResponseObjectFactory;
    private static EJBContext mockEJBContext;
    private static CredentialsCacheBean mockCredentialsCache;
    private static ElytronCacheManager mockElytronCacheManager;
    private static Instance<CachedDatawaveUserService> mockCachedDatawaveUserService;
    private static AccumuloConnectionFactory mockAccumuloConnectionFactory;
    private static UserOperations mockRemoteUserOperations1;

    @BeforeAll
    public static void setupStatic() {
        mockResponseObjectFactory = mock(ResponseObjectFactory.class);
        mockEJBContext = mock(EJBContext.class);
        mockCredentialsCache = mock(CredentialsCacheBean.class);
        mockElytronCacheManager = mock(ElytronCacheManager.class);
        mockCachedDatawaveUserService = mock(Instance.class);
        mockAccumuloConnectionFactory = mock(AccumuloConnectionFactory.class);
        mockRemoteUserOperations1 = mock(UserOperations.class);
    }

    private void resetMocks() {
        reset(mockResponseObjectFactory, mockEJBContext, mockCredentialsCache, mockElytronCacheManager, mockCachedDatawaveUserService,
                        mockAccumuloConnectionFactory, mockRemoteUserOperations1);
    }

    @AfterEach
    public void cleanup() {
        resetMocks();
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
        public ElytronCacheManager elytronCacheManager() {
            return mockElytronCacheManager;
        }

        @Bean
        public Instance<CachedDatawaveUserService> cachedDatawaveUserService() {
            return mockCachedDatawaveUserService;
        }

        @Bean
        @SpringBean
        public AccumuloConnectionFactory accumuloConnectionFactory() {
            return mockAccumuloConnectionFactory;
        }
    }

    @Autowired
    private UserOperationsBean uob;

    @BeforeEach
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

        doReturn(new DefaultAuthorizationsList()).when(mockResponseObjectFactory).getAuthorizationsList();
        doReturn(remoteUserPrincipal).when(mockRemoteUserOperations1).getRemoteUser(proxiedUserPrincipal);

        AuthorizationsListBase result = uob.listEffectiveAuthorizations(proxiedUserPrincipal);

        verify(mockResponseObjectFactory, times(1)).getAuthorizationsList();
        verify(mockRemoteUserOperations1, times(1)).getRemoteUser(proxiedUserPrincipal);

        Set<String> expectedUsers = new HashSet<>();
        expectedUsers.add(userDN.subjectDN());
        expectedUsers.add(p1dn.subjectDN());

        LinkedHashMap<AuthorizationsListBase.SubjectIssuerDNPair,Set<String>> authMap = result.getAuths();
        for (AuthorizationsListBase.SubjectIssuerDNPair pair : authMap.keySet()) {
            assertTrue(expectedUsers.remove(pair.subjectDN));
        }
        assertTrue(expectedUsers.isEmpty());

        if (mockAccumuloConnectionFactory != null && mockingDetails(mockAccumuloConnectionFactory).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockAccumuloConnectionFactory));
        }
        if (mockCachedDatawaveUserService != null && mockingDetails(mockCachedDatawaveUserService).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockCachedDatawaveUserService));
        }
        if (mockCredentialsCache != null && mockingDetails(mockCredentialsCache).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockCredentialsCache));
        }
        if (mockEJBContext != null && mockingDetails(mockEJBContext).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockEJBContext));
        }
        if (mockElytronCacheManager != null && mockingDetails(mockElytronCacheManager).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockElytronCacheManager));
        }
        if (mockRemoteUserOperations1 != null && mockingDetails(mockRemoteUserOperations1).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockRemoteUserOperations1));
        }
        if (mockResponseObjectFactory != null && mockingDetails(mockResponseObjectFactory).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockResponseObjectFactory));
        }
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

        doReturn(new DefaultAuthorizationsList()).when(mockResponseObjectFactory).getAuthorizationsList();
        doReturn(remoteUserPrincipal).when(mockRemoteUserOperations1).getRemoteUser(proxiedUserPrincipal);

        AuthorizationsListBase result = uob.listEffectiveAuthorizations(proxiedUserPrincipal);

        verify(mockResponseObjectFactory, times(1)).getAuthorizationsList();
        verify(mockRemoteUserOperations1, times(1)).getRemoteUser(proxiedUserPrincipal);

        Set<String> expectedUsers = new HashSet<>();
        expectedUsers.add(userDN.subjectDN());
        expectedUsers.add(p1dn.subjectDN());

        LinkedHashMap<AuthorizationsListBase.SubjectIssuerDNPair,Set<String>> authMap = result.getAuths();
        for (AuthorizationsListBase.SubjectIssuerDNPair pair : authMap.keySet()) {
            assertTrue(expectedUsers.remove(pair.subjectDN));
        }
        assertTrue(expectedUsers.isEmpty());

        if (mockAccumuloConnectionFactory != null && mockingDetails(mockAccumuloConnectionFactory).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockAccumuloConnectionFactory));
        }
        if (mockCachedDatawaveUserService != null && mockingDetails(mockCachedDatawaveUserService).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockCachedDatawaveUserService));
        }
        if (mockCredentialsCache != null && mockingDetails(mockCredentialsCache).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockCredentialsCache));
        }
        if (mockEJBContext != null && mockingDetails(mockEJBContext).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockEJBContext));
        }
        if (mockElytronCacheManager != null && mockingDetails(mockElytronCacheManager).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockElytronCacheManager));
        }
        if (mockRemoteUserOperations1 != null && mockingDetails(mockRemoteUserOperations1).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockRemoteUserOperations1));
        }
        if (mockResponseObjectFactory != null && mockingDetails(mockResponseObjectFactory).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockResponseObjectFactory));
        }
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

        doReturn(new DefaultAuthorizationsList()).when(mockResponseObjectFactory).getAuthorizationsList();
        doReturn(remoteUserPrincipal).when(mockRemoteUserOperations1).getRemoteUser(proxiedUserPrincipal);

        AuthorizationsListBase result = uob.listEffectiveAuthorizations(proxiedUserPrincipal);

        verify(mockResponseObjectFactory, times(1)).getAuthorizationsList();
        verify(mockRemoteUserOperations1, times(1)).getRemoteUser(proxiedUserPrincipal);

        Set<String> expectedUsers = new HashSet<>();
        expectedUsers.add(userDN.subjectDN());
        expectedUsers.add(p1dn.subjectDN());

        LinkedHashMap<AuthorizationsListBase.SubjectIssuerDNPair,Set<String>> authMap = result.getAuths();
        for (AuthorizationsListBase.SubjectIssuerDNPair pair : authMap.keySet()) {
            assertTrue(expectedUsers.remove(pair.subjectDN));
        }
        assertTrue(expectedUsers.isEmpty());

        if (mockAccumuloConnectionFactory != null && mockingDetails(mockAccumuloConnectionFactory).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockAccumuloConnectionFactory));
        }
        if (mockCachedDatawaveUserService != null && mockingDetails(mockCachedDatawaveUserService).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockCachedDatawaveUserService));
        }
        if (mockCredentialsCache != null && mockingDetails(mockCredentialsCache).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockCredentialsCache));
        }
        if (mockEJBContext != null && mockingDetails(mockEJBContext).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockEJBContext));
        }
        if (mockElytronCacheManager != null && mockingDetails(mockElytronCacheManager).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockElytronCacheManager));
        }
        if (mockRemoteUserOperations1 != null && mockingDetails(mockRemoteUserOperations1).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockRemoteUserOperations1));
        }
        if (mockResponseObjectFactory != null && mockingDetails(mockResponseObjectFactory).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockResponseObjectFactory));
        }
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
        DatawaveUser filteredUser = new DatawaveUser(filteredUserDN, DatawaveUser.UserType.USER, Collections.emptyList(), null, null,
                        System.currentTimeMillis());
        DatawavePrincipal proxiedUserPrincipal = new DatawavePrincipal(Lists.newArrayList(user));
        DatawavePrincipal remoteUserPrincipal = new DatawavePrincipal(Lists.newArrayList(filteredUser));

        doReturn(new DefaultAuthorizationsList()).when(mockResponseObjectFactory).getAuthorizationsList();
        doReturn(remoteUserPrincipal).when(mockRemoteUserOperations1).getRemoteUser(proxiedUserPrincipal);

        AuthorizationsListBase result = uob.listEffectiveAuthorizations(proxiedUserPrincipal);

        verify(mockResponseObjectFactory, times(1)).getAuthorizationsList();
        verify(mockRemoteUserOperations1, times(1)).getRemoteUser(proxiedUserPrincipal);

        Set<String> expectedUsers = new HashSet<>();
        expectedUsers.add(userDN.subjectDN());

        LinkedHashMap<AuthorizationsListBase.SubjectIssuerDNPair,Set<String>> authMap = result.getAuths();
        for (AuthorizationsListBase.SubjectIssuerDNPair pair : authMap.keySet()) {
            assertTrue(expectedUsers.remove(pair.subjectDN));
        }
        assertTrue(expectedUsers.isEmpty());

        if (mockAccumuloConnectionFactory != null && mockingDetails(mockAccumuloConnectionFactory).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockAccumuloConnectionFactory));
        }
        if (mockCachedDatawaveUserService != null && mockingDetails(mockCachedDatawaveUserService).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockCachedDatawaveUserService));
        }
        if (mockCredentialsCache != null && mockingDetails(mockCredentialsCache).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockCredentialsCache));
        }
        if (mockEJBContext != null && mockingDetails(mockEJBContext).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockEJBContext));
        }
        if (mockElytronCacheManager != null && mockingDetails(mockElytronCacheManager).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockElytronCacheManager));
        }
        if (mockRemoteUserOperations1 != null && mockingDetails(mockRemoteUserOperations1).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockRemoteUserOperations1));
        }
        if (mockResponseObjectFactory != null && mockingDetails(mockResponseObjectFactory).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockResponseObjectFactory));
        }
    }

    private static class StubbedCredentialsCacheBean extends CredentialsCacheBean {
        @Override
        protected void postConstruct() {
            // no-op to avoid unmockable expectation
        }
    }

}
