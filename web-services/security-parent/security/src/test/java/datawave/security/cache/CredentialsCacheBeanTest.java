package datawave.security.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.lang.annotation.Annotation;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.enterprise.inject.Instance;
import javax.enterprise.util.TypeLiteral;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.security.DnList;
import datawave.security.authorization.CachedDatawaveUserService;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUserInfo;
import datawave.security.authorization.SubjectIssuerDNPair;

@ExtendWith(MockitoExtension.class)
public class CredentialsCacheBeanTest {

    private CredentialsCacheBean bean;

    @Mock
    private ElytronCacheManager elytronCacheManager;

    @Mock
    private CachedDatawaveUserService cachedDatawaveUserService;

    @Mock
    private AccumuloConnectionFactory connectionFactory;

    private final DatawaveUser user1 = new DatawaveUser(SubjectIssuerDNPair.of("user1", "issuer1"), DatawaveUser.UserType.USER, null, null, null, -1);
    private final DatawaveUser user2 = new DatawaveUser(SubjectIssuerDNPair.of("user2", "issuer2"), DatawaveUser.UserType.USER, null, null, null, -1);
    private final DatawaveUser server1 = new DatawaveUser(SubjectIssuerDNPair.of("server1", "issuer1"), DatawaveUser.UserType.SERVER, null, null, null, -1);

    private final DatawavePrincipal principal1 = new DatawavePrincipal(List.of(user1, server1));
    private final DatawavePrincipal principal2 = new DatawavePrincipal(List.of(user1));
    private final DatawavePrincipal principal3 = new DatawavePrincipal(List.of(user2, server1));

    @BeforeEach
    public void setUp() {
        bean = new CredentialsCacheBean();
        bean.setElytronCache(elytronCacheManager);
        bean.setCachedDatawaveUserServiceInstance(new TestInstance());
        bean.setAccumuloConnectionFactory(connectionFactory);
    }

    /**
     * When a {@link CachedDatawaveUserService} is not set in the bean, verify that all cache-related methods delegate to the elytron cache only.
     */
    @DisplayName("When a CachedDatawaveUserService is not present")
    @Nested
    class NoCachedDatawaveUserService {

        @BeforeEach
        void setUp() {
            bean.setCachedDatawaveUserServiceInstance(new TestInstance());
        }

        /**
         * When {@link CredentialsCacheBean#flushAll()} is called, verify that the elytron cache evicts all entries.
         */
        @Test
        public void testFlushAll() {
            bean.flushAll();
            verify(elytronCacheManager).clear();
            verifyNoInteractions(cachedDatawaveUserService);
        }

        /**
         * When {@link CredentialsCacheBean#evict(String)} is called, verify that the elytron cache evicts entries matching the given DN.
         */
        @Test
        void testEvict() {
            bean.evict("user1<issuer1>");
            verify(elytronCacheManager).evictUsersWithName("user1<issuer1>");
            verifyNoInteractions(cachedDatawaveUserService);
        }

        /**
         * When {@link CredentialsCacheBean#listDNs(boolean)}, regardless of the value of localOnly, verify that DNs are only fetched from the elytron cache.
         * @param localOnly whether only the local cache should be used.
         */
        @ParameterizedTest()
        @ValueSource(booleans = {true, false})
        void testListDns(boolean localOnly) {
            when(elytronCacheManager.getUsers()).thenReturn(Set.of(user1, user2, server1));
            DnList dnList = bean.listDNs(localOnly);
            assertEquals(List.of("user2<issuer2>", "server1<issuer1>", "user1<issuer1>"), List.copyOf(dnList.getDns()));
            verifyNoInteractions(cachedDatawaveUserService);
        }

        /**
         * When {@link CredentialsCacheBean#listDNsMatching(String)} is called, verify that the elytron cache is used.
         */
        @Test
        void testListDnsMatching() {
            when(elytronCacheManager.getUsersWhereNameContains("issuer1")).thenReturn(Set.of(user1, server1));
            DnList dnList = bean.listDNsMatching("issuer1");
            assertEquals(List.of("server1<issuer1>", "user1<issuer1>"), List.copyOf(dnList.getDns()));
            verifyNoInteractions(cachedDatawaveUserService);
        }

        /**
         * When {@link CredentialsCacheBean#list(String)} is called, verify that the elytron cache is used.
         */
        @Test
        void testList() {
            when(elytronCacheManager.getUserWithName("user1<issuer1>")).thenReturn(user1);
            DatawaveUser user = bean.list("user1<issuer1>");
            assertSame(user1, user);
            verifyNoInteractions(cachedDatawaveUserService);
        }
    }

    /**
     * When a {@link CachedDatawaveUserService} is present, verify that caching methods are for the most part, delegated to the user service only except in some
     * cases.
     */
    @DisplayName("When a CachedDatawaveUserService is present")
    @Nested
    class CachedDatawaveUserServicePresent {

        @BeforeEach
        void setUp() {
            bean.setCachedDatawaveUserServiceInstance(new TestInstance(cachedDatawaveUserService));
        }

        @Test
        public void testFlushAll() {
            bean.flushAll();
            verify(elytronCacheManager).clear();
            verify(cachedDatawaveUserService).evictAll();
        }

        @Test
        void testEvict() {
            bean.evict("user1<issuer1>");
            verify(elytronCacheManager).evictUsersWithName("user1<issuer1>");
            verify(cachedDatawaveUserService).evictMatching("user1<issuer1>");
        }

        @Test
        void testListDnsGivenNotLocalOnly() {
            List<? extends DatawaveUserInfo> userInfos = Stream.of(user1, user2, server1).map(DatawaveUserInfo::new).collect(Collectors.toList());
            Mockito.doReturn(userInfos).when(cachedDatawaveUserService).listAll();
            DnList dnList = bean.listDNs(false);
            assertEquals(List.of("user2<issuer2>", "server1<issuer1>", "user1<issuer1>"), List.copyOf(dnList.getDns()));
            verifyNoInteractions(elytronCacheManager);
        }

        @Test
        void testListDnsGivenLocalOnly() {
            when(elytronCacheManager.getUsers()).thenReturn(Set.of(user1, user2, server1));
            DnList dnList = bean.listDNs(true);
            assertEquals(List.of("user2<issuer2>", "server1<issuer1>", "user1<issuer1>"), List.copyOf(dnList.getDns()));
            verifyNoInteractions(cachedDatawaveUserService);
        }

        @Test
        void testListDnsMatching() {
            List<? extends DatawaveUserInfo> userInfos = Stream.of(user1, server1).map(DatawaveUserInfo::new).collect(Collectors.toList());
            Mockito.doReturn(userInfos).when(cachedDatawaveUserService).listMatching("issuer1");
            DnList dnList = bean.listDNsMatching("issuer1");
            assertEquals(List.of("server1<issuer1>", "user1<issuer1>"), List.copyOf(dnList.getDns()));
            verifyNoInteractions(elytronCacheManager);
        }

        @Test
        void testList() {
            when(cachedDatawaveUserService.list("user1<issuer1>")).thenReturn(user1);
            DatawaveUser user = bean.list("user1<issuer1>");
            assertSame(user1, user);
            verifyNoInteractions(elytronCacheManager);
        }
    }

    private static class TestInstance implements Instance<CachedDatawaveUserService> {

        public CachedDatawaveUserService cachedDatawaveUserService;

        public TestInstance() {}

        public TestInstance(CachedDatawaveUserService cachedDatawaveUserService) {
            this.cachedDatawaveUserService = cachedDatawaveUserService;
        }

        @Override
        public Instance<CachedDatawaveUserService> select(Annotation... qualifiers) {
            return null;
        }

        @Override
        public <U extends CachedDatawaveUserService> Instance<U> select(Class<U> subtype, Annotation... qualifiers) {
            return null;
        }

        @Override
        public <U extends CachedDatawaveUserService> Instance<U> select(TypeLiteral<U> subtype, Annotation... qualifiers) {
            return null;
        }

        @Override
        public boolean isUnsatisfied() {
            return this.cachedDatawaveUserService == null;
        }

        @Override
        public boolean isAmbiguous() {
            return false;
        }

        @Override
        public void destroy(CachedDatawaveUserService instance) {

        }

        @Override
        public Iterator<CachedDatawaveUserService> iterator() {
            return null;
        }

        @Override
        public CachedDatawaveUserService get() {
            return this.cachedDatawaveUserService;
        }
    }
}
