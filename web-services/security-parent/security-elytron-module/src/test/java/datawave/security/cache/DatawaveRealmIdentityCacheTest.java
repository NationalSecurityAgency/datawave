package datawave.security.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.Principal;
import java.security.spec.AlgorithmParameterSpec;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.wildfly.security.auth.SupportLevel;
import org.wildfly.security.auth.principal.NamePrincipal;
import org.wildfly.security.auth.server.RealmIdentity;
import org.wildfly.security.credential.Credential;
import org.wildfly.security.evidence.Evidence;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;

class DatawaveRealmIdentityCacheTest {

    private static final String TEST_ISSUER = "cn=testissuer";
    private static final Set<String> auths = Set.of("A", "B", "C");
    private static final Set<String> roles = Set.of("Administrator", "InternalUser");
    private static final Multimap<String,String> rolesToAuths = ImmutableMultimap.of("Administrator", "A", "Administrator", "B", "Administrator", "C",
                    "InternalUser", "A");

    /**
     * Verify {@link DatawaveRealmIdentityCache#put(Principal, RealmIdentity)} throws an exception given a non {@link DatawavePrincipal} key.
     */
    @Test
    void testPutGivenNonDatawavePrincipalKey() {
        DatawaveRealmIdentityCache cache = new DatawaveRealmIdentityCache(-1, -1);

        // @formatter:off
        assertThatThrownBy(() -> cache.put(new NamePrincipal("user"), new SimpleNameRealmIdentity("user")))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("principal must be of type datawave.security.authorization.DatawavePrincipal");
        // @formatter:on
    }

    /**
     * Verify that {@link DatawaveRealmIdentityCache#get(Principal)} given a domain principal with a match returns the match.
     */
    @Test
    void testGetByDomainPrincipal() {
        DatawaveRealmIdentityCache cache = new DatawaveRealmIdentityCache(-1, -1);

        DatawavePrincipal principal = new DatawavePrincipal("datawaveUser");
        RealmIdentity identity = new SimpleNameRealmIdentity("realmUser");
        cache.put(principal, identity);

        assertThat(cache.get(principal)).isEqualTo(identity);
    }

    /**
     * Verify that {@link DatawaveRealmIdentityCache#get(Principal)} given a realm principal with a match returns the match.
     */
    @Test
    void testGetByRealmPrincipal() {
        DatawaveRealmIdentityCache cache = new DatawaveRealmIdentityCache(-1, -1);

        DatawavePrincipal principal = new DatawavePrincipal("datawaveUser");
        RealmIdentity identity = new SimpleNameRealmIdentity("realmUser");
        cache.put(principal, identity);

        assertThat(cache.get(identity.getRealmIdentityPrincipal())).isEqualTo(identity);
    }

    /**
     * Verify that {@link DatawaveRealmIdentityCache#remove(Principal)} given a domain principal with a single match removes the match.
     */
    @Test
    void testRemoveByDomainPrincipal() {
        DatawaveRealmIdentityCache cache = new DatawaveRealmIdentityCache(-1, -1);

        DatawavePrincipal principal = new DatawavePrincipal("datawaveUser");
        RealmIdentity identity = new SimpleNameRealmIdentity("realmUser");

        cache.put(principal, identity);
        assertThat(cache.get(principal)).isEqualTo(identity);

        cache.remove(principal);

        assertThat(cache.get(principal)).isNull();
    }

    /**
     * Verify that {@link DatawaveRealmIdentityCache#remove(Principal)} given a domain principal with cross-matches removes all matches.
     */
    @Test
    void testRemoveByDomainPrincipalWithCrossMatches() {
        DatawaveRealmIdentityCache cache = new DatawaveRealmIdentityCache(-1, -1);

        DatawavePrincipal principal1 = new DatawavePrincipal("datawaveUser");
        DatawavePrincipal principal2 = new DatawavePrincipal("datawaveUser2");
        DatawavePrincipal principal3 = new DatawavePrincipal("datawaveUser3");

        RealmIdentity identity1 = new SimpleNameRealmIdentity("realmUser");
        RealmIdentity identity3 = new SimpleNameRealmIdentity("realmUser3");

        cache.put(principal1, identity1);
        cache.put(principal2, identity1); // Add a cross-match between principal 1 and principal 2.
        cache.put(principal3, identity3);

        cache.remove(principal1);

        assertThat(cache.get(principal1)).isNull();
        assertThat(cache.get(principal2)).isNull(); // Principal 2 should also have been removed.
        assertThat(cache.get(principal3)).isNotNull();
    }

    /**
     * Verify that {@link DatawaveRealmIdentityCache#remove(Principal)} given a realm principal with a single match removes the match.
     */
    @Test
    void testRemoveByRealmPrincipal() {
        DatawaveRealmIdentityCache cache = new DatawaveRealmIdentityCache(-1, -1);

        DatawavePrincipal principal = new DatawavePrincipal("datawaveUser");
        RealmIdentity identity = new SimpleNameRealmIdentity("realmUser");
        cache.put(principal, identity);

        Principal realmPrincipal = identity.getRealmIdentityPrincipal();
        assertThat(cache.get(realmPrincipal)).isEqualTo(identity);

        cache.remove(realmPrincipal);

        assertThat(cache.get(realmPrincipal)).isNull();
    }

    /**
     * Verify that {@link DatawaveRealmIdentityCache#remove(Principal)} given a realm principal with cross-matches removes all matches.
     */
    @Test
    void testRemoveByRealmPrincipalWithCrossMatches() {
        DatawaveRealmIdentityCache cache = new DatawaveRealmIdentityCache(-1, -1);

        DatawavePrincipal principal1 = new DatawavePrincipal("datawaveUser");
        DatawavePrincipal principal2 = new DatawavePrincipal("datawaveUser2");
        DatawavePrincipal principal3 = new DatawavePrincipal("datawaveUser3");

        RealmIdentity identity1 = new SimpleNameRealmIdentity("realmUser");
        RealmIdentity identity3 = new SimpleNameRealmIdentity("realmUser3");

        cache.put(principal1, identity1);
        cache.put(principal2, identity1); // Add a cross-match between principal 1 and principal 2.
        cache.put(principal3, identity3);

        cache.remove(identity1.getRealmIdentityPrincipal());

        assertThat(cache.get(principal1)).isNull();
        assertThat(cache.get(principal2)).isNull(); // Principal 2 should also have been removed.
        assertThat(cache.get(principal3)).isNotNull();
    }

    /**
     * Verify that {@link DatawaveRealmIdentityCache#clear()} clears the cache.
     */
    @Test
    void testClearCache() {
        DatawaveRealmIdentityCache cache = new DatawaveRealmIdentityCache(-1, -1);

        DatawavePrincipal principal1 = new DatawavePrincipal("datawaveUser");
        DatawavePrincipal principal2 = new DatawavePrincipal("datawaveUser2");
        DatawavePrincipal principal3 = new DatawavePrincipal("datawaveUser3");

        RealmIdentity identity1 = new SimpleNameRealmIdentity("realmUser");
        RealmIdentity identity2 = new SimpleNameRealmIdentity("realmUser2");
        RealmIdentity identity3 = new SimpleNameRealmIdentity("realmUser3");

        cache.put(principal1, identity1);
        cache.put(principal2, identity2);
        cache.put(principal3, identity3);

        cache.clear();

        assertThat(cache.get(principal1)).isNull();
        assertThat(cache.get(principal2)).isNull();
        assertThat(cache.get(principal3)).isNull();
    }

    /**
     * Verify that {@link DatawaveRealmIdentityCache#getUsers()} returns an empty set when the cache is empty.
     */
    @Test
    void testGetUsersGivenEmptyCache() {
        DatawaveRealmIdentityCache cache = new DatawaveRealmIdentityCache(-1, -1);
        assertTrue(cache.getUsers().isEmpty());
    }

    /**
     * Verify that {@link DatawaveRealmIdentityCache#getUsers()} returns all users from the domain principals in the cache.
     */
    @Test
    void testGetUsers() {
        DatawaveRealmIdentityCache cache = new DatawaveRealmIdentityCache(-1, -1);

        DatawavePrincipal principal1 = createPrincipal(createUser("cn=user1"), createUser("cn=proxyuser1"));
        DatawavePrincipal principal2 = createPrincipal(createUser("cn=user2"), createUser("cn=proxyuser2"));
        DatawavePrincipal principal3 = createPrincipal(createUser("cn=user3"), createUser("cn=proxyuser3"));

        cache.put(principal1, new SimpleNameRealmIdentity("realmUser1"));
        cache.put(principal2, new SimpleNameRealmIdentity("realmUser2"));
        cache.put(principal3, new SimpleNameRealmIdentity("realmUser3"));

        Set<DatawaveUser> users = cache.getUsers();
        assertEquals(6, users.size());
        assertTrue(users.containsAll(principal1.getProxiedUsers()));
        assertTrue(users.containsAll(principal2.getProxiedUsers()));
        assertTrue(users.containsAll(principal3.getProxiedUsers()));
    }

    /**
     * Verify that {@link DatawaveRealmIdentityCache#getUsersWhereNameContains(String)} returns an empty set when the cache is empty.
     */
    @Test
    void testGetUsersWhereNameContainsGivenEmptyCache() {
        DatawaveRealmIdentityCache cache = new DatawaveRealmIdentityCache(-1, -1);
        assertTrue(cache.getUsersWhereNameContains("a").isEmpty());
    }

    /**
     * Verify that {@link DatawaveRealmIdentityCache#getUsersWhereNameContains(String)} returns an empty set when there are no matches.
     */
    @Test
    void testGetUsersWhereNameContainsGivenNoMatches() {
        DatawaveRealmIdentityCache cache = new DatawaveRealmIdentityCache(-1, -1);

        DatawavePrincipal principal1 = createPrincipal(createUser("cn=user1"), createUser("cn=proxyuser1"));
        DatawavePrincipal principal2 = createPrincipal(createUser("cn=user2"), createUser("cn=proxyuser2"));
        DatawavePrincipal principal3 = createPrincipal(createUser("cn=user3"), createUser("cn=proxyuser3"));

        cache.put(principal1, new SimpleNameRealmIdentity("realmUser1"));
        cache.put(principal2, new SimpleNameRealmIdentity("realmUser2"));
        cache.put(principal3, new SimpleNameRealmIdentity("realmUser3"));

        assertTrue(cache.getUsersWhereNameContains("a").isEmpty());
    }

    /**
     * Verify that {@link DatawaveRealmIdentityCache#getUsersWhereNameContains(String)} returns matches.
     */
    @Test
    void testGetUsersWhereNameContainsGivenMatches() {
        DatawaveRealmIdentityCache cache = new DatawaveRealmIdentityCache(-1, -1);

        DatawaveUser userA = createUser("cn=userA");
        DatawaveUser proxyUserA = createUser("cn=proxyUserA");

        DatawavePrincipal principal1 = createPrincipal(userA, createUser("cn=proxyuser1"));
        DatawavePrincipal principal2 = createPrincipal(createUser("cn=user2"), proxyUserA);
        DatawavePrincipal principal3 = createPrincipal(createUser("cn=user3"), createUser("cn=proxyuser3"));

        cache.put(principal1, new SimpleNameRealmIdentity("realmUser1"));
        cache.put(principal2, new SimpleNameRealmIdentity("realmUser2"));
        cache.put(principal3, new SimpleNameRealmIdentity("realmUser3"));

        Set<DatawaveUser> matchingUsers = cache.getUsersWhereNameContains("a");
        assertEquals(2, matchingUsers.size());
        assertTrue(matchingUsers.contains(userA));
        assertTrue(matchingUsers.contains(proxyUserA));
    }

    /**
     * Verify {@link DatawaveRealmIdentityCache#getUserWithName(String)} returns null when the cache is empty.
     */
    @Test
    void testGetUserWithNameGivenEmptyCache() {
        DatawaveRealmIdentityCache cache = new DatawaveRealmIdentityCache(-1, -1);
        assertNull(cache.getUserWithName("cn=user1<cn=issuer1>"));
    }

    /**
     * Verify {@link DatawaveRealmIdentityCache#getUserWithName(String)} returns null when there is no match.
     */
    @Test
    void testGetUserWithNameGivenNoMatch() {
        DatawaveRealmIdentityCache cache = new DatawaveRealmIdentityCache(-1, -1);

        DatawavePrincipal principal1 = createPrincipal(createUser("cn=user1"), createUser("cn=proxyuser1"));
        DatawavePrincipal principal2 = createPrincipal(createUser("cn=user2"), createUser("cn=proxyuser2"));
        DatawavePrincipal principal3 = createPrincipal(createUser("cn=user3"), createUser("cn=proxyuser3"));

        cache.put(principal1, new SimpleNameRealmIdentity("realmUser1"));
        cache.put(principal2, new SimpleNameRealmIdentity("realmUser2"));
        cache.put(principal3, new SimpleNameRealmIdentity("realmUser3"));

        assertNull(cache.getUserWithName("cn=user4<" + TEST_ISSUER + ">"));
    }

    /**
     * Verify {@link DatawaveRealmIdentityCache#getUserWithName(String)} returns a user when there is no match.
     */
    @Test
    void testGetUserWithNameGivenMatch() {
        DatawaveRealmIdentityCache cache = new DatawaveRealmIdentityCache(-1, -1);

        DatawavePrincipal principal1 = createPrincipal(createUser("cn=user1"), createUser("cn=proxyuser1"));
        DatawavePrincipal principal2 = createPrincipal(createUser("cn=user2"), createUser("cn=proxyuser2"));
        DatawavePrincipal principal3 = createPrincipal(createUser("cn=user3"), createUser("cn=proxyuser3"));

        cache.put(principal1, new SimpleNameRealmIdentity("realmUser1"));
        cache.put(principal2, new SimpleNameRealmIdentity("realmUser2"));
        cache.put(principal3, new SimpleNameRealmIdentity("realmUser3"));

        assertNotNull(cache.getUserWithName("cn=user1<" + TEST_ISSUER + ">"));
    }

    /**
     * Verify that {@link DatawaveRealmIdentityCache#evictUsersWithName(String)} with no matches evicts no entries.
     */
    @Test
    void testEvictUserGivenNoMatch() {
        DatawaveRealmIdentityCache cache = new DatawaveRealmIdentityCache(-1, -1);

        DatawavePrincipal principal1 = createPrincipal(createUser("cn=user1"), createUser("cn=proxyuser1"));
        DatawavePrincipal principal2 = createPrincipal(createUser("cn=user2"), createUser("cn=proxyuser2"));
        DatawavePrincipal principal3 = createPrincipal(createUser("cn=user3"), createUser("cn=proxyuser3"));

        cache.put(principal1, new SimpleNameRealmIdentity("realmUser1"));
        cache.put(principal2, new SimpleNameRealmIdentity("realmUser2"));
        cache.put(principal3, new SimpleNameRealmIdentity("realmUser3"));

        cache.evictUsersWithName("cn=user4<" + TEST_ISSUER + ">");

        assertNotNull(cache.get(principal1));
        assertNotNull(cache.get(principal2));
        assertNotNull(cache.get(principal3));
    }

    /**
     * Verify that {@link DatawaveRealmIdentityCache#evictUsersWithName(String)} with matches evicts matching entries.
     */
    @Test
    void testEvictUserGivenMatches() {
        DatawaveRealmIdentityCache cache = new DatawaveRealmIdentityCache(-1, -1);

        DatawavePrincipal principal1 = createPrincipal(createUser("cn=user1"), createUser("cn=proxyuser1"));
        DatawavePrincipal principal2 = createPrincipal(createUser("cn=user2"), createUser("cn=proxyuser2"));
        DatawavePrincipal principal3 = createPrincipal(createUser("cn=user3"), createUser("cn=proxyuser3"));
        DatawavePrincipal principal4 = createPrincipal(createUser("cn=user4"), createUser("cn=proxyuser2"));

        cache.put(principal1, new SimpleNameRealmIdentity("realmUser1"));
        cache.put(principal2, new SimpleNameRealmIdentity("realmUser2"));
        cache.put(principal3, new SimpleNameRealmIdentity("realmUser3"));
        cache.put(principal4, new SimpleNameRealmIdentity("realmUser4"));

        cache.evictUsersWithName("cn=proxyuser2<" + TEST_ISSUER + ">");

        assertNotNull(cache.get(principal1));
        assertNotNull(cache.get(principal3));

        assertNull(cache.get(principal2));
        assertNull(cache.get(principal4));
    }

    /**
     * Verify that {@link DatawaveRealmIdentityCache#put(Principal, RealmIdentity)} stores the entry when the calling thread carries an interrupt. The cache
     * sits on the authentication path, where request threads can be interrupted by a client disconnect or a timeout.
     */
    @Test
    void testPutOnInterruptedThread() {
        DatawaveRealmIdentityCache cache = new DatawaveRealmIdentityCache(-1, -1);
        DatawavePrincipal principal = createPrincipal(createUser("cn=user1"));
        RealmIdentity identity = new SimpleNameRealmIdentity("realmUser1");

        Thread.currentThread().interrupt();
        try {
            cache.put(principal, identity);
        } finally {
            assertTrue(Thread.interrupted(), "the interrupt should not have been consumed");
        }

        assertEquals(identity, cache.get(principal));
    }

    /**
     * Verify that {@link DatawaveRealmIdentityCache#get(Principal)} returns a cached entry when the calling thread carries an interrupt. Reporting a miss here
     * silently forces a full re-authentication for that request.
     */
    @Test
    void testGetOnInterruptedThread() {
        DatawaveRealmIdentityCache cache = new DatawaveRealmIdentityCache(-1, -1);
        DatawavePrincipal principal = createPrincipal(createUser("cn=user1"));
        RealmIdentity identity = new SimpleNameRealmIdentity("realmUser1");
        cache.put(principal, identity);

        Thread.currentThread().interrupt();
        try {
            assertEquals(identity, cache.get(principal));
            assertEquals(identity, cache.get(new NamePrincipal("realmUser1")));
        } finally {
            assertTrue(Thread.interrupted(), "the interrupt should not have been consumed");
        }
    }

    /**
     * Verify that concurrent readers, writers and removers leave the cache consistent and never fail. Reads must also not stall behind the writers.
     */
    @Test
    void testConcurrentAccess() throws Exception {
        DatawaveRealmIdentityCache cache = new DatawaveRealmIdentityCache(-1, -1);

        int threadCount = 16;
        int iterations = 2000;
        List<DatawavePrincipal> principals = new ArrayList<>();
        List<RealmIdentity> identities = new ArrayList<>();
        for (int i = 0; i < 32; i++) {
            principals.add(createPrincipal(createUser("cn=user" + i)));
            identities.add(new SimpleNameRealmIdentity("realmUser" + i));
        }

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int t = 0; t < threadCount; t++) {
                final int offset = t;
                futures.add(executor.submit(() -> {
                    for (int i = 0; i < iterations; i++) {
                        int index = (i + offset) % principals.size();
                        DatawavePrincipal principal = principals.get(index);
                        switch ((i + offset) % 4) {
                            case 0:
                                cache.put(principal, identities.get(index));
                                break;
                            case 1:
                                cache.get(principal);
                                break;
                            case 2:
                                cache.get(new NamePrincipal("realmUser" + index));
                                break;
                            default:
                                cache.remove(principal);
                                break;
                        }
                    }
                }));
            }
            // Any data race in the cache surfaces here as an exception from the worker.
            for (Future<?> future : futures) {
                future.get(60, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }

        // The cache must still be usable and internally consistent once the storm subsides.
        cache.clear();
        DatawavePrincipal principal = createPrincipal(createUser("cn=after"));
        RealmIdentity identity = new SimpleNameRealmIdentity("realmAfter");
        cache.put(principal, identity);
        assertEquals(identity, cache.get(principal));
        assertEquals(identity, cache.get(new NamePrincipal("realmAfter")));
    }

    /**
     * Verify that evicting one domain principal leaves the reverse mapping intact for a sibling domain principal that resolves to the same realm identity
     * principal and still holds a live entry.
     */
    @Test
    void testEvictionRetainsSiblingMappings() {
        // A size of one guarantees the first entry is evicted once the second is written.
        DatawaveRealmIdentityCache cache = new DatawaveRealmIdentityCache(1, -1);

        DatawavePrincipal principal1 = createPrincipal(createUser("cn=user1"));
        DatawavePrincipal principal2 = createPrincipal(createUser("cn=user2"));
        // Both principals resolve to the same realm identity principal.
        cache.put(principal1, new SimpleNameRealmIdentity("sharedRealmUser"));
        RealmIdentity identity2 = new SimpleNameRealmIdentity("sharedRealmUser");
        cache.put(principal2, identity2);

        // Caffeine evicts on a maintenance pass, so poll until the first entry is gone.
        long deadline = System.currentTimeMillis() + 10_000;
        while (cache.get(principal1) != null && System.currentTimeMillis() < deadline) {
            Thread.onSpinWait();
        }
        assertNull(cache.get(principal1), "the first entry should have been evicted on size");

        // The surviving entry must still be reachable by its realm identity principal.
        assertEquals(identity2, cache.get(principal2));
        assertEquals(identity2, cache.get(new NamePrincipal("sharedRealmUser")));
    }

    private DatawaveUser createUser(String subjectDn) {
        SubjectIssuerDNPair dnPair = SubjectIssuerDNPair.of(subjectDn, TEST_ISSUER);
        return new DatawaveUser(dnPair, DatawaveUser.UserType.USER, auths, roles, rolesToAuths, System.currentTimeMillis());
    }

    private DatawavePrincipal createPrincipal(DatawaveUser... users) {
        return new DatawavePrincipal(List.of(users));
    }

    /**
     * A simple {@link RealmIdentity} that returns a {@link NamePrincipal} as its realm identity principal.
     */
    private static class SimpleNameRealmIdentity implements RealmIdentity {

        private final String name;

        public SimpleNameRealmIdentity(String name) {
            this.name = name;
        }

        @Override
        public Principal getRealmIdentityPrincipal() {
            return new NamePrincipal(name);
        }

        @Override
        public SupportLevel getCredentialAcquireSupport(Class<? extends Credential> credentialType, String algorithmName,
                        AlgorithmParameterSpec parameterSpec) {
            return null;
        }

        @Override
        public <C extends Credential> C getCredential(Class<C> credentialType) {
            return null;
        }

        @Override
        public SupportLevel getEvidenceVerifySupport(Class<? extends Evidence> evidenceType, String algorithmName) {
            return null;
        }

        @Override
        public boolean verifyEvidence(Evidence evidence) {
            return false;
        }

        @Override
        public boolean exists() {
            return false;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SimpleNameRealmIdentity that = (SimpleNameRealmIdentity) o;
            return Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(name);
        }
    }
}
