package datawave.security.cache;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;

/**
 * Tests for {@link DatawaveUserCache}.
 */
class DatawaveUserCacheTest {

    private static final String TEST_ISSUER = "cn=testissuer";
    private static final Set<String> auths = Set.of("A", "B", "C");
    private static final Set<String> roles = Set.of("Administrator", "InternalUser");
    private static final Multimap<String,String> rolesToAuths = ImmutableMultimap.of("Administrator", "A", "Administrator", "B", "Administrator", "C",
                    "InternalUser", "A");

    /**
     * Verify putting a null key results in an NPE.
     */
    @Test
    void testPutGivenNullKey() {
        DatawaveUserCache cache = new DatawaveUserCache(-1, -1);
        Set<DatawaveUser> users = Set.of(createUser("cn=user"), createUser("cn=proxyUser"));
        assertThatThrownBy(() -> cache.put(null, users)).isInstanceOf(NullPointerException.class).hasMessage("key cannot be null");
    }

    /**
     * Verify putting a null value results in an NPE.
     */
    @Test
    void testPutGivenNullValue() {
        DatawaveUserCache cache = new DatawaveUserCache(-1, -1);
        assertThatThrownBy(() -> cache.put("abc", null)).isInstanceOf(NullPointerException.class).hasMessage("user collection cannot be null");
    }

    /**
     * Verify putting and getting entries to/from the cache works as expected.
     */
    @Test
    void testPutAndGet() {
        DatawaveUserCache cache = new DatawaveUserCache(-1, -1);
        Set<DatawaveUser> users = Set.of(createUser("cn=user"), createUser("cn=proxyUser"));
        cache.put("abc", users);
        assertEquals(users, cache.get("abc"));
    }

    /**
     * Test {@link DatawaveUserCache#getUsers()} given an empty cache.
     */
    @Test
    void testGetUsersWithEmptyCache() {
        DatawaveUserCache cache = new DatawaveUserCache(-1, -1);
        assertTrue(cache.getUsers().isEmpty());
    }

    /**
     * Test {@link DatawaveUserCache#getUsers()} given a non-empty cache.
     */
    @Test
    void testGetUsers() {
        DatawaveUserCache cache = new DatawaveUserCache(-1, -1);

        Set<DatawaveUser> users1 = Set.of(createUser("cn=user"), createUser("cn=proxyUser1"));
        cache.put("a", users1);

        Set<DatawaveUser> users2 = Set.of(createUser("cn=user2"), createUser("cn=proxyUser2"));
        cache.put("b", users2);

        Set<DatawaveUser> users3 = Set.of(createUser("cn=user3"), createUser("cn=proxyUser3"));
        cache.put("c", users3);

        Set<DatawaveUser> allUsers = cache.getUsers();
        assertEquals(6, allUsers.size());
        assertTrue(allUsers.containsAll(users1));
        assertTrue(allUsers.containsAll(users2));
        assertTrue(allUsers.containsAll(users3));
    }

    /**
     * Test {@link DatawaveUserCache#getUsersWhereNameContains(String)} given an empty cache.
     */
    @Test
    void testGetUsersWhereNameContainsGivenEmptyCache() {
        DatawaveUserCache cache = new DatawaveUserCache(-1, -1);
        assertTrue(cache.getUsersWhereNameContains("a").isEmpty());
    }

    /**
     * Test {@link DatawaveUserCache#getUsersWhereNameContains(String)} given no matches.
     */
    @Test
    void testGetUsersWhereNameContainsGivenNoMatches() {
        DatawaveUserCache cache = new DatawaveUserCache(-1, -1);

        cache.put("a", Set.of(createUser("cn=user1"), createUser("cn=proxyUser1")));
        cache.put("b", Set.of(createUser("cn=user2"), createUser("cn=proxyUser2")));
        cache.put("c", Set.of(createUser("cn=user3"), createUser("cn=proxyUser3")));

        Set<DatawaveUser> matchingUsers = cache.getUsersWhereNameContains("a");
        assertTrue(matchingUsers.isEmpty());
    }

    /**
     * Test {@link DatawaveUserCache#getUsersWhereNameContains(String)} given matches.
     */
    @Test
    void testGetUsersWhereNameContainsGivenMatches() {
        DatawaveUserCache cache = new DatawaveUserCache(-1, -1);

        DatawaveUser userA = createUser("cn=userA");
        DatawaveUser proxyUserA = createUser("cn=proxyUserA");

        cache.put("a", Set.of(userA, createUser("cn=proxyUser1")));
        cache.put("b", Set.of(createUser("cn=user2"), proxyUserA));
        cache.put("c", Set.of(createUser("cn=user3"), createUser("cn=proxyUser3")));

        Set<DatawaveUser> matchingUsers = cache.getUsersWhereNameContains("a");
        assertEquals(2, matchingUsers.size());
        assertTrue(matchingUsers.contains(userA));
        assertTrue(matchingUsers.contains(proxyUserA));
    }

    /**
     * Test {@link DatawaveUserCache#getUserWithName(String)} given an empty cache.
     */
    @Test
    void testGetUserWithNameGivenEmptyCache() {
        DatawaveUserCache cache = new DatawaveUserCache(-1, -1);
        assertNull(cache.getUserWithName("cn=user1<cn=isser1>"));
    }

    /**
     * Test {@link DatawaveUserCache#getUserWithName(String)} given no match.
     */
    @Test
    void testGetUserWithNameGivenNoMatch() {
        DatawaveUserCache cache = new DatawaveUserCache(-1, -1);

        cache.put("a", Set.of(createUser("cn=user1"), createUser("cn=proxyUser1")));
        cache.put("b", Set.of(createUser("cn=user2"), createUser("cn=proxyUser2")));
        cache.put("c", Set.of(createUser("cn=user3"), createUser("cn=proxyUser3")));

        assertNull(cache.getUserWithName("cn=user4<" + TEST_ISSUER + ">"));
    }

    /**
     * Test {@link DatawaveUserCache#getUserWithName(String)} given a match.
     */
    @Test
    void testGetUserWithNameGivenMatch() {
        DatawaveUserCache cache = new DatawaveUserCache(-1, -1);

        cache.put("a", Set.of(createUser("cn=user1"), createUser("cn=proxyUser1")));
        cache.put("b", Set.of(createUser("cn=user2"), createUser("cn=proxyUser2")));
        cache.put("c", Set.of(createUser("cn=user3"), createUser("cn=proxyUser3")));
        cache.put("d", Set.of(createUser("cn=user4"), createUser("cn=proxyUser2")));

        assertNotNull(cache.getUserWithName("cn=proxyuser2<" + TEST_ISSUER + ">"));
    }

    /**
     * Test {@link DatawaveUserCache#evictUsersWithName(String)} given no match.
     */
    @Test
    void testEvictUserWithNameGivenNoMatch() {
        DatawaveUserCache cache = new DatawaveUserCache(-1, -1);

        cache.put("a", Set.of(createUser("cn=user1"), createUser("cn=proxyUser1")));
        cache.put("b", Set.of(createUser("cn=user2"), createUser("cn=proxyUser2")));
        cache.put("c", Set.of(createUser("cn=user3"), createUser("cn=proxyUser3")));

        cache.evictUsersWithName("cn=proxyuser4<" + TEST_ISSUER + ">");
        assertNotNull(cache.get("a"));
        assertNotNull(cache.get("b"));
        assertNotNull(cache.get("c"));
    }

    /**
     * Test {@link DatawaveUserCache#evictUsersWithName(String)} given matches.
     */
    @Test
    void testEvictUserWithNameGivenMatch() {
        DatawaveUserCache cache = new DatawaveUserCache(-1, -1);

        cache.put("a", Set.of(createUser("cn=user1"), createUser("cn=proxyUser1")));
        cache.put("b", Set.of(createUser("cn=user2"), createUser("cn=proxyUser2")));
        cache.put("c", Set.of(createUser("cn=user3"), createUser("cn=proxyUser3")));
        cache.put("d", Set.of(createUser("cn=user4"), createUser("cn=proxyUser2")));

        cache.evictUsersWithName("cn=proxyuser2<" + TEST_ISSUER + ">");
        assertNotNull(cache.get("a"));
        assertNotNull(cache.get("c"));

        assertNull(cache.get("b"));
        assertNull(cache.get("d"));
    }

    /**
     * Test {@link DatawaveUserCache#clear()}.
     */
    @Test
    void testClear() {
        DatawaveUserCache cache = new DatawaveUserCache(-1, -1);

        cache.put("a", Set.of(createUser("cn=user1"), createUser("cn=proxyUser1")));
        cache.put("b", Set.of(createUser("cn=user2"), createUser("cn=proxyUser2")));
        cache.put("c", Set.of(createUser("cn=user3"), createUser("cn=proxyUser3")));
        cache.put("d", Set.of(createUser("cn=user4"), createUser("cn=proxyUser2")));

        cache.clear();

        assertNull(cache.get("a"));
        assertNull(cache.get("b"));
        assertNull(cache.get("c"));
        assertNull(cache.get("d"));
    }

    private DatawaveUser createUser(String subjectDn) {
        SubjectIssuerDNPair dnPair = SubjectIssuerDNPair.of(subjectDn, TEST_ISSUER);
        return new DatawaveUser(dnPair, DatawaveUser.UserType.USER, auths, roles, rolesToAuths, System.currentTimeMillis());
    }

}
