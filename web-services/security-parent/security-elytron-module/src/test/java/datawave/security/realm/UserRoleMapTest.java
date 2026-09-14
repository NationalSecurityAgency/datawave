package datawave.security.realm;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

class UserRoleMapTest {

    /**
     * Verify the expected behavior of an empty {@link UserRoleMap}.
     */
    @Test
    void testEmptyUserRolesMap() {
        UserRoleMap userRoleMap = new UserRoleMap();
        assertThat(userRoleMap.isEmpty()).isTrue();

        Collection<String> roles = userRoleMap.get("User A");
        assertThat(roles).isNotNull();
        assertThat(roles).isEmpty();
    }

    /**
     * Verify that when a {@link UserRoleMap} is created via {@link UserRoleMap#UserRoleMap(Multimap)}, null and blank roles are dropped.
     */
    @Test
    void testUserRolesMapDropsBadRoles() {
        Multimap<String,String> rawMap = HashMultimap.create();
        rawMap.putAll("User A", Set.of(" Admin ", " ", " User"));
        rawMap.putAll("User B", Set.of(" DBAdmin ", " ", " User"));
        rawMap.putAll("User C", Set.of(" Guest ", " "));
        rawMap.putAll(null, Set.of(" Admin "));

        Set<String> userDRoles = new HashSet<>();
        userDRoles.add("User");
        userDRoles.add(null);
        rawMap.putAll("User D", userDRoles);

        UserRoleMap userRoleMap = new UserRoleMap(rawMap);
        assertThat(userRoleMap.isEmpty()).isFalse();
        assertThat(userRoleMap.get("User A")).containsExactlyInAnyOrder("Admin", "User");
        assertThat(userRoleMap.get("User B")).containsExactlyInAnyOrder("DBAdmin", "User");
        assertThat(userRoleMap.get("User C")).containsExactlyInAnyOrder("Guest");
        assertThat(userRoleMap.get("User D")).containsExactlyInAnyOrder("User");
    }

    /**
     * Verify that looking up roles via {@link UserRoleMap#get(String)} is case-insensitive.
     */
    @Test
    void testGetByKeyIsCaseInsensitive() {
        Multimap<String,String> rawMap = HashMultimap.create();
        rawMap.putAll("User A", Set.of("Admin", "DBAdmin", "User"));
        rawMap.putAll("User B", Set.of("DBAdmin ", "Manager", " User"));
        rawMap.putAll("User C", Set.of("Guest", "Tester"));

        UserRoleMap userRoleMap = new UserRoleMap(rawMap);
        assertThat(userRoleMap.isEmpty()).isFalse();
        assertThat(userRoleMap.get("user a")).containsExactlyInAnyOrder("Admin", "DBAdmin", "User");
        assertThat(userRoleMap.get("User B")).containsExactlyInAnyOrder("DBAdmin", "Manager", "User");
        assertThat(userRoleMap.get("uSeR C")).containsExactlyInAnyOrder("Guest", "Tester");
    }
}
