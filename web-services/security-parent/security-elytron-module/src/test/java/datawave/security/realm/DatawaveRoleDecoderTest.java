package datawave.security.realm;

import static datawave.security.realm.AttributeConstants.ATTRIBUTE_PRIMARY_USER_ROLES;
import static datawave.security.realm.AttributeConstants.ATTRIBUTE_PROXIED_USER_KEYS;
import static datawave.security.realm.AttributeConstants.ATTRIBUTE_TERMINAL_SERVER_ROLES;
import static datawave.security.realm.AttributeConstants.ROLE_AUTHORIZED_SERVER;
import static datawave.security.realm.AttributeConstants.ROLE_AUTHORIZED_USER;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.wildfly.security.authz.AuthorizationIdentity;
import org.wildfly.security.authz.MapAttributes;
import org.wildfly.security.authz.Roles;

/**
 * Tests for {@link DatawaveRoleDecoder}.
 */
class DatawaveRoleDecoderTest {

    private static final String ACCESS_DENIED_ROLE = "AccessDenied";

    private DatawaveRoleDecoder decoder;

    @BeforeEach
    void setUp() {
        decoder = new DatawaveRoleDecoder();
        Map<String,String> configMap = new HashMap<>();
        configMap.put(DatawaveRoleDecoder.Config.OPTION_ACCESS_DENIED_ROLE, ACCESS_DENIED_ROLE);
        decoder.initialize(configMap);
    }

    /**
     * Verify that given an identity without the attribute {@link AttributeConstants#ATTRIBUTE_PRIMARY_USER_ROLES}, an empty role set is returned.
     */
    @Test
    void testDecodeRolesGivenNoPrimaryRolesAttribute() {
        MapAttributes attributes = new MapAttributes();
        AuthorizationIdentity identity = AuthorizationIdentity.basicIdentity(attributes);

        Roles roles = decoder.decodeRoles(identity);
        assertTrue(roles.isEmpty());
    }

    /**
     * Verify that given an identity with the attribute {@link AttributeConstants#ATTRIBUTE_TERMINAL_SERVER_ROLES} that does not contain any of the required
     * terminal server roles, an empty role set is returned.
     */
    @Test
    void testDecodeRolesGivenMissingTerminalServerRole() {
        MapAttributes attributes = new MapAttributes();
        attributes.addLast(ATTRIBUTE_PRIMARY_USER_ROLES, ROLE_AUTHORIZED_USER);
        attributes.addLast(ATTRIBUTE_TERMINAL_SERVER_ROLES, "Foo");

        AuthorizationIdentity identity = AuthorizationIdentity.basicIdentity(attributes);

        Roles roles = decoder.decodeRoles(identity);
        assertTrue(roles.isEmpty());
    }

    /**
     * Verify that given an identity with a proxied user that has the access-denied role, that an empty role set is returned.
     */
    @Test
    void testDecodeRolesGivenAccessDeniedRole() {
        MapAttributes attributes = new MapAttributes();
        attributes.addLast(ATTRIBUTE_PRIMARY_USER_ROLES, "Foo");
        attributes.addLast(ATTRIBUTE_PRIMARY_USER_ROLES, "Bar");
        attributes.addLast(ATTRIBUTE_PRIMARY_USER_ROLES, ROLE_AUTHORIZED_USER);

        attributes.addLast(ATTRIBUTE_TERMINAL_SERVER_ROLES, ROLE_AUTHORIZED_SERVER);

        attributes.addLast(ATTRIBUTE_PROXIED_USER_KEYS, "PROXIED_USER_1");
        attributes.addLast(ATTRIBUTE_PROXIED_USER_KEYS, "PROXIED_USER_2");
        attributes.addLast("PROXIED_USER_1", "Foo");
        attributes.addLast("PROXIED_USER_1", "Bar");
        attributes.addLast("PROXIED_USER_1", ROLE_AUTHORIZED_USER);
        attributes.addLast("PROXIED_USER_2", "Foo");
        attributes.addLast("PROXIED_USER_2", ACCESS_DENIED_ROLE);

        AuthorizationIdentity identity = AuthorizationIdentity.basicIdentity(attributes);

        Roles roles = decoder.decodeRoles(identity);
        assertTrue(roles.isEmpty());
    }

    /**
     * Verify that given an identity with a proxied user's roles that does not contain any of the required roles, the final role set does not have any of the
     * required roles in it.
     */
    @Test
    void testDecodeRolesGivenProxiedUserMissingRequiredRole() {
        MapAttributes attributes = new MapAttributes();
        attributes.addLast(ATTRIBUTE_PRIMARY_USER_ROLES, "Foo");
        attributes.addLast(ATTRIBUTE_PRIMARY_USER_ROLES, "Bar");
        attributes.addLast(ATTRIBUTE_PRIMARY_USER_ROLES, ROLE_AUTHORIZED_USER);

        attributes.addLast(ATTRIBUTE_TERMINAL_SERVER_ROLES, ROLE_AUTHORIZED_SERVER);

        attributes.addLast(ATTRIBUTE_PROXIED_USER_KEYS, "PROXIED_USER_1");
        attributes.addLast(ATTRIBUTE_PROXIED_USER_KEYS, "PROXIED_USER_2");
        attributes.addLast("PROXIED_USER_1", "Foo");
        attributes.addLast("PROXIED_USER_1", "Bar");
        attributes.addLast("PROXIED_USER_1", ROLE_AUTHORIZED_USER);
        attributes.addLast("PROXIED_USER_2", "Foo");

        AuthorizationIdentity identity = AuthorizationIdentity.basicIdentity(attributes);

        Roles roles = decoder.decodeRoles(identity);
        assertTrue(roles.containsAll(Set.of("Foo", "Bar")));
        assertFalse(roles.contains(ROLE_AUTHORIZED_USER));
    }

    /**
     * Verify that given an identity with valid roles, a role set with all expected roles are returned.
     */
    @Test
    void testDecodeRolesGivenValidAttributes() {
        MapAttributes attributes = new MapAttributes();
        attributes.addLast(ATTRIBUTE_PRIMARY_USER_ROLES, "Foo");
        attributes.addLast(ATTRIBUTE_PRIMARY_USER_ROLES, ROLE_AUTHORIZED_USER);

        attributes.addLast(ATTRIBUTE_TERMINAL_SERVER_ROLES, ROLE_AUTHORIZED_SERVER);
        attributes.addLast(ATTRIBUTE_TERMINAL_SERVER_ROLES, "Bar");
        attributes.addLast(ATTRIBUTE_TERMINAL_SERVER_ROLES, "Hat");

        attributes.addLast(ATTRIBUTE_PROXIED_USER_KEYS, "PROXIED_USER_1");
        attributes.addLast(ATTRIBUTE_PROXIED_USER_KEYS, "PROXIED_USER_2");

        attributes.addLast("PROXIED_USER_1", "Foo");
        attributes.addLast("PROXIED_USER_1", ROLE_AUTHORIZED_USER);

        attributes.addLast("PROXIED_USER_2", "Bar");
        attributes.addLast("PROXIED_USER_2", "Hat");
        attributes.addLast("PROXIED_USER_2", ROLE_AUTHORIZED_SERVER);

        AuthorizationIdentity identity = AuthorizationIdentity.basicIdentity(attributes);
        Roles roles = decoder.decodeRoles(identity);

        Set<String> expectedRoles = Set.of(ROLE_AUTHORIZED_USER, "Foo");

        assertTrue(roles.containsAll(expectedRoles));
    }
}
