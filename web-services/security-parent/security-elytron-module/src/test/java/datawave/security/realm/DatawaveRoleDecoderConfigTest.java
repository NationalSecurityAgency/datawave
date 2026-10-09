package datawave.security.realm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link DatawaveRoleDecoder.Config}.
 */
class DatawaveRoleDecoderConfigTest {

    /**
     * Verify the default values of {@link DatawaveRoleDecoder.Config#fromMap(Map)}.
     */
    @Test
    void testDefaultValues() {
        DatawaveRoleDecoder.Config config = DatawaveRoleDecoder.Config.fromMap(Map.of());

        assertThat(config.getRequiredRoles()).containsExactlyInAnyOrderElementsOf(DatawaveRoleDecoder.defaultRequiredRoles);
        assertThat(config.getTerminalServerRoles()).containsExactlyInAnyOrderElementsOf(DatawaveRoleDecoder.defaultTerminalServerRoles);
        assertFalse(config.hasAccessDeniedRole());
        assertNull(config.getAccessDeniedRole());
    }

    /**
     * Verify that when no valid roles are given for required roles or terminal server roles, they are empty.
     */
    @Test
    void testEmptyRequiredRolesAndEmptyTerminalServerRoles() {
        Map<String,String> configMap = new HashMap<>();
        configMap.put(DatawaveRoleDecoder.Config.OPTION_REQUIRED_ROLES, ": : :");
        configMap.put(DatawaveRoleDecoder.Config.OPTION_ACCESS_DENIED_ROLE, "AccessDenied");
        configMap.put(DatawaveRoleDecoder.Config.OPTION_TERMINAL_SERVER_ROLES, ": : :");

        DatawaveRoleDecoder.Config config = DatawaveRoleDecoder.Config.fromMap(configMap);

        assertTrue(config.getRequiredRoles().isEmpty());
        assertTrue(config.getTerminalServerRoles().isEmpty());
        assertThat(config.getAccessDeniedRole()).isEqualTo("AccessDenied");
    }

    /**
     * Verify the values of {@link DatawaveRoleDecoder.Config#fromMap(Map)}.
     */
    @Test
    void testNonDefaultValues() {
        Map<String,String> configMap = new HashMap<>();
        configMap.put(DatawaveRoleDecoder.Config.OPTION_REQUIRED_ROLES, "RequiredA: : RequiredB:");
        configMap.put(DatawaveRoleDecoder.Config.OPTION_ACCESS_DENIED_ROLE, "AccessDenied");
        configMap.put(DatawaveRoleDecoder.Config.OPTION_TERMINAL_SERVER_ROLES, "TerminalServerA: : TerminalServerB:");

        DatawaveRoleDecoder.Config config = DatawaveRoleDecoder.Config.fromMap(configMap);

        assertThat(config.getRequiredRoles()).containsExactlyInAnyOrder("RequiredA", "RequiredB");
        assertThat(config.getTerminalServerRoles()).containsExactlyInAnyOrder("TerminalServerA", "TerminalServerB");
        assertThat(config.getAccessDeniedRole()).isEqualTo("AccessDenied");
    }
}
