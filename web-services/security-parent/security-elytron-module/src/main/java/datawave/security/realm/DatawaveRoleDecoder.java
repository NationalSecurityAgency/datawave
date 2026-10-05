package datawave.security.realm;

import static datawave.security.realm.AttributeConstants.ATTRIBUTE_PRIMARY_USER_ROLES;
import static datawave.security.realm.AttributeConstants.ATTRIBUTE_PROXIED_USER_KEYS;
import static datawave.security.realm.AttributeConstants.ATTRIBUTE_TERMINAL_SERVER_ROLES;
import static datawave.security.realm.AttributeConstants.ATTRIBUTE_USERNAME;
import static datawave.security.realm.AttributeConstants.ROLE_AUTHORIZED_PROXIED_SERVER;
import static datawave.security.realm.AttributeConstants.ROLE_AUTHORIZED_QUERY_SERVER;
import static datawave.security.realm.AttributeConstants.ROLE_AUTHORIZED_SERVER;
import static datawave.security.realm.AttributeConstants.ROLE_AUTHORIZED_USER;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wildfly.security.authz.Attributes;
import org.wildfly.security.authz.AuthorizationIdentity;
import org.wildfly.security.authz.RoleDecoder;
import org.wildfly.security.authz.Roles;

/**
 * A {@link RoleDecoder} implementation that will decode roles from the attributes of an {@link AuthorizationIdentity}. The role decoder expects the following
 * mappings in the attributes:
 * <ol>
 * <li>{@value AttributeConstants#ATTRIBUTE_USERNAME}: The identity's username.</li>
 * <li>{@value AttributeConstants#ATTRIBUTE_PRIMARY_USER_ROLES}: The primary user's roles.</li>
 * <li>{@value AttributeConstants#ATTRIBUTE_TERMINAL_SERVER_ROLES}: The roles of the terminal server present in the proxied users, if any.</li>
 * <li>{@value AttributeConstants#ATTRIBUTE_PROXIED_USER_KEYS}: The attribute keys that will map to the roles of individual proxied users.</li>
 * </ol>
 * NOTE: In the case where an empty role set is returned, login will automatically fail when the security domain is configured with the default permissions
 * mapper provided by Wildfly.
 */
public class DatawaveRoleDecoder implements RoleDecoder {

    private static final Logger log = LoggerFactory.getLogger(DatawaveRoleDecoder.class);

    // @formatter:off
    public static final Set<String> defaultRequiredRoles = Set.of(
                    ROLE_AUTHORIZED_USER,
                    ROLE_AUTHORIZED_SERVER,
                    ROLE_AUTHORIZED_QUERY_SERVER,
                    ROLE_AUTHORIZED_PROXIED_SERVER);
    // @formatter:on

    // @formatter:off
    public static final Set<String> defaultTerminalServerRoles = Set.of(
                    ROLE_AUTHORIZED_SERVER,
                    ROLE_AUTHORIZED_QUERY_SERVER);
    // @formatter:on

    /**
     * The configuration for this {@link RoleDecoder}. This will be overridden if configuration properties are passed in from Wildfly via
     * {@link #initialize(Map)}.
     */
    private Config config = Config.fromMap(Map.of());

    /**
     * Initializes this role decoder with the given configuration options. This method is invoked by the Wildfly Elytron subsystem when the decoder is first
     * created to provide configuration parameters. These parameters are typically defined in the Wildfly configuration files, such as jboss .cli files or in
     * the standalone.xml.
     *
     * @param configMap
     *            the configuration
     */
    @SuppressWarnings("unused")
    public void initialize(Map<String,String> configMap) {
        this.config = Config.fromMap(configMap);
    }

    /**
     * Decode the role set for the given identity. The role set will consist of the values present in the attributes
     * {@value AttributeConstants#ATTRIBUTE_PRIMARY_USER_ROLES}. The final role set can be affected if any of the following scenarios occur:
     * <ul>
     * <li>If identity does not have the attribute {@value AttributeConstants#ATTRIBUTE_PRIMARY_USER_ROLES}, an empty role set will be returned.</li>
     * <li>If the role decoder has been configured with required terminal server roles, and the identity has terminal server roles where none of them match a
     * required terminal server role, an empty role set will be returned.</li>
     * <li>If the role decoder has been configured with an access-denied role, and any of the proxied users in the identity have the access-denied role, an
     * empty role set will be returned.</li>
     * <li>If the role decoder has been configured with required roles, and any of the proxied users in the identity do not have at least one of the required
     * roles, the required roles will be removed from the final role set.</li>
     * </ul>
     *
     * @param identity
     *            the authorization identity (not {@code null})
     * @return the final role set
     */
    @Override
    public Roles decodeRoles(AuthorizationIdentity identity) {
        Attributes attributes = identity.getAttributes();
        if (attributes.containsKey(ATTRIBUTE_PRIMARY_USER_ROLES)) {
            // If there was a terminal server present in the list of proxied users, and it does not have at least one of the required terminal server roles,
            // return an empty role set so that login will fail.
            if (attributes.containsKey(ATTRIBUTE_TERMINAL_SERVER_ROLES) && config.hasTerminalServerRoles()) {
                if (Collections.disjoint(config.getTerminalServerRoles(), attributes.get(ATTRIBUTE_TERMINAL_SERVER_ROLES))) {
                    if (log.isWarnEnabled()) {
                        log.warn("User '{}' has terminal server without any required terminal server roles {}, returning empty role set",
                                        attributes.get(ATTRIBUTE_USERNAME), config.getTerminalServerRoles());
                    }
                    return Roles.NONE;
                }
            }

            // Create an initial role set from the primary user roles.
            Set<String> userRoles = new HashSet<>(attributes.get(ATTRIBUTE_PRIMARY_USER_ROLES));

            // If required roles or an access-denied role was configured, check the proxied users.
            boolean requiredRolesRemoved = false;
            if (config.hasRequiredRoles() || config.hasAccessDeniedRole()) {
                Set<String> requiredRoles = config.getRequiredRoles();
                String accessDeniedRole = config.getAccessDeniedRole();
                for (String proxyUserKey : attributes.get(ATTRIBUTE_PROXIED_USER_KEYS)) {
                    Set<String> roles = new HashSet<>(attributes.get(proxyUserKey));

                    // If a proxied user has the access-denied role, return an empty role set.
                    if (accessDeniedRole != null && roles.contains(accessDeniedRole)) {
                        if (log.isWarnEnabled()) {
                            log.warn("User '{}' has access denied role {}, returning empty role set", attributes.get(ATTRIBUTE_USERNAME), accessDeniedRole);
                        }
                        return Roles.NONE;
                    }

                    // If the proxied user does not have any of the required roles, remove all required roles from the final role set.
                    if (!requiredRolesRemoved && !requiredRoles.isEmpty() && Collections.disjoint(requiredRoles, roles)) {
                        if (log.isWarnEnabled()) {
                            log.warn("User '{}' has proxied user without any required roles. Removing all required roles {} from final role set.",
                                            attributes.get(ATTRIBUTE_USERNAME), requiredRoles);
                        }
                        userRoles.removeAll(requiredRoles);

                        // If no access denied role was configured, we do not need to examine any other proxied users.
                        if (!config.hasAccessDeniedRole()) {
                            break;
                        }
                        requiredRolesRemoved = true;
                    }
                }
            }

            if (log.isTraceEnabled()) {
                log.trace("Returning final role set for user {}: {}", attributes.get(ATTRIBUTE_USERNAME), userRoles);
            }

            // Return the final role set for the user.
            return userRoles.isEmpty() ? Roles.NONE : Roles.fromSet(userRoles);
        } else {
            if (log.isWarnEnabled()) {
                log.warn("Unable to decode role without presence of attribute {}", ATTRIBUTE_PRIMARY_USER_ROLES);
            }
            return Roles.NONE;
        }
    }

    /**
     * Configuration options for {@link DatawaveRoleDecoder}.
     */
    public static class Config {

        /**
         * A colon-delimited list of roles that all proxied users are required to have at least one of.
         */
        public static final String OPTION_REQUIRED_ROLES = "requiredRoles";

        /**
         * A role that if present in any of the proxied user roles, will result in no roles being returned for the user. No roles for the user will result in a
         * failed login.
         */
        public static final String OPTION_ACCESS_DENIED_ROLE = "accessDeniedRole";

        /**
         * A colon-delimited list of roles that the terminal server present in the proxied users is required to have at least one of.
         */
        public static final String OPTION_TERMINAL_SERVER_ROLES = "terminalServerRoles";

        public static Config fromMap(Map<String,String> map) {
            // Parse the required roles.
            String requiredProxiedRoleStr = map.get(OPTION_REQUIRED_ROLES);
            Set<String> requiredProxiedRoles;
            if (requiredProxiedRoleStr == null) {
                requiredProxiedRoles = defaultRequiredRoles;
            } else {
                // @formatter:off
                List<String> roles = Arrays.stream(requiredProxiedRoleStr.split(":"))
                                .filter(StringUtils::isNotBlank)
                                .map(String::trim)
                                .collect(Collectors.toList());
                requiredProxiedRoles = roles.isEmpty() ? Set.of() : Set.copyOf(roles);
            }

            // Parse the access denied role.
            String accessDeniedRoleOption = map.get(OPTION_ACCESS_DENIED_ROLE);
            String accessDeniedRole = null;
            if (accessDeniedRoleOption != null && !accessDeniedRoleOption.isBlank()) {
                accessDeniedRole = accessDeniedRoleOption.trim();
            }

            // Parse the terminal server roles.
            String terminalServerRolesOption = map.get(OPTION_TERMINAL_SERVER_ROLES);
            Set<String> terminalServerRoles = defaultTerminalServerRoles;
            if (terminalServerRolesOption != null) {
                List<String> roles = Arrays.stream(terminalServerRolesOption.split(":"))
                                .filter(StringUtils::isNotBlank)
                                .map(String::trim)
                                .collect(Collectors.toList());
                terminalServerRoles = roles.isEmpty() ? Set.of() : Set.copyOf(roles);
            }

            return new Config(requiredProxiedRoles, accessDeniedRole, terminalServerRoles);
        }

        private final Set<String> requiredRoles;
        private final String accessDeniedRole;
        private final Set<String> terminalServerRoles;

        private Config(Set<String> requiredRoles, String accessDeniedRole, Set<String> terminalServerRoles) {
            this.requiredRoles = Set.copyOf(requiredRoles);
            this.accessDeniedRole = accessDeniedRole;
            this.terminalServerRoles = Set.copyOf(terminalServerRoles);
        }

        public Set<String> getRequiredRoles() {
            return requiredRoles;
        }

        public boolean hasRequiredRoles() {
            return !requiredRoles.isEmpty();
        }

        public String getAccessDeniedRole() {
            return accessDeniedRole;
        }

        public boolean hasAccessDeniedRole() {
            return accessDeniedRole != null;
        }

        public Set<String> getTerminalServerRoles() {
            return terminalServerRoles;
        }

        private boolean hasTerminalServerRoles() {
            return !terminalServerRoles.isEmpty();
        }
    }
}
