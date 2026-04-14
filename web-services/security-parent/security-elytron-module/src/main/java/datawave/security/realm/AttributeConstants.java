package datawave.security.realm;

/**
 * Constants that are frequently used when tracking roles in an {@link org.wildfly.security.authz.Attributes} associated with a security identity.
 */
public final class AttributeConstants {

    /**
     * The identity's username.
     */
    public static final String ATTRIBUTE_USERNAME = "USERNAME";

    /**
     * The primary user's roles.
     */
    public static final String ATTRIBUTE_PRIMARY_USER_ROLES = "PRIMARY_USER_ROLES";

    /**
     * The attribute keys that will map to roles of individual proxied users.
     */
    public static final String ATTRIBUTE_PROXIED_USER_KEYS = "PROXIED_USER_KEYS";

    /**
     * The roles of the terminal server (if any) present in the proxied user.
     */
    public static final String ATTRIBUTE_TERMINAL_SERVER_ROLES = "TERMINAL_SERVER_ROLES";

    public static final String ROLE_AUTHORIZED_USER = "AuthorizedUser";
    public static final String ROLE_AUTHORIZED_SERVER = "AuthorizedServer";
    public static final String ROLE_AUTHORIZED_QUERY_SERVER = "AuthorizedQueryServer";
    public static final String ROLE_AUTHORIZED_PROXIED_SERVER = "AuthorizedProxiedServer";

    private AttributeConstants() {
        throw new UnsupportedOperationException();
    }
}
