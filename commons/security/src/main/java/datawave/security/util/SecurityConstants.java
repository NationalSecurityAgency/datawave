package datawave.security.util;

/**
 * Contains commonly used security-related constants.
 */
public final class SecurityConstants {

    /**
     * An internal header used to store the start time of the HTTP request, as retrieved from the web container (e.g., Undertow).
     */
    public static final String REQUEST_START_TIME_HEADER = "X-Internal-RequestStartTimeNanos";

    /**
     * An internal header used to store the time required to authenticate the user for the current request.
     */
    public static final String REQUEST_LOGIN_TIME_HEADER = "X-Internal-RequestLoginTimeMillis";

    /**
     * The header used to store proxied subjects.
     */
    public static final String PROXIED_ENTITIES_HEADER = "X-ProxiedEntitiesChain";

    /**
     * The header used to store proxied issuers.
     */
    public static final String PROXIED_ISSUERS_HEADER = "X-ProxiedIssuersChain";

    /**
     * The default header used to store the subject DN when using trusted header authentication.
     */
    public static final String DEFAULT_TRUSTED_SUBJECT_DN_HEADER = "X-SSL-ClientCert-Subject";

    /**
     * The default header used to store the issuer DN when using trusted header authentication.
     */
    public static final String DEFAULT_TRUSTED_ISSUER_DN_HEADER = "X-SSL-ClientCert-Issuer";

    /**
     * The system property used to store whether JWT authentication is enabled.
     */
    public static final String JWT_HEADER_AUTHENTICATION_SYSTEM_PROPERTY = "dw.jwt.header.authentication";

    /**
     * The system property used to store whether trusted header authentication is enabled.
     */
    public static final String TRUSTED_HEADER_AUTHENTICATION_SYSTEM_PROPERTY = "dw.trusted.header.authentication";

    /**
     * The system property used to store the header that will be used to store the subject DN when using trusted header authentication.
     */
    public static final String TRUSTED_SUBJECT_DN_HEADER_SYSTEM_PROPERTY = "dw.trusted.header.subjectDn";

    /**
     * The system property used to store the header that will be used to store the issuer DN when using trusted header authentication.
     */
    public static final String TRUSTED_ISSUER_DN_HEADER_SYSTEM_PROPERTY = "dw.trusted.header.issuerDn";

    /**
     * The system property used to store trusted proxied entities.
     */
    public static final String TRUSTED_PROXIED_ENTITIES_SYSTEM_PROPERTY = "dw.trusted.proxied.entities";

    private SecurityConstants() {
        throw new UnsupportedOperationException();
    }
}
