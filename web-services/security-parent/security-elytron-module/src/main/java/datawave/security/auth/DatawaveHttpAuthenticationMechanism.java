package datawave.security.auth;

import static datawave.security.auth.DatawaveHttpAuthenticationMechanismFactory.DATAWAVE_AUTH_NAME;
import static datawave.security.util.SecurityConstants.DEFAULT_TRUSTED_ISSUER_DN_HEADER;
import static datawave.security.util.SecurityConstants.DEFAULT_TRUSTED_SUBJECT_DN_HEADER;
import static datawave.security.util.SecurityConstants.PROXIED_ENTITIES_HEADER;
import static datawave.security.util.SecurityConstants.PROXIED_ISSUERS_HEADER;
import static datawave.security.util.SecurityConstants.TRUSTED_ISSUER_DN_HEADER_SYSTEM_PROPERTY;
import static datawave.security.util.SecurityConstants.TRUSTED_SUBJECT_DN_HEADER_SYSTEM_PROPERTY;

import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.Supplier;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wildfly.security.auth.callback.AnonymousAuthorizationCallback;
import org.wildfly.security.auth.callback.AuthenticationCompleteCallback;
import org.wildfly.security.auth.callback.CachedIdentityAuthorizeCallback;
import org.wildfly.security.auth.callback.EvidenceVerifyCallback;
import org.wildfly.security.auth.callback.PrincipalAuthorizeCallback;
import org.wildfly.security.auth.server.SecurityIdentity;
import org.wildfly.security.cache.CachedIdentity;
import org.wildfly.security.cache.IdentityCache;
import org.wildfly.security.evidence.Evidence;
import org.wildfly.security.http.HttpAuthenticationException;
import org.wildfly.security.http.HttpScope;
import org.wildfly.security.http.HttpServerAuthenticationMechanism;
import org.wildfly.security.http.HttpServerRequest;
import org.wildfly.security.http.Scope;
import org.wildfly.security.mechanism.AuthenticationMechanismException;
import org.wildfly.security.x500.X500;

import datawave.security.evidence.EvidenceFactory;
import datawave.security.evidence.JWTEvidence;
import datawave.security.evidence.TrustedHeaderEvidence;
import datawave.security.evidence.X509CertificateEvidence;
import datawave.security.util.SecurityConstants;
import datawave.security.utils.ConfigUtils;

/**
 * A custom {@link HttpServerAuthenticationMechanism} that handles authentication for the mechanism
 * {@value DatawaveHttpAuthenticationMechanismFactory#DATAWAVE_AUTH_NAME}.
 */
public class DatawaveHttpAuthenticationMechanism implements HttpServerAuthenticationMechanism {

    private static final Logger log = LoggerFactory.getLogger(DatawaveHttpAuthenticationMechanism.class);

    /**
     * The HTTP header to fetch JWT tokens from.
     */
    private static final String AUTHORIZATION_HEADER = "Authorization";

    /**
     * The expected prefix for any tokens found in the header '{@value AUTHORIZATION_HEADER}'.
     */
    private static final String JWT_TOKEN_PREFIX = "Bearer ";

    /**
     * The length of the string {@link #JWT_TOKEN_PREFIX}.
     */
    private static final int JWT_TOKEN_PREFIX_LEN = JWT_TOKEN_PREFIX.length();

    /**
     * The key used when attaching a cached authorization result to an HTTP scope.
     */
    protected static final String CACHED_IDENTITY_KEY = DatawaveHttpAuthenticationMechanism.class.getName() + ".elytron-identity";

    /**
     * The configuration for this mechanism.
     */
    private final Config config;

    /**
     * The callback handler that will handle executing any callbacks we need to.
     */
    private final CallbackHandler callbackHandler;

    public DatawaveHttpAuthenticationMechanism(Map<String,?> properties, CallbackHandler callbackHandler) {
        this.callbackHandler = callbackHandler;
        // Parse the configuration.
        this.config = Config.fromMap(properties);
        if (log.isTraceEnabled()) {
            log.trace("Created mechanism with config: {} from properties: {}", config, properties);
        }
    }

    /**
     * Return the mechanism name {@value datawave.security.auth.DatawaveHttpAuthenticationMechanismFactory#DATAWAVE_AUTH_NAME}.
     *
     * @return the mechanism name
     */
    @Override
    public String getMechanismName() {
        return DATAWAVE_AUTH_NAME;
    }

    /**
     * Evaluate and attempt to authenticate the given request.
     *
     * @param request
     *            the request to authenticate
     * @throws HttpAuthenticationException
     *             if an error occurs or authentication fails
     */
    @Override
    public void evaluateRequest(HttpServerRequest request) throws HttpAuthenticationException {
        // If identity restoration is enabled, and we succeed reauthentication, this is a success.
        if (config.isIdentityRestorationEnabled() && attemptReauthentication(request)) {
            log.trace("Reauthentication succeeded");
            return;
        }

        // Otherwise, attempt to perform a new authentication.
        if (attemptAuthentication(request)) {
            log.trace("New authentication succeeded");
            return;
        }

        // If we've reached this point, authentication has failed.
        log.trace("Both re-authentication and authentication failed");

        try {
            // Free any resources required for the authentication process.
            handleCallback(AuthenticationCompleteCallback.FAILED);
        } catch (AuthenticationMechanismException e) {
            throw e.toHttpAuthenticationException();
        }

        // Mark in the request that authentication failed.
        request.authenticationFailed("Authentication failed");
    }

    /**
     * Attempt to reauthenticate the calling user using an identity cached for the session, if any.
     *
     * @param request
     *            the request
     * @return true if reauthentication succeeded, or false otherwise
     * @throws HttpAuthenticationException
     *             if an error occurs
     */
    private boolean attemptReauthentication(HttpServerRequest request) throws HttpAuthenticationException {
        IdentityCache identityCache = createIdentityCache(request);

        // Authorize this attempt if a cached identity is present in the session, and it has permissions.
        CachedIdentityAuthorizeCallback authorizeCallback = new CachedIdentityAuthorizeCallback(identityCache);
        return attemptAuthorization(request, authorizeCallback, authorizeCallback::isAuthorized, identityCache::remove);
    }

    /**
     * Attempt to authenticate the given request.
     *
     * @param request
     *            the request to authenticate
     * @return true if authentication succeeded, or false otherwise
     * @throws HttpAuthenticationException
     *             if an error occurs or authentication fails
     */
    private boolean attemptAuthentication(HttpServerRequest request) throws HttpAuthenticationException {
        // Obtain a piece of evidence that identifies the calling user.
        Evidence evidence;
        try {
            evidence = getEvidence(request);
        } catch (Exception e) {
            throw new HttpAuthenticationException("Error occurred when obtaining evidence for authentication", e);
        }

        // If we failed to obtain any evidence, proceed with anonymous login
        if (evidence == null) {
            log.trace("Failed to obtain any evidence for authentication, proceeding with anonymous login");
            AnonymousAuthorizationCallback authorizeCallback = new AnonymousAuthorizationCallback("anonymous");
            return attemptAuthorization(request, authorizeCallback, authorizeCallback::isAuthorized, null);
        }

        if (log.isTraceEnabled()) {
            log.trace("Attempting authentication with evidence: {}", evidence);
        }

        // Verify the evidence. This will load the decoded principal into the evidence. NOTE: this mechanism expects an evidence decoder such as
        // DatawaveEvidenceDecoder to be configured with the backing security domain.
        EvidenceVerifyCallback evidenceVerifyCallback = new EvidenceVerifyCallback(evidence);
        try {
            handleCallback(evidenceVerifyCallback);
        } catch (AuthenticationMechanismException e) {
            throw e.toHttpAuthenticationException();
        }

        // If the evidence passed verification, attempt to authorize the decoded principal. Authorization will only succeed if the user has valid roles.
        if (evidenceVerifyCallback.isVerified()) {
            // If we should restore identities, cache the identity for the request session if authorization passes.
            if (config.isIdentityRestorationEnabled()) {
                IdentityCache identityCache = createIdentityCache(request);
                CachedIdentityAuthorizeCallback authorizeCallback = new CachedIdentityAuthorizeCallback(evidence.getDecodedPrincipal(), identityCache);
                return attemptAuthorization(request, authorizeCallback, authorizeCallback::isAuthorized, identityCache::remove);
            } else {
                // Otherwise, attempt to authorize the identity and do not cache it.
                PrincipalAuthorizeCallback authorizedCallback = new PrincipalAuthorizeCallback(evidence.getDecodedPrincipal());
                return attemptAuthorization(request, authorizedCallback, authorizedCallback::isAuthorized, null);
            }
        } else {
            if (log.isTraceEnabled()) {
                log.trace("Evidence verification failed with decoded principal {}", evidence.getDecodedPrincipal());
            }
            return false;
        }
    }

    /**
     * Attempt to authorize the request using the given authorization callback to execute the authentication workflow. This method exists to handle different
     * {@link Callback} types that may perform authorization.
     *
     * @param request
     *            the request
     * @param authorizeCallback
     *            the callback
     * @param authorizeResult
     *            the supplier that will return whether authentication succeeded via the callback
     * @param logoutHandler
     *            an operation that should execute when the session logs out. This may be null, or an operation to remove an identity from the identity cache
     *            after the session is finished.
     * @return true if authorization succeeded, or false otherwise
     * @throws HttpAuthenticationException
     *             if an error occurs
     */
    private boolean attemptAuthorization(HttpServerRequest request, Callback authorizeCallback, Supplier<Boolean> authorizeResult, Runnable logoutHandler)
                    throws HttpAuthenticationException {
        // Attempt to authorize the request.
        try {
            handleCallback(authorizeCallback);
        } catch (AuthenticationMechanismException e) {
            throw e.toHttpAuthenticationException();
        }
        // If authorization succeeded, mark the request as successfully authorized.
        if (authorizeResult.get()) {
            succeed(request, logoutHandler);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Mark the request with a successful authentication.
     *
     * @param request
     *            the request to mark as succeeded.
     * @param logoutHandler
     *            an operation that should execute when the session logs out. This may be null, or an operation to remove an identity from the identity cache
     *            after the session is finished.
     */
    private void succeed(HttpServerRequest request, Runnable logoutHandler) throws HttpAuthenticationException {
        try {
            handleCallback(AuthenticationCompleteCallback.SUCCEEDED);
        } catch (AuthenticationMechanismException e) {
            throw e.toHttpAuthenticationException();
        }

        request.authenticationComplete(null, logoutHandler);
    }

    /**
     * Execute the given callbacks using the callback handler of this {@link DatawaveHttpAuthenticationMechanism}.
     *
     * @param callbacks
     *            the callbacks to handle
     * @throws AuthenticationMechanismException
     *             if an authentication error occurs
     */
    private void handleCallback(Callback... callbacks) throws AuthenticationMechanismException {
        try {
            this.callbackHandler.handle(callbacks);
        } catch (AuthenticationMechanismException e) {
            throw e;
        } catch (Throwable e) {
            throw new AuthenticationMechanismException("Callback handler failed for unknown reason", e);
        }
    }

    /**
     * Attempt to extract {@link Evidence} from the request representing the user that can be used for authentication and authorization.
     *
     * @param request
     *            the request
     * @return the first valid {@link Evidence} found, or null if none was found
     * @throws MultipleHeaderValuesException
     *             if a header had multiple values
     * @throws MissingHeaderException
     *             if a header was missing
     */
    private Evidence getEvidence(HttpServerRequest request) throws MultipleHeaderValuesException, MissingHeaderException {
        // First check if we have a JSON web token.
        Evidence evidence = getJwtEvidence(request);
        if (evidence != null) {
            return evidence;
        }

        // Proxied entities and issuers may be specified in configured headers. Extract them for use when either creating evidence from a certificate or trusted
        // headers.
        Pair<String,String> proxiedHeaderValues = getProxiedEntitiesAndIssuers(request);
        String proxiedEntities = proxiedHeaderValues.getLeft();
        String proxiedIssuers = proxiedHeaderValues.getRight();
        if (log.isTraceEnabled()) {
            log.trace("Authenticating with proxied entities={} amd proxied issuers={}", proxiedEntities, proxiedIssuers);
        }

        // Next check if we have a certificate from an SSL session.
        evidence = getX509Evidence(request, proxiedEntities, proxiedIssuers);
        if (evidence != null) {
            return evidence;
        }

        // Lastly check if we have entities stored in trusted headers.
        return getTrustedHeadersEvidence(request, proxiedEntities, proxiedIssuers);
    }

    /**
     * Extract proxied entities and proxied issuers (if any) from the request headers {@value SecurityConstants#PROXIED_ENTITIES_HEADER} and
     * {@value SecurityConstants#PROXIED_ISSUERS_HEADER}.
     *
     * @param request
     *            the request to extract the entities from
     * @return the pair of values representing the proxied entities and issuers
     * @throws MultipleHeaderValuesException
     *             if multiple values were found in the header
     * @throws MissingHeaderException
     *             if proxied entities were provided, but proxied issuers were not.
     */
    private Pair<String,String> getProxiedEntitiesAndIssuers(HttpServerRequest request) throws MultipleHeaderValuesException, MissingHeaderException {
        String proxiedEntities = getSingularHeaderValue(request, PROXIED_ENTITIES_HEADER);
        String proxiedIssuers = getSingularHeaderValue(request, PROXIED_ISSUERS_HEADER);

        // If proxied entities are specified, but proxied issuers are not, then fail authentication immediately.
        if (proxiedEntities != null && proxiedIssuers == null) {
            throw new MissingHeaderException(PROXIED_ENTITIES_HEADER + " provided, but missing " + PROXIED_ISSUERS_HEADER);
        }

        return Pair.of(proxiedEntities, proxiedIssuers);
    }

    /**
     * Attempt to find and return evidence based on a JWT token from the header {@value AUTHORIZATION_HEADER} in the request.
     *
     * @param request
     *            the request to examine
     * @return a {@link JWTEvidence} if a JWT token was found, or null otherwise
     * @throws MultipleHeaderValuesException
     *             if multiple token values were found in the header
     */
    private JWTEvidence getJwtEvidence(HttpServerRequest request) throws MultipleHeaderValuesException {
        String authorizationHeader = getSingularHeaderValue(request, AUTHORIZATION_HEADER);
        if (authorizationHeader != null && authorizationHeader.startsWith(JWT_TOKEN_PREFIX)) {
            String jwtToken = authorizationHeader.substring(JWT_TOKEN_PREFIX_LEN);
            return EvidenceFactory.getDefault().createJwtEvidence(jwtToken);
        } else {
            return null;
        }
    }

    /**
     * Attempt to find and return evidence based off certificates from the SSL session.
     *
     * @param request
     *            the request to examine
     * @param proxiedSubjects
     *            the proxied subjects (if any)
     * @param proxiedIssuers
     *            the proxied issuers (if any)
     * @return a {@link X509CertificateEvidence} if a certificate was found in the SSL session, or null otherwise
     */
    private X509CertificateEvidence getX509Evidence(HttpServerRequest request, String proxiedSubjects, String proxiedIssuers) {
        if (request.getSSLSession() != null) {
            Certificate[] peerCertificates = request.getPeerCertificates();
            if (peerCertificates != null) {
                // If the request has any peer certificates, grab the first one.
                X509Certificate[] x509Certificates = X500.asX509CertificateArray(peerCertificates);
                X509Certificate certificate = x509Certificates[0];
                return EvidenceFactory.getDefault().createX509CertificateEvidence(certificate, proxiedSubjects, proxiedIssuers);
            }
        }
        return null;
    }

    /**
     * Attempt to find and return evidence based of trusted headers.
     *
     * @param request
     *            the request to examine
     * @param proxiedSubjects
     *            the proxied subjects (if any)
     * @param proxiedIssuers
     *            the proxied issuers (if any)
     * @return a {@link TrustedHeaderEvidence} if trusted subject and issuer DNs were found, or null otherwise
     * @throws MultipleHeaderValuesException
     *             if multiple DNs were found in the headers
     * @throws MissingHeaderException
     *             if either a subject DN or issuer DN was provided, but its counterpart was not
     */
    private TrustedHeaderEvidence getTrustedHeadersEvidence(HttpServerRequest request, String proxiedSubjects, String proxiedIssuers)
                    throws MultipleHeaderValuesException, MissingHeaderException {
        String subjectDn = getSingularHeaderValue(request, config.getTrustedSubjectDnHeader());
        String issuerDn = getSingularHeaderValue(request, config.getTrustedIssuerDnHeader());

        // If no DN headers were supplied, we cannot create trusted header evidence.
        if (subjectDn == null && issuerDn == null) {
            return null;
        }

        // Require both a subject DN and issuer DN to be specified.
        if (subjectDn == null || issuerDn == null) {
            throw new MissingHeaderException(
                            "Missing trusted subject DN (" + subjectDn + ") or issuer DN (" + issuerDn + ") for trusted header authentication");
        }

        return EvidenceFactory.getDefault().createTrustedHeadersEvidence(subjectDn, issuerDn, proxiedSubjects, proxiedIssuers);
    }

    /**
     * Returns the value if one was provided for the given header name in the given http request. If no value was provided, null will be returned. If multiple
     * values were provided, an exception will be thrown.
     *
     * @param httpServerRequest
     *            the http request
     * @param headerName
     *            the header name
     * @return the value, possibly null
     * @throws MultipleHeaderValuesException
     *             if multiple values were provided for the header
     */
    private String getSingularHeaderValue(HttpServerRequest httpServerRequest, String headerName) throws MultipleHeaderValuesException {
        List<String> values = httpServerRequest.getRequestHeaderValues(headerName);
        if (values != null && !values.isEmpty()) {
            if (values.size() > 1) {
                throw new MultipleHeaderValuesException(headerName + " may not be specified multiple times");
            }
            return values.get(0);
        } else {
            return null;
        }
    }

    /**
     * Create an identity cache that can be associated with the SESSION scope of the request.
     *
     * @param request
     *            the request
     * @return the identity cache
     */
    private IdentityCache createIdentityCache(HttpServerRequest request) {
        return new IdentityCache() {

            /**
             * Attempt to attach the given identity to the request's SESSION scope.
             *
             * @param identity
             *            the identity to cache (not {@code null})
             */
            @Override
            public void put(SecurityIdentity identity) {
                // Attempt to get an attachable SESSION scope for the request, creating it if need be. If we cannot obtain the scope, return early.
                HttpScope scope = getAttachableSessionScope(request, true);
                if (scope == null || !scope.exists()) {
                    return;
                }

                // If we are associating an identity with the session for the first time, change the ID of the session unless otherwise disabled.
                if (config.isSessionIdChangeEnabled() && scope.getAttachment(CACHED_IDENTITY_KEY) == null) {
                    scope.changeID();
                }

                // Wrap the identity in a CachedIdentity and attach it to the scope.
                CachedIdentity cachedIdentity = new CachedIdentity(getMechanismName(), false, identity);
                scope.setAttachment(CACHED_IDENTITY_KEY, cachedIdentity);
            }

            /**
             * Return the cached identity attached to the SESSION scope of the request.
             *
             * @return the cached identity, or null if no identity is cached
             */
            @Override
            public CachedIdentity get() {
                // If we cannot obtain a scope that could have an attached identity, return early.
                HttpScope scope = getAttachableSessionScope(request, false);
                if (scope == null || !scope.exists()) {
                    return null;
                }

                return (CachedIdentity) scope.getAttachment(CACHED_IDENTITY_KEY);
            }

            /**
             * Delete the cached identity (if any) attached to the SESSION scope of the request.
             *
             * @return the identity that was removed, possibly null
             */
            @Override
            public CachedIdentity remove() {
                // If we cannot obtain a scope that could have an attached identity, return early.
                HttpScope scope = getAttachableSessionScope(request, false);
                if (scope == null || !scope.exists()) {
                    return null;
                }

                CachedIdentity identity = (CachedIdentity) scope.getAttachment(CACHED_IDENTITY_KEY);
                scope.setAttachment(CACHED_IDENTITY_KEY, null);
                return identity;
            }
        };
    }

    /**
     * Return the SESSION scope for the request if it exists and supports attachments.
     *
     * @param request
     *            the request
     * @param createSession
     *            whether to create the session if it does not exist
     * @return the scope, or null if no attachable SESSION scope could be obtained
     */
    private HttpScope getAttachableSessionScope(HttpServerRequest request, boolean createSession) {
        HttpScope scope = request.getScope(Scope.SESSION);

        // If no scope could be obtained, or it doesn't support attachments, return null.
        if (scope == null || !scope.supportsAttachments()) {
            return null;
        }

        // Create the scope if indicated.
        if (!scope.exists() && createSession) {
            scope.create();
        }

        return scope;
    }

    /**
     * Configuration class that will handle parsing configuration options for a {@link DatawaveHttpAuthenticationMechanism} instance.
     */
    public static class Config {

        /**
         * The header that will be used to pass in the trusted subject DN when using trusted header authentication.
         */
        public static final String OPTION_TRUSTED_SUBJECT_DN_HEADER = "trustedSubjectDnHeader";

        /**
         * The header that will be used to pass in the trusted issuer DN when using trusted header authentication.
         */
        public static final String OPTION_TRUSTED_ISSUER_DN_HEADER = "trustedIssuerDnHeader";

        /**
         * Whether to enable the ability to restore identities for the HTTP session.
         */
        public static final String OPTION_ENABLE_RESTORE_IDENTITY = "enableRestoreIdentity";

        /**
         * Whether to enable the ability to change the session ID after an identity is first established.
         */
        public static final String OPTION_ENABLE_SESSION_ID_CHANGE = "enableSessionIdChange";

        private final String trustedSubjectDnHeader;
        private final String trustedIssuerDnHeader;
        private final boolean identityRestorationEnabled;
        private final boolean sessionIdChangeEnabled;

        public static Config fromMap(Map<String,?> properties) {
            String trustedSubjectDnHeader = getValue((String) properties.get(OPTION_TRUSTED_SUBJECT_DN_HEADER), TRUSTED_SUBJECT_DN_HEADER_SYSTEM_PROPERTY,
                            DEFAULT_TRUSTED_SUBJECT_DN_HEADER);
            String trustedIssuerDnHeader = getValue((String) properties.get(OPTION_TRUSTED_ISSUER_DN_HEADER), TRUSTED_ISSUER_DN_HEADER_SYSTEM_PROPERTY,
                            DEFAULT_TRUSTED_ISSUER_DN_HEADER);

            boolean identityRestorationEnabled = ConfigUtils.getBoolean((String) properties.get(OPTION_ENABLE_RESTORE_IDENTITY), true);
            boolean sessionIdChangeEnabled = ConfigUtils.getBoolean((String) properties.get(OPTION_ENABLE_SESSION_ID_CHANGE), true);

            return new Config(trustedSubjectDnHeader, trustedIssuerDnHeader, identityRestorationEnabled, sessionIdChangeEnabled);
        }

        private static String getValue(String mapValue, String systemProperty, String defaultValue) {
            String value = ConfigUtils.getString(mapValue, null);
            if (value == null) {
                value = ConfigUtils.getString(System.getProperty(systemProperty), defaultValue);
            }
            return value;
        }

        public Config(String trustedSubjectDnHeader, String trustedIssuerDnHeader, boolean identityRestorationEnabled, boolean sessionIdChangeEnabled) {
            this.trustedSubjectDnHeader = trustedSubjectDnHeader;
            this.trustedIssuerDnHeader = trustedIssuerDnHeader;
            this.identityRestorationEnabled = identityRestorationEnabled;
            this.sessionIdChangeEnabled = sessionIdChangeEnabled;
        }

        public String getTrustedSubjectDnHeader() {
            return trustedSubjectDnHeader;
        }

        public String getTrustedIssuerDnHeader() {
            return trustedIssuerDnHeader;
        }

        public boolean isIdentityRestorationEnabled() {
            return identityRestorationEnabled;
        }

        public boolean isSessionIdChangeEnabled() {
            return sessionIdChangeEnabled;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", Config.class.getSimpleName() + "[", "]").add("trustedSubjectDnHeader='" + trustedSubjectDnHeader + "'")
                            .add("trustedIssuerDnHeader='" + trustedIssuerDnHeader + "'").add("identityRestorationEnabled=" + identityRestorationEnabled)
                            .add("sessionIdChangeEnabled=" + sessionIdChangeEnabled).toString();
        }
    }
}
