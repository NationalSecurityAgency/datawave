package datawave.security.realm;

import static datawave.security.realm.AttributeConstants.ATTRIBUTE_PRIMARY_USER_ROLES;
import static datawave.security.realm.AttributeConstants.ATTRIBUTE_PROXIED_USER_KEYS;
import static datawave.security.realm.AttributeConstants.ATTRIBUTE_TERMINAL_SERVER_ROLES;
import static datawave.security.realm.AttributeConstants.ATTRIBUTE_USERNAME;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.Principal;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wildfly.security.auth.SupportLevel;
import org.wildfly.security.auth.principal.NamePrincipal;
import org.wildfly.security.auth.server.RealmIdentity;
import org.wildfly.security.auth.server.RealmUnavailableException;
import org.wildfly.security.auth.server.SecurityRealm;
import org.wildfly.security.authz.Attributes;
import org.wildfly.security.authz.AuthorizationIdentity;
import org.wildfly.security.authz.MapAttributes;
import org.wildfly.security.credential.Credential;
import org.wildfly.security.evidence.Evidence;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.cache.DatawaveRealmIdentityCache;
import datawave.security.cache.ElytronCacheManager;
import datawave.security.cert.DatawaveCertVerifier;
import datawave.security.cert.SSLStores;
import datawave.security.cert.X509CertificateVerifier;
import datawave.security.evidence.DatawaveEvidence;
import datawave.security.evidence.X509CertificateEvidence;
import datawave.security.evidence.X509EvidenceValidator;
import datawave.security.system.SecurityEJBProvider;
import datawave.security.utils.ConfigUtils;

/**
 * A {@link SecurityRealm} implementation that will handle the creation of {@link RealmIdentity} when authentication users against the Datawave security domain.
 * This realm is responsible for converting a {@link DatawavePrincipal} to a {@link RealmIdentity} with an {@link Attributes} containing attributes with mapped
 * roles that can later be used by the {@link DatawaveRoleDecoder} to map the roles for the final security identity.
 */
public class DatawaveSecurityRealm implements SecurityRealm {

    private static final Logger log = LoggerFactory.getLogger(DatawaveSecurityRealm.class);

    /**
     * Base attribute name when mapping roles for individual proxied users in an {@link Attributes}.
     */
    private static final String ATTRIBUTE_PROXY_USER_KEY_BASE = "PROXIED_USER_";

    /**
     * The original configuration map passed to {@link #initialize(Map)}.
     */
    private Map<String,String> configMap;

    /**
     * The configuration properties. This is updated if {@link #initialize(Map)} is called.
     */
    private Config config = Config.fromMap(Map.of());

    /**
     * Indicates whether {@link #completeInitialization()} was called.
     */
    private volatile boolean initializationComplete = false;

    /**
     * The validator that, if configured, will validate the certificate of any {@link X509CertificateEvidence} instances passed to this realm for verification.
     */
    private X509EvidenceValidator x509EvidenceValidator = null;

    /**
     * The realm identity cache. This cache will hold mappings of principals to realm identities to improve performance, and removes the need to wrap this
     * security realm in a Wildfly caching realm.
     */
    private volatile DatawaveRealmIdentityCache realmIdentityCache;

    /**
     * A provider for EJBs not normally injectable within an Elytron context.
     */
    private SecurityEJBProvider securityEJBProvider;

    /**
     * The local user roles loaded from the properties file specified in the {@value Config#OPTION_ROLE_PROPERTIES} config option.
     */
    private UserRoleMap localUserRoles = new UserRoleMap();

    /**
     * This method is invoked by the Wildfly Elytron subsystem when the realm is first created to provide configuration parameters. These parameters are
     * typically defined in the Wildfly configuration files, such as jboss .cli files or in the standalone.xml.
     * <p>
     * NOTE: Any configuration parameters passed into here will be parsed and stored, but any configuration steps requiring the presence of CDI beans will not
     * take effect until the first time {@link #getRealmIdentity(Principal)} is called.
     * <p>
     *
     * @param config
     *            the configuration parameters
     * @see Config#fromMap(Map) The list of supported configuration parameters
     */
    @SuppressWarnings("unused")
    public void initialize(Map<String,String> config) {
        // Parse the configuration properties.
        this.configMap = config;
        this.config = Config.fromMap(config);
        this.initializationComplete = false;

    }

    /**
     * Complete the initialization of this security realm. This initialization is expected to be completed the first time {@link #getRealmIdentity(Principal)}
     * is called. Some of these initialization steps require access to EJBs that will not be available during the initial start up of Wildfly, so we must wait
     * until the first authentication attempt.
     *
     * @throws RealmUnavailableException
     *             if an error occurs while completing initialization
     */
    private void completeInitialization() throws RealmUnavailableException {
        // Checked without synchronizing so that the steady state stays lock free. Concurrent first requests fall through to the synchronized path, which
        // rechecks, so the realm is initialized exactly once and never publishes a half built cache.
        if (!initializationComplete) {
            initializeOnce();
        }
    }

    /**
     * Perform the one time initialization guarded by this realm's monitor.
     *
     * @throws RealmUnavailableException
     *             if an error occurs while completing initialization
     */
    private synchronized void initializeOnce() throws RealmUnavailableException {
        if (!initializationComplete) {
            try {
                logConfig();
                initCache();
                initX509EvidenceValidator();
                loadLocalUserRoles();
                this.initializationComplete = true;
            } catch (Exception e) {
                log.error("Failed to complete realm initialization", e);
                throw new RealmUnavailableException("Failed to complete realm initialization", e);
            }
        }
    }

    /**
     * Log the configuration that was loaded when {@link #initialize(Map)} was invoked. Logging statements in that method are not captured by the logging
     * framework, so we'll do it once here.
     */
    private void logConfig() {
        if (log.isDebugEnabled()) {
            log.debug("configMap={}", this.configMap);
            log.debug("config={}", this.config);
        }
    }

    /**
     * Initialize the realm identity cache used by this realm. The cache will be added to the {@link ElytronCacheManager} returned by the security EJB provider
     * so that we can invoke lookup and eviction methods on it in the credentials cache bean.
     */
    private void initCache() {
        this.realmIdentityCache = new DatawaveRealmIdentityCache(this.config.getMaxCacheEntries(), this.config.getMaxCacheAge());
        ElytronCacheManager elytronCacheManager = getSecurityEJBProvider().getElytronCacheManager();
        if (elytronCacheManager != null) {
            elytronCacheManager.addCache(this.realmIdentityCache);
            log.debug("Added realm idenitity cache to the cache manager");
        } else {
            log.debug("No elytron cache manager configured");
        }
    }

    /**
     * Initialize a {@link X509EvidenceValidator} if required. This will handle validating the certificate of any {@link X509CertificateEvidence} passed to this
     * realm for verification.
     */
    private void initX509EvidenceValidator() {
        // If a verifier class was configured, attempt to create an instance of it.
        String certVerifierClass = config.getCertVerifierClass();
        if (certVerifierClass != null) {
            X509CertificateVerifier certificateVerifier;
            try {
                ClassLoader loader = Thread.currentThread().getContextClassLoader();
                Class<?> verifierClass = loader.loadClass(certVerifierClass);
                certificateVerifier = (X509CertificateVerifier) verifierClass.getDeclaredConstructor().newInstance();

                // If this is a DatawaveCertVerifier, additional configuration is required.
                if (certificateVerifier instanceof DatawaveCertVerifier) {
                    ((DatawaveCertVerifier) certificateVerifier).setLogger(log);
                    String oscpLevel = config.getOscpLevel();
                    if (oscpLevel != null && !oscpLevel.isBlank()) {
                        ((DatawaveCertVerifier) certificateVerifier).setOcspLevel(oscpLevel);
                    }
                }

                SSLStores sslStores = getSecurityEJBProvider().getSSLStores();
                this.x509EvidenceValidator = new X509EvidenceValidator(certificateVerifier, sslStores.getKeyStore(), sslStores.getTrustStore());
                if (log.isDebugEnabled()) {
                    log.debug("Initialized X509 evidence validator with certificate validator {}", certificateVerifier.getClass().getName());
                }
            } catch (Throwable e) {
                log.error("Failed to create create X509 evidence validator", e);
                throw new IllegalStateException("Failed to create X509 evidence validator", e);
            }
        }
    }

    /**
     * Return the {@link X509EvidenceValidator} configured for this {@link DatawaveSecurityRealm}.
     *
     * @return the validator
     */
    X509EvidenceValidator getX509EvidenceValidator() {
        return this.x509EvidenceValidator;
    }

    /**
     * Loads a map of usernames to local server roles from a properties file if configured.
     */
    private void loadLocalUserRoles() {
        // If no role properties path was specified, return early.
        String rolePropertiesPath = config.getRolePropertiesPath();
        if (rolePropertiesPath == null) {
            return;
        }

        // Load properties from the file.
        Properties properties = new Properties();
        try (InputStream fis = Files.newInputStream(Paths.get(rolePropertiesPath))) {
            properties.load(fis);
        } catch (IOException e) {
            log.error("Failed to load local role properties file {}", rolePropertiesPath, e);
            throw new IllegalStateException("Failed to load local role properties file " + rolePropertiesPath, e);
        }

        // Extract the roles for each username.
        Multimap<String,String> localRoles = HashMultimap.create();
        for (String username : properties.stringPropertyNames()) {
            String roleStr = properties.getProperty(username);
            if (roleStr != null && !roleStr.isBlank()) {
                // Roles are expected to be comma-delimited.
                Collection<String> roles = Arrays.asList(roleStr.split(","));
                localRoles.putAll(username, roles);
            }
        }

        this.localUserRoles = new UserRoleMap(localRoles);
        if (log.isDebugEnabled()) {
            log.debug("Successfully loaded local user roles from properties file {}", rolePropertiesPath);
        }
    }

    /**
     * Return the local user roles loaded for this {@link DatawaveSecurityRealm}.
     *
     * @return the local user roles.
     */
    UserRoleMap getLocalUserRoles() {
        return this.localUserRoles;
    }

    /**
     * Exposed for the purpose of allowing a {@link SecurityEJBProvider} to be set for testing purposes without needing to worry about JNDI.
     *
     * @param securityEJBProvider
     *            the provider
     */
    public void setSecurityEJBProvider(SecurityEJBProvider securityEJBProvider) {
        this.securityEJBProvider = securityEJBProvider;
    }

    /**
     * Return the {@link SecurityEJBProvider} instance. If no provider was configured for this realm, the instance loaded from JNDI will be returned.
     *
     * @return the provider
     */
    private SecurityEJBProvider getSecurityEJBProvider() {
        return this.securityEJBProvider == null ? SecurityEJBUtils.getSecurityEJBProvider() : this.securityEJBProvider;
    }

    /**
     * Always returns {@link SupportLevel#UNSUPPORTED}.
     *
     * @return {@link SupportLevel#UNSUPPORTED}
     */
    @Override
    public SupportLevel getCredentialAcquireSupport(Class<? extends Credential> credentialType, String algorithmName, AlgorithmParameterSpec parameterSpec) {
        Preconditions.checkNotNull(credentialType, "Parameter credentialType cannot be null");
        return SupportLevel.UNSUPPORTED;
    }

    /**
     * Return a {@link SupportLevel} indicating whether the given evidence type is supported by this realm.
     *
     * @return {@link SupportLevel#POSSIBLY_SUPPORTED} if the evidence type is a {@link DatawaveEvidence}, or {@link SupportLevel#UNSUPPORTED} otherwise
     */
    @Override
    public SupportLevel getEvidenceVerifySupport(Class<? extends Evidence> evidenceType, String algorithmName) {
        Preconditions.checkNotNull(evidenceType, "Parameter evidenceType may not be null");
        return DatawaveEvidence.class.isAssignableFrom(evidenceType) ? SupportLevel.POSSIBLY_SUPPORTED : SupportLevel.UNSUPPORTED;
    }

    /**
     * Return a realm identity for the given principal. It is expected that this principal will be a {@link DatawavePrincipal} that was decoded from a piece of
     * {@link Evidence} using {@link DatawaveEvidenceDecoder}. If a realm identity is already cached for the principal, that identity will be returned.
     * Otherwise, a new realm identity will be created and returned after caching it.
     *
     * @param principal
     *            the principal which identifies the identity within the realm (must not be {@code null})
     * @return the realm identity
     * @throws RealmUnavailableException
     *             if an error occurs
     */
    @Override
    public RealmIdentity getRealmIdentity(Principal principal) throws RealmUnavailableException {
        // Ensure we complete initialization for this realm.
        completeInitialization();

        // If we already have a realm identity cached for the principal, return the realm identity.
        RealmIdentity cached = realmIdentityCache.get(principal);
        if (cached != null) {
            if (log.isTraceEnabled()) {
                log.trace("Returning cached identity for principal {}", principal.getName());
            }
            return cached;
        }

        // Otherwise create a new realm identity for the principal.
        if (principal instanceof DatawavePrincipal) {
            // Create a CachedRealmIdentity that wraps around a DatawaveRealmIdentity and add it to the cache before returning it.
            RealmIdentity realmIdentity = new DatawaveRealmIdentity((DatawavePrincipal) principal);
            CachedRealmIdentity cachedIdentity = new CachedRealmIdentity(realmIdentity);
            realmIdentityCache.put(principal, cachedIdentity);
            if (log.isTraceEnabled()) {
                log.trace("Created realm identity for principal {} with realm identity principal {} and attributes {}", principal,
                                cachedIdentity.getRealmIdentityPrincipal(), cachedIdentity.getAttributes().entries());
            }
            return cachedIdentity;
        } else {
            // If somehow we were given a non-DatawavePrincipal, log a warning and return a NON_EXISTENT realm identity.
            if (log.isWarnEnabled()) {
                log.warn("Returning NON_EXISTENT identity for a principal that is not an instance of {} but is a {}: {}", DatawavePrincipal.class.getName(),
                                principal.getClass().getName(), principal);
            }
            return RealmIdentity.NON_EXISTENT;
        }
    }

    /**
     * A wrapper {@link RealmIdentity} class that will cache some aspects of the information
     */
    private static class CachedRealmIdentity implements RealmIdentity {

        private final RealmIdentity identity;
        // A cached identity is shared by every concurrent request that authenticates as the same principal, and verifyEvidence resets both fields. They are
        // volatile so that a request never observes a partially published value from another request's verification.
        private volatile AuthorizationIdentity authorizationIdentity;
        private volatile Attributes attributes;

        public CachedRealmIdentity(RealmIdentity identity) {
            this.identity = identity;
        }

        @Override
        public Principal getRealmIdentityPrincipal() {
            return identity.getRealmIdentityPrincipal();
        }

        @Override
        public SupportLevel getCredentialAcquireSupport(Class<? extends Credential> credentialType, String algorithmName, AlgorithmParameterSpec parameterSpec)
                        throws RealmUnavailableException {
            return identity.getCredentialAcquireSupport(credentialType, algorithmName, parameterSpec);
        }

        @Override
        public <C extends Credential> C getCredential(Class<C> credentialType) throws RealmUnavailableException {
            return identity.getCredential(credentialType);
        }

        @Override
        public SupportLevel getEvidenceVerifySupport(Class<? extends Evidence> evidenceType, String algorithmName) throws RealmUnavailableException {
            return identity.getEvidenceVerifySupport(evidenceType, algorithmName);
        }

        @Override
        public boolean verifyEvidence(Evidence evidence) throws RealmUnavailableException {
            // Nullify the attributes and authorization identity so that the next time their getters are called, we store fresh instances from the wrapped
            // identity. The attributes (and authorization identity) can be changed as a side effect of the call to verifyEvidence here.
            this.attributes = null;
            this.authorizationIdentity = null;
            return identity.verifyEvidence(evidence);
        }

        @Override
        public boolean exists() throws RealmUnavailableException {
            return identity.exists();
        }

        @Override
        public void dispose() {
            identity.dispose();
        }

        @Override
        public AuthorizationIdentity getAuthorizationIdentity() throws RealmUnavailableException {
            if (authorizationIdentity == null) {
                authorizationIdentity = identity.getAuthorizationIdentity();
            }
            return authorizationIdentity;
        }

        @Override
        public Attributes getAttributes() throws RealmUnavailableException {
            if (attributes == null) {
                attributes = identity.getAttributes();
            }
            return attributes;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CachedRealmIdentity that = (CachedRealmIdentity) o;
            return Objects.equals(identity, that.identity);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(identity);
        }
    }

    /**
     * A {@link RealmIdentity} implementation that represents a specific authentication attempt against this security realm.
     */
    private class DatawaveRealmIdentity implements RealmIdentity {

        private final String principalName;

        // The attributes decoded from the principal alone. verifyEvidence derives from these rather than from the last verification's result, so that
        // per-evidence roles are not re-added to a cached identity on every request.
        private final Attributes baseAttributes;

        // Mutated by verifyEvidence on an instance shared by every concurrent request for this principal.
        private volatile Attributes attributes;

        public DatawaveRealmIdentity(DatawavePrincipal principal) {
            this.principalName = principal.getName();
            this.baseAttributes = loadAttributes(principal);
            this.attributes = this.baseAttributes;
        }

        /**
         * Return a new {@link Attributes} instance with mappings for the username and roles found within the given principal.
         *
         * @param principal
         *            the principal
         * @return the new {@link Attributes}
         */
        private Attributes loadAttributes(DatawavePrincipal principal) {
            if (principal == null) {
                return Attributes.EMPTY;
            }

            MapAttributes mapAttributes = new MapAttributes();

            // Add the username.
            mapAttributes.addLast(ATTRIBUTE_USERNAME, principal.getName());

            // Add all roles for the primary user.
            DatawaveUser primaryUser = principal.getPrimaryUser();
            if (primaryUser != null) {
                mapAttributes.addAll(ATTRIBUTE_PRIMARY_USER_ROLES, primaryUser.getRoles());
            }

            // Add all roles for the proxied users. We must iterate through the proxied users in chronological order from original caller to last.
            Iterator<DatawaveUser> proxiedUsers = principal.getOrderedProxiedUsers().iterator();
            Set<String> proxyUserKeys = new HashSet<>();
            int proxyUserNum = 0;
            while (proxiedUsers.hasNext()) {
                DatawaveUser proxiedUser = proxiedUsers.next();

                // Add the proxy user's roles to a mapping with a key for the specific user.
                Collection<String> userRoles = proxiedUser.getRoles();
                String proxyUserKey = ATTRIBUTE_PROXY_USER_KEY_BASE + proxyUserNum;
                mapAttributes.addAll(proxyUserKey, userRoles);
                proxyUserKeys.add(proxyUserKey);
                proxyUserNum++;

                // If the last proxied user is a terminal server, add the terminal server roles.
                if (!proxiedUsers.hasNext()) {
                    if (proxiedUser.getUserType() == DatawaveUser.UserType.SERVER) {
                        mapAttributes.addAll(ATTRIBUTE_TERMINAL_SERVER_ROLES, userRoles);
                    }
                }
            }

            // Add all local roles associated with the principal name to the primary user's roles.
            Collection<String> localRoles = localUserRoles.get(principalName);
            if (!localRoles.isEmpty()) {
                if (log.isTraceEnabled()) {
                    log.trace("Added local roles {} for principal username {}", localRoles, principalName);
                }
                mapAttributes.addAll(ATTRIBUTE_PRIMARY_USER_ROLES, localRoles);
            }

            // Add a mapping for the proxy user keys.
            mapAttributes.addAll(ATTRIBUTE_PROXIED_USER_KEYS, proxyUserKeys);

            return mapAttributes.asReadOnly();
        }

        @Override
        public Principal getRealmIdentityPrincipal() {
            try {
                if (exists()) {
                    return new NamePrincipal(principalName);
                }
            } catch (Exception e) {
                log.error("Failed to obtain realm identity principal", e);
            }
            return null;
        }

        @Override
        public SupportLevel getCredentialAcquireSupport(Class<? extends Credential> credentialType, String algorithmName,
                        AlgorithmParameterSpec parameterSpec) {
            return SupportLevel.UNSUPPORTED;
        }

        @Override
        public <C extends Credential> C getCredential(Class<C> credentialType) {
            return null;
        }

        @Override
        public SupportLevel getEvidenceVerifySupport(Class<? extends Evidence> evidenceType, String algorithmName) {
            Preconditions.checkNotNull(evidenceType, "Parameter evidenceType may not be null");

            if (DatawaveEvidence.class.isAssignableFrom(evidenceType)) {
                return SupportLevel.POSSIBLY_SUPPORTED;
            } else {
                return SupportLevel.UNSUPPORTED;
            }
        }

        @Override
        public boolean verifyEvidence(Evidence evidence) {
            Preconditions.checkNotNull(evidence, "Parameter evidence may not be null");

            // Check if the evidence type is supported.
            if (!(evidence instanceof DatawaveEvidence)) {
                if (log.isTraceEnabled()) {
                    log.trace("Evidence {} is an unsupported type: {}", evidence, evidence.getClass().getName());
                }
                return false;
            }

            // If the evidence has a certificate, validate the certificate.
            if (evidence instanceof X509CertificateEvidence && !hasValidCertificate((X509CertificateEvidence) evidence)) {
                if (log.isTraceEnabled()) {
                    log.trace("Evidence certificate failed validation");
                }
                return false;
            }

            DatawaveEvidence datawaveEvidence = (DatawaveEvidence) evidence;
            try {
                // If the evidence username is different from the principal name, add any local roles associated with the evidence's username to the overall
                // identity's attributes.
                if (!datawaveEvidence.getUsername().equalsIgnoreCase(principalName)) {
                    Collection<String> localRoles = localUserRoles.get(datawaveEvidence.getUsername());
                    if (!localRoles.isEmpty()) {
                        MapAttributes updatedAttributes = new MapAttributes(this.baseAttributes);
                        updatedAttributes.addAll(AttributeConstants.ATTRIBUTE_PRIMARY_USER_ROLES, localRoles);
                        this.attributes = updatedAttributes.asReadOnly();
                        if (log.isTraceEnabled()) {
                            log.trace("Added local roles {} associated with evidence username {}", localRoles, datawaveEvidence.getUsername());
                        }
                    } else {
                        this.attributes = this.baseAttributes;
                    }
                } else {
                    this.attributes = this.baseAttributes;
                }
            } catch (Exception e) {
                log.error("Failed to load local user roles for evidence username {}", datawaveEvidence.getUsername(), e);
                throw new IllegalStateException("Failed to load local user roles using evidence username " + datawaveEvidence.getUsername(), e);
            }

            return true;
        }

        /**
         * Return whether the given evidence has a valid certificate
         *
         * @param evidence
         *            the evidence
         * @return true if no certificate validator has been configured or the evidence has a valid certificate, or false otherwise
         */
        private boolean hasValidCertificate(X509CertificateEvidence evidence) {
            // If the evidence is an X509CertificateEvidence, and a x509 evidence validator is configured, validate the evidence.
            if (x509EvidenceValidator != null) {
                try {
                    // If the evidence is not valid, nullify the attributes to negate the existence of the realm identity.
                    if (!x509EvidenceValidator.validate(evidence)) {
                        return false;
                    }
                } catch (Exception e) {
                    log.error("Error occurred while validating certificate evidence {}", evidence, e);
                    return false;
                }
            }
            return true;
        }

        @Override
        public AuthorizationIdentity getAuthorizationIdentity() {
            return AuthorizationIdentity.basicIdentity(attributes);
        }

        @Override
        public Attributes getAttributes() {
            return attributes;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DatawaveRealmIdentity that = (DatawaveRealmIdentity) o;
            return Objects.equals(principalName, that.principalName);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(principalName);
        }
    }

    /**
     * Configuration options for {@link DatawaveSecurityRealm}.
     */
    public static class Config {

        /**
         * The fully qualified name of the certificate verifier class to use to verify the user cert during SSL authentication. Must be a class that implements
         * 4 {@link datawave.security.cert.X509CertificateVerifier}.
         */
        public static final String OPTION_CERT_VERIFIER = "certVerifier";

        /**
         * The OSCP level to set in the certificate verifier instance if it is an instance of {@link DatawaveCertVerifier}.
         */
        public static final String OPTION_OSCP_LEVEL = "oscpLevel";

        /**
         * The maximum number of entries allowed for the realm identity cache. A negative value implies no limit.
         */
        public static final String OPTION_MAX_CACHE_ENTRIES = "maxCacheEntries";

        /**
         * The maximum age in milliseconds an entry in the cache may reach before it expires and is evicted from the cache. A negative value implies no limit.
         */
        public static final String OPTION_MAX_CACHE_AGE = "maxCacheAge";

        /**
         * The fully qualified path to a properties file that contains mappings of usernames to comma-delimited local roles that should be added to the role set
         * of matching users. The usernames must either match the username returned by {@link DatawaveEvidence#getUsername()} or
         * {@link DatawavePrincipal#getName()}. Matching is case-insensitive.
         */
        public static final String OPTION_ROLE_PROPERTIES = "roleProperties";

        public static Config fromMap(Map<String,String> config) {
            String certVerifierClass = ConfigUtils.getString(config.get(OPTION_CERT_VERIFIER), null);
            String oscpLevel = ConfigUtils.getString(config.get(OPTION_OSCP_LEVEL), null);
            long maxCacheEntries = ConfigUtils.getLong(config.get(OPTION_MAX_CACHE_ENTRIES), -1L);
            long maxCacheAge = ConfigUtils.getLong(config.get(OPTION_MAX_CACHE_AGE), -1L);
            String rolePropertiesPath = ConfigUtils.getString(config.get(OPTION_ROLE_PROPERTIES), null);

            return new Config(certVerifierClass, oscpLevel, maxCacheEntries, maxCacheAge, rolePropertiesPath);
        }

        private final String certVerifierClass;
        private final String oscpLevel;
        private final long maxCacheEntries;
        private final long maxCacheAge;
        private final String rolePropertiesPath;

        public Config(String certVerifierClass, String oscpLevel, long maxCacheEntries, long maxCacheAge, String roleProperties) {
            this.certVerifierClass = certVerifierClass;
            this.oscpLevel = oscpLevel;
            this.maxCacheEntries = maxCacheEntries;
            this.maxCacheAge = maxCacheAge;
            this.rolePropertiesPath = roleProperties;
        }

        public String getCertVerifierClass() {
            return certVerifierClass;
        }

        public String getOscpLevel() {
            return oscpLevel;
        }

        public long getMaxCacheEntries() {
            return maxCacheEntries;
        }

        public long getMaxCacheAge() {
            return maxCacheAge;
        }

        public String getRolePropertiesPath() {
            return rolePropertiesPath;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", Config.class.getSimpleName() + "[", "]").add("certVerifierClass='" + certVerifierClass + "'")
                            .add("oscpLevel='" + oscpLevel + "'").add("maxCacheEntries=" + maxCacheEntries).add("maxCacheAge=" + maxCacheAge)
                            .add("rolePropertiesPath=" + rolePropertiesPath).toString();
        }
    }

}
