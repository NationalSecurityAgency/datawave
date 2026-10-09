package datawave.security.realm;

import java.security.Principal;
import java.util.Collection;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wildfly.security.auth.server.EvidenceDecoder;
import org.wildfly.security.evidence.Evidence;

import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.cache.DatawaveUserCache;
import datawave.security.cache.ElytronCacheManager;
import datawave.security.evidence.DatawaveEvidence;
import datawave.security.evidence.JWTEvidence;
import datawave.security.evidence.TrustedHeaderEvidence;
import datawave.security.evidence.X509CertificateEvidence;
import datawave.security.system.SecurityEJBProvider;
import datawave.security.utils.ConfigUtils;

/**
 * A {@link EvidenceDecoder} that will decode instances of {@link DatawaveEvidence} to a corresponding {@link DatawavePrincipal}. This evidence decoder has
 * support for caching, and can be configured to enable/disable support for JWT authentication and trusted header authentication.
 */
public class DatawaveEvidenceDecoder implements EvidenceDecoder {

    private static final Logger log = LoggerFactory.getLogger(DatawaveEvidenceDecoder.class);

    /**
     * The configuration for this {@link DatawaveEvidenceDecoder}.
     */
    private Config config = Config.fromMap(Map.of());

    /**
     * The provider used to obtain {@link DatawaveUser} instances from a remote service.
     */
    private DatawaveUserProvider datawaveUserProvider;

    /**
     * The provider used to obtain EJB instances.
     */
    private SecurityEJBProvider securityEJBProvider;

    /**
     * A cache of {@link DatawaveUser}.
     */
    private volatile DatawaveUserCache cache;

    /**
     * Whether initialization is complete.
     */
    private volatile boolean initializationComplete = false;

    /**
     * Initializes this role decoder with the given configuration options. This method is invoked once by the Wildfly Elytron subsystem when the decoder is
     * first created to provide configuration parameters. These parameters are typically defined in the Wildfly configuration files, such as jboss .cli files or
     * in the standalone.xml.
     *
     * @param configMap
     *            the configuration
     */
    @SuppressWarnings("unused")
    public void initialize(Map<String,String> configMap) {
        this.config = Config.fromMap(configMap);
        this.initializationComplete = false;
    }

    /**
     * Perform any initialization that could not be completed until EJBs are available for use.
     */
    private void completeInitialization() {
        // Checked without synchronizing so that the steady state stays lock free. Concurrent first requests fall through to the synchronized path, which
        // rechecks, so exactly one user cache is built and registered with the cache manager.
        if (!initializationComplete) {
            initializeOnce();
        }
    }

    /**
     * Perform the one time initialization guarded by this decoder's monitor.
     */
    private synchronized void initializeOnce() {
        if (!initializationComplete) {
            // Log the configuration.
            if (log.isDebugEnabled()) {
                log.debug("Initialized with config {}", config);
            }

            // Add the user cache to the elytron cache manager so that the CredentialsCacheBean can invoke operations on it as needed.
            this.cache = new DatawaveUserCache(this.config.getMaxCacheEntries(), this.config.getMaxCacheAge());
            ElytronCacheManager cacheManager = getSecurityEJBProvider().getElytronCacheManager();
            if (cacheManager != null) {
                log.debug("Adding user cache to the elytron cache manager");
                cacheManager.addCache(this.cache);
            } else {
                log.debug("No elytron cache manager configured.");
            }

            initializationComplete = true;
        }
    }

    /**
     * Return a {@link DatawavePrincipal} decoded from the given evidence.
     *
     * @param evidence
     *            the evidence to decode
     * @return a {@link DatawavePrincipal}, or null if one could not be decoded
     */
    @Override
    public Principal getPrincipal(Evidence evidence) {
        completeInitialization();

        if (evidence instanceof DatawaveEvidence) {
            // Check if JWT is enabled if this is a JWT evidence.
            if (!config.isJwtEnabled() && evidence instanceof JWTEvidence) {
                log.trace("JWT evidence provided, but JWT authentication is disabled");
                return null;
            }

            // Check if trusted headers is enabled if this is a trusted header evidence.
            if (!config.isTrustedHeadersEnabled() && evidence instanceof TrustedHeaderEvidence) {
                log.trace("Trusted header evidence provided, but trusted header authentication is disabled");
                return null;
            }

            try {
                // Check if we already have users cached for the evidence.
                Collection<DatawaveUser> users = getCachedUsers(evidence);

                // Otherwise, fetch users from the user provider for the evidence and cache them.
                if (users == null) {
                    log.trace("No users found in cache, fetching users from user provider");
                    DatawaveUserProvider userProvider = getDatawaveUserProvider();
                    users = userProvider.getUsers(evidence);
                    cacheUsers(evidence, users);
                } else {
                    log.trace("Users loaded from cache");
                }

                DatawavePrincipal principal = new DatawavePrincipal(users);
                if (log.isTraceEnabled()) {
                    log.trace("Decoded evidence {} into principal {}", evidence, principal);
                }
                return principal;
            } catch (Exception e) {
                log.error("Failed to decode principal", e);
                throw new RuntimeException(e);
            }
        } else {
            if (log.isWarnEnabled()) {
                log.warn("Unable to decode evidence {} which is not a {}", evidence, DatawaveEvidence.class.getName());
            }
        }

        return null;
    }

    /**
     * Get the users (if any) cached for the given evidence.
     *
     * @param evidence
     *            the evidence
     * @return the cached users, or null if there are none cached
     */
    private Collection<DatawaveUser> getCachedUsers(Evidence evidence) {
        String cacheKey = getCacheKey(evidence);
        return cacheKey != null ? cache.get(cacheKey) : null;
    }

    /**
     * Cache users for the given evidence.
     *
     * @param evidence
     *            the evidence
     * @param users
     *            the users
     */
    private void cacheUsers(Evidence evidence, Collection<DatawaveUser> users) {
        String cacheKey = getCacheKey(evidence);
        if (cacheKey != null) {
            cache.put(cacheKey, users);
        }
    }

    /**
     * Return a cache key representing the given evidence. The following will be returned based on the evidence type:
     * <ul>
     * <li>{@link JWTEvidence}: returns null. We do not cache users for JWT tokens. Users should always be resolved directly from the token to ensure we check
     * if the token has expired.</li>
     * <li>{@link TrustedHeaderEvidence}: returns the entities as a String.</li>
     * <li>{@link X509CertificateEvidence}: returns the entities as a String.</li>
     * </ul>
     *
     * @param evidence
     *            the evidence
     * @return the cache key
     */
    private String getCacheKey(Evidence evidence) {
        if (evidence instanceof JWTEvidence) {
            return null;
        } else if (evidence instanceof TrustedHeaderEvidence) {
            return getEntitiesAsString(((TrustedHeaderEvidence) evidence).getEntities());
        } else if (evidence instanceof X509CertificateEvidence) {
            return getEntitiesAsString(((X509CertificateEvidence) evidence).getEntities());
        }
        return null;
    }

    /**
     * Return the given list of {@link SubjectIssuerDNPair} as a string.
     *
     * @param entities
     *            the entities
     * @return the entities as a string
     */
    private String getEntitiesAsString(Collection<SubjectIssuerDNPair> entities) {
        return entities.stream().map(SubjectIssuerDNPair::toString).collect(Collectors.joining());
    }

    /**
     * Return the {@link DatawaveUserProvider} set for this {@link DatawaveEvidenceDecoder}. If no user provider has been set, the user provider will be set to
     * the result of {@link DatawaveUserProvider#getInstance()} and returned.
     *
     * @return the user provider
     * @throws Exception
     *             if an error occurs while fetching the default instance of {@link DatawaveUserProvider}
     */
    DatawaveUserProvider getDatawaveUserProvider() throws Exception {
        if (this.datawaveUserProvider == null) {
            this.datawaveUserProvider = DatawaveUserProvider.getInstance();
        }
        return this.datawaveUserProvider;
    }

    /**
     * Exposed to allow a {@link DatawaveUserProvider} to be set for testing purposes. In production, the {@link DatawaveUserProvider} returned by
     * {@link DatawaveUserProvider#getInstance()} should be used.
     *
     * @param datawaveUserProvider
     *            the user provider
     */
    void setDatawaveUserProvider(DatawaveUserProvider datawaveUserProvider) {
        this.datawaveUserProvider = datawaveUserProvider;
    }

    /**
     * Return the {@link SecurityEJBProvider} set for this {@link DatawaveEvidenceDecoder}. If no provider has been set, the provider will be set to the result
     * of {@link SecurityEJBUtils#getSecurityEJBProvider()} and returned.
     *
     * @return the provider
     */
    private SecurityEJBProvider getSecurityEJBProvider() {
        if (this.securityEJBProvider == null) {
            this.securityEJBProvider = SecurityEJBUtils.getSecurityEJBProvider();
        }
        return this.securityEJBProvider;
    }

    /**
     * Exposed to allow a {@link SecurityEJBProvider} to be set for testing purposes. In production, the {@link SecurityEJBProvider} returned by
     * {@link SecurityEJBUtils#getSecurityEJBProvider()} should be used.
     *
     * @param securityEJBProvider
     *            the EJB provider
     */
    public void setSecurityEJBProvider(SecurityEJBProvider securityEJBProvider) {
        this.securityEJBProvider = securityEJBProvider;
    }

    public static class Config {

        /**
         * Whether JWT authentication is enabled.
         */
        public static final String OPTION_JWT_ENABLED = "jwtEnabled";

        /**
         * Whether trusted header authentication is enabled.
         */
        public static final String OPTION_TRUSTED_HEADERS_ENABLED = "trustedHeadersEnabled";

        /**
         * The maximum size allowed for the user cache. A negative value implies no limit.
         */
        public static final String OPTION_MAX_CACHE_ENTRIES = "maxCacheEntries";

        /**
         * The maximum age (TTL) in milliseconds for entries in the user cache. A negative value implies no limit.
         */
        public static final String OPTION_MAX_CACHE_AGE = "maxCacheAge";

        public static Config fromMap(Map<String,String> config) {
            boolean jwtEnabled = ConfigUtils.getBoolean(config.get(OPTION_JWT_ENABLED), false);
            boolean trustedHeadersEnabled = ConfigUtils.getBoolean(config.get(OPTION_TRUSTED_HEADERS_ENABLED), false);
            long maxCacheEntries = ConfigUtils.getLong(config.get(OPTION_MAX_CACHE_ENTRIES), -1L);
            long maxCacheAge = ConfigUtils.getLong(config.get(OPTION_MAX_CACHE_AGE), -1L);

            return new Config(jwtEnabled, trustedHeadersEnabled, maxCacheEntries, maxCacheAge);
        }

        private final boolean jwtEnabled;
        private final boolean trustedHeadersEnabled;
        private final long maxCacheEntries;
        private final long maxCacheAge;

        public Config(boolean jwtEnabled, boolean trustedHeadersEnabled, long maxCacheEntries, long maxCacheAge) {
            this.jwtEnabled = jwtEnabled;
            this.trustedHeadersEnabled = trustedHeadersEnabled;
            this.maxCacheEntries = maxCacheEntries;
            this.maxCacheAge = maxCacheAge;
        }

        public boolean isJwtEnabled() {
            return jwtEnabled;
        }

        public boolean isTrustedHeadersEnabled() {
            return trustedHeadersEnabled;
        }

        public long getMaxCacheEntries() {
            return maxCacheEntries;
        }

        public long getMaxCacheAge() {
            return maxCacheAge;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", Config.class.getSimpleName() + "[", "]").add("jwtEnabled=" + jwtEnabled)
                            .add("trustedHeadersEnabled=" + trustedHeadersEnabled).add("maxCacheEntries=" + maxCacheEntries).add("maxCacheAge=" + maxCacheAge)
                            .toString();
        }
    }

}
