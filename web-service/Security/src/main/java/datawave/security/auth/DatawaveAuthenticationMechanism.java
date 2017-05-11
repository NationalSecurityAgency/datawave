package datawave.security.auth;

import static datawave.webservice.metrics.Constants.REQUEST_LOGIN_TIME_HEADER;
import static datawave.webservice.metrics.Constants.REQUEST_START_TIME_HEADER;

import java.io.IOException;
import java.lang.reflect.Field;
import java.security.Principal;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLPeerUnverifiedException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.undertow.security.api.AuthenticationMechanism;
import io.undertow.security.api.AuthenticationMechanismFactory;
import io.undertow.security.api.SecurityContext;
import io.undertow.security.idm.Account;
import io.undertow.security.idm.IdentityManager;
import io.undertow.security.impl.ClientCertAuthenticationMechanism;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.RenegotiationRequiredException;
import io.undertow.server.SSLSessionInfo;
import io.undertow.server.handlers.form.FormParserFactory;
import io.undertow.util.HeaderMap;
import io.undertow.util.HeaderValues;
import io.undertow.util.HttpString;
import org.jboss.as.security.plugins.SecurityDomainContext;
import org.jboss.security.AuthenticationManager;
import org.jboss.security.authentication.JBossCachedAuthenticationManager;
import org.jboss.security.authentication.JBossCachedAuthenticationManager.DomainInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.SslClientAuthMode;

/**
 * The custom DATAWAVE servlet authentication mechanism. This auth mechanism acts just like CLIENT-CERT if there is an SSL session found, and otherwise uses
 * trusted headers.
 */
public class DatawaveAuthenticationMechanism implements AuthenticationMechanism {
    public static final String PROXIED_ENTITIES_HEADER = "X-ProxiedEntitiesChain";
    public static final String PROXIED_ISSUERS_HEADER = "X-ProxiedIssuersChain";
    public static final String MECHANISM_NAME = "DATAWAVE-AUTH";
    private static final long CACHE_TIMEOUT_SECONDS = 300L;
    private static final HttpString HEADER_START_TIME = new HttpString(REQUEST_START_TIME_HEADER);
    private static final HttpString HEADER_LOGIN_TIME = new HttpString(REQUEST_LOGIN_TIME_HEADER);
    
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    private final String name;
    /**
     * If we should force a renegotiation if client certs were not supplied. <code>true</code> by default
     */
    private final boolean forceRenegotiation;
    private final long cacheTimeoutSeconds;
    private final IdentityManager identityManager;
    protected final String SUBJECT_DN_HEADER;
    protected final String ISSUER_DN_HEADER;
    private final boolean trustedHeaderAuthentication;
    private boolean checkedAuthenticationCache;
    
    private static Map<AuthenticationManager,ConcurrentMap<Principal,DomainInfo>> caches = new HashMap<>();
    
    @SuppressWarnings("UnusedDeclaration")
    public DatawaveAuthenticationMechanism() {
        this(MECHANISM_NAME, true, CACHE_TIMEOUT_SECONDS, null);
    }
    
    @SuppressWarnings("UnusedDeclaration")
    public DatawaveAuthenticationMechanism(String mechanismName) {
        this(mechanismName, true, CACHE_TIMEOUT_SECONDS, null);
    }
    
    @SuppressWarnings("UnusedDeclaration")
    public DatawaveAuthenticationMechanism(String mechanismName, long cacheTimeoutSeconds) {
        this(mechanismName, true, cacheTimeoutSeconds, null);
    }
    
    @SuppressWarnings("UnusedDeclaration")
    public DatawaveAuthenticationMechanism(boolean forceRenegotiation) {
        this(MECHANISM_NAME, forceRenegotiation, CACHE_TIMEOUT_SECONDS, null);
    }
    
    @SuppressWarnings("UnusedDeclaration")
    public DatawaveAuthenticationMechanism(boolean forceRenegotiation, long cacheTimoutSeconds) {
        this(MECHANISM_NAME, forceRenegotiation, cacheTimoutSeconds, null);
    }
    
    public DatawaveAuthenticationMechanism(String mechanismName, boolean forceRenegotiation, long cacheTimeoutSeconds, IdentityManager identityManager) {
        this.name = mechanismName;
        this.forceRenegotiation = forceRenegotiation;
        this.cacheTimeoutSeconds = cacheTimeoutSeconds;
        this.identityManager = identityManager;
        trustedHeaderAuthentication = Boolean.valueOf(System.getProperty("dw.trusted.header.authentication", "false"));
        SUBJECT_DN_HEADER = System.getProperty("dw.trusted.header.subjectDn", "X-SSL-ClientCert-Subject".toLowerCase());
        ISSUER_DN_HEADER = System.getProperty("dw.trusted.header.issuerDn", "X-SSL-ClientCert-Issuer".toLowerCase());
    }
    
    @Override
    public AuthenticationMechanismOutcome authenticate(HttpServerExchange exchange, SecurityContext securityContext) {
        AuthenticationMechanismOutcome outcome = AuthenticationMechanismOutcome.NOT_ATTEMPTED;
        
        // Pull proxied entity info from the headers. If proxied entities are there, but proxied issuers are missing, then fail authentication immediately.
        String proxiedEntities;
        String proxiedIssuers;
        try {
            proxiedEntities = getSingleHeader(exchange.getRequestHeaders(), PROXIED_ENTITIES_HEADER);
            proxiedIssuers = getSingleHeader(exchange.getRequestHeaders(), PROXIED_ISSUERS_HEADER);
            if (proxiedEntities != null && proxiedIssuers == null) {
                securityContext.authenticationFailed(PROXIED_ENTITIES_HEADER + " supplied, but missing " + PROXIED_ISSUERS_HEADER + " is missing!", name);
                return AuthenticationMechanismOutcome.NOT_AUTHENTICATED;
            }
        } catch (MultipleHeaderException e) {
            securityContext.authenticationFailed(e.getMessage(), name);
            return AuthenticationMechanismOutcome.NOT_AUTHENTICATED;
        }
        
        // Pull subject/issuer DN from the SSL client certificate if it's there.
        DatawaveCredential credential = null;
        SSLSessionInfo sslSession = exchange.getConnection().getSslSessionInfo();
        if (sslSession != null) {
            try {
                Certificate[] clientCerts = getPeerCertificates(exchange, sslSession, securityContext);
                if (clientCerts[0] instanceof X509Certificate) {
                    X509Certificate certificate = (X509Certificate) clientCerts[0];
                    credential = new DatawaveCredential(certificate, proxiedEntities, proxiedIssuers);
                }
            } catch (SSLPeerUnverifiedException e) {
                // No action - this mechanism can not attempt authentication without peer certificates, so allow it to drop out to NOT_ATTEMPTED
            }
        }
        // We're not using SSL, so get the user info from trusted headers, if we're configured to do so.
        else if (trustedHeaderAuthentication) {
            try {
                String subjectDN = getSingleHeader(exchange.getRequestHeaders(), SUBJECT_DN_HEADER);
                String issuerDN = getSingleHeader(exchange.getRequestHeaders(), ISSUER_DN_HEADER);
                // If no DN headers supplied, then report that we did not authenticate
                if (subjectDN == null && issuerDN == null) {
                    return AuthenticationMechanismOutcome.NOT_ATTEMPTED;
                }
                // If one of subject or issuer is missing, then report authentication failure.
                if (subjectDN == null || issuerDN == null) {
                    securityContext.authenticationFailed("Missing trusted subject DN (" + subjectDN + ") or issuer DN (" + issuerDN
                                    + ") for trusted header authentication.", name);
                    return AuthenticationMechanismOutcome.NOT_AUTHENTICATED;
                }
                credential = new DatawaveCredential(subjectDN, issuerDN, proxiedEntities, proxiedIssuers);
            } catch (MultipleHeaderException e) {
                securityContext.authenticationFailed(e.getMessage(), name);
                return AuthenticationMechanismOutcome.NOT_AUTHENTICATED;
            }
        }
        
        if (credential != null) {
            String username = credential.getUserName();
            
            IdentityManager idm = getIdentityManager(securityContext);
            replaceAuthenticationManagerCacheIfNecessary(idm);
            Account account = idm.verify(username, credential);
            if (account != null) {
                securityContext.authenticationComplete(account, name, false);
                outcome = AuthenticationMechanismOutcome.AUTHENTICATED;
            }
        }
        
        long requestStartTime = exchange.getRequestStartTime();
        long loginTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - requestStartTime);
        HeaderMap headers = exchange.getRequestHeaders();
        headers.add(HEADER_START_TIME, requestStartTime);
        headers.add(HEADER_LOGIN_TIME, loginTime);
        
        return outcome;
    }
    
    /**
     * Replace the domain cache on the {@link JBossCachedAuthenticationManager} associated with the security context on {@code idm}. This method is fragile and
     * unsafe. It will not work if a security manager is in use, it is specific to Wildfly, and may not work with Wildfly updates. It is here to work around a
     * bug in Wildfly (<a href="https://issues.jboss.org/browse/WFLY-3858">WFLY-3858</a>) which prevents configuration of the infinispan caching for security
     * domains. Without configuration of the caching, the authentication manager will cache entries potentially forever. This means we'll never re-try our back
     * end authentication which may be necessary to check since user roles could change.
     */
    protected void replaceAuthenticationManagerCacheIfNecessary(IdentityManager idm) {
        if (checkedAuthenticationCache)
            return;
        
        try {
            logger.trace("Checking to see if we need to replace the authentication manager cache in {}", this);
            Field field = idm.getClass().getDeclaredField("securityDomainContext");
            field.setAccessible(true);
            SecurityDomainContext sdc = (SecurityDomainContext) field.get(idm);
            logger.trace("Retrieved SecurityDomainContext from {}", idm);
            JBossCachedAuthenticationManager am = (JBossCachedAuthenticationManager) sdc.getAuthenticationManager();
            logger.trace("Retrieved authentication manager {} from {}", am, sdc);
            field = am.getClass().getDeclaredField("domainCache");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            ConcurrentMap<Principal,DomainInfo> cache = (ConcurrentMap<Principal,DomainInfo>) field.get(am);
            logger.trace("Retrieved cache {} from authentication manager {}.", cache, am);
            
            synchronized (DatawaveAuthenticationMechanism.class) {
                if (cacheTimeoutSeconds > 0) {
                    ConcurrentMap<Principal,DomainInfo> replacementCache = caches.get(am);
                    if (replacementCache == null) {
                        Cache<Principal,DomainInfo> authenticationCache = CacheBuilder.newBuilder().expireAfterWrite(cacheTimeoutSeconds, TimeUnit.SECONDS)
                                        .maximumSize(3000).build();
                        replacementCache = authenticationCache.asMap();
                        caches.put(am, replacementCache);
                    }
                    if (cache != replacementCache) {
                        field.set(am, replacementCache);
                    }
                }
            }
            
        } catch (NoSuchFieldException e) {
            logger.warn("Unable to locate field {}. Will fall back to default authentication manager behavior.", e.getMessage(), e);
        } catch (IllegalAccessException e) {
            logger.warn("Unable to replace domain cache in authentication module. Will fall back to default authentication manager behavior.", e);
        } catch (ClassCastException e) {
            logger.warn("Unable to cast AuenticationManager to a JBossCachedAuthentication manager. Will fall back to default authentication manager behavior.",
                            e);
        }
        checkedAuthenticationCache = true;
    }
    
    // impl copied from io.undertow.security.impl.ClientCertAuthenticationMechanism
    private Certificate[] getPeerCertificates(HttpServerExchange exchange, SSLSessionInfo sslSession, SecurityContext securityContext)
                    throws SSLPeerUnverifiedException {
        try {
            return sslSession.getPeerCertificates();
        } catch (RenegotiationRequiredException e) {
            // we only renegotiate if authentication is required
            if (forceRenegotiation && securityContext.isAuthenticationRequired()) {
                try {
                    sslSession.renegotiate(exchange, SslClientAuthMode.REQUESTED);
                    return sslSession.getPeerCertificates();
                } catch (IOException | RenegotiationRequiredException e1) {
                    // ignore
                }
            }
        }
        throw new SSLPeerUnverifiedException("");
    }
    
    @Override
    public ChallengeResult sendChallenge(HttpServerExchange httpServerExchange, SecurityContext securityContext) {
        return new ChallengeResult(false);
    }
    
    private String getSingleHeader(HeaderMap headers, String headerName) throws MultipleHeaderException {
        String value = null;
        HeaderValues values = (headers == null) ? null : headers.get(headerName);
        if (values != null) {
            if (values.size() > 1)
                throw new MultipleHeaderException(headerName + " was specified multiple times, which is not allowed!");
            value = values.getFirst();
        }
        return value;
    }
    
    @SuppressWarnings("deprecation")
    private IdentityManager getIdentityManager(SecurityContext securityContext) {
        return identityManager != null ? identityManager : securityContext.getIdentityManager();
    }
    
    private static final class MultipleHeaderException extends Exception {
        public MultipleHeaderException(String message) {
            super(message);
        }
    }
    
    protected static final class Factory implements AuthenticationMechanismFactory {
        
        private final IdentityManager identityManager;
        
        public Factory(IdentityManager identityManager) {
            this.identityManager = identityManager;
        }
        
        @Override
        public AuthenticationMechanism create(String mechanismName, FormParserFactory formParserFactory, Map<String,String> properties) {
            String forceRenegotiation = properties.get(ClientCertAuthenticationMechanism.FORCE_RENEGOTIATION);
            long cacheTimeoutSeconds = 300;
            String timeoutOverride = properties.get("cache_timeout_seconds");
            if (timeoutOverride != null) {
                cacheTimeoutSeconds = Long.parseLong(timeoutOverride);
            }
            return new DatawaveAuthenticationMechanism(mechanismName, (forceRenegotiation == null) || "true".equals(forceRenegotiation), cacheTimeoutSeconds,
                            identityManager);
        }
    }
}
