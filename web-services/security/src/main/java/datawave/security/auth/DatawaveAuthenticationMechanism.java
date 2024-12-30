package datawave.security.auth;

import static datawave.webservice.metrics.Constants.REQUEST_LOGIN_TIME_HEADER;
import static datawave.webservice.metrics.Constants.REQUEST_START_TIME_HEADER;

import java.io.IOException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.security.auth.login.AccountLockedException;
import javax.security.auth.login.CredentialException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.jboss.security.SecurityContextAssociation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.SslClientAuthMode;

import datawave.security.util.ProxiedEntityUtils;
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

/**
 * The custom DATAWAVE servlet authentication mechanism. This auth mechanism acts just like CLIENT-CERT if there is an SSL session found, and otherwise uses
 * trusted headers.
 */
public class DatawaveAuthenticationMechanism implements AuthenticationMechanism {
    public static final String PROXIED_ENTITIES_HEADER = "X-ProxiedEntitiesChain";
    public static final String PROXIED_ISSUERS_HEADER = "X-ProxiedIssuersChain";
    public static final String MECHANISM_NAME = "DATAWAVE-AUTH";
    private static final HttpString HEADER_START_TIME = new HttpString(REQUEST_START_TIME_HEADER);
    private static final HttpString HEADER_LOGIN_TIME = new HttpString(REQUEST_LOGIN_TIME_HEADER);
    protected static final HttpString HEADER_PROXIED_ENTITIES = new HttpString(PROXIED_ENTITIES_HEADER);
    protected static final HttpString HEADER_PROXIED_ENTITIES_ACCEPTED = new HttpString("X-ProxiedEntitiesAccepted");

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final String name;
    /**
     * If we should force a renegotiation if client certs were not supplied. <code>true</code> by default
     */
    private final boolean forceRenegotiation;
    private final IdentityManager identityManager;
    protected final String SUBJECT_DN_HEADER;
    protected final String ISSUER_DN_HEADER;
    private final boolean trustedHeaderAuthentication;
    private final boolean jwtHeaderAuthentication;
    private final Set<String> dnsToPrune;
    private final Map<Class<? extends LoginException>,Integer> returnCodeMap = new HashMap();

    @SuppressWarnings("UnusedDeclaration")
    public DatawaveAuthenticationMechanism() {
        this(MECHANISM_NAME, true, null);
    }

    @SuppressWarnings("UnusedDeclaration")
    public DatawaveAuthenticationMechanism(String mechanismName) {
        this(mechanismName, true, null);
    }

    @SuppressWarnings("UnusedDeclaration")
    public DatawaveAuthenticationMechanism(boolean forceRenegotiation) {
        this(MECHANISM_NAME, forceRenegotiation, null);
    }

    public DatawaveAuthenticationMechanism(String mechanismName, boolean forceRenegotiation, IdentityManager identityManager) {
        this.name = mechanismName;
        this.forceRenegotiation = forceRenegotiation;
        this.identityManager = identityManager;
        trustedHeaderAuthentication = Boolean.valueOf(System.getProperty("dw.trusted.header.authentication", "false"));
        jwtHeaderAuthentication = Boolean.valueOf(System.getProperty("dw.jwt.header.authentication", "false"));
        String dns = System.getProperty("dw.trusted.proxied.entities", null);
        if (!StringUtils.isEmpty(dns)) {
            dnsToPrune = new HashSet<>(Arrays.asList(ProxiedEntityUtils.splitProxiedDNs(dns, false)));
        } else {
            dnsToPrune = null;
        }
        SUBJECT_DN_HEADER = System.getProperty("dw.trusted.header.subjectDn", "X-SSL-ClientCert-Subject".toLowerCase());
        ISSUER_DN_HEADER = System.getProperty("dw.trusted.header.issuerDn", "X-SSL-ClientCert-Issuer".toLowerCase());
        // These LoginExceptions are thrown from DatawavePrincipalLoginModule and
        // caught and saved in the SecurityContext in JBossCachedAuthenticationManager.

        // there was some problem with the credential that prevented evaluation
        returnCodeMap.put(CredentialException.class, HttpStatus.SC_UNAUTHORIZED);
        // credential was evaluated and rejected
        returnCodeMap.put(AccountLockedException.class, HttpStatus.SC_FORBIDDEN);
        returnCodeMap.put(FailedLoginException.class, HttpStatus.SC_FORBIDDEN);
        // there was a system error that prevented evaluation of the credential
        returnCodeMap.put(LoginException.class, HttpStatus.SC_SERVICE_UNAVAILABLE);
    }

    @Override
    public AuthenticationMechanismOutcome authenticate(HttpServerExchange exchange, SecurityContext securityContext) {
        // Pull proxied entity info from the headers. If proxied entities are there, but proxied issuers are missing, then fail authentication immediately.
        String proxiedEntities;
        String proxiedIssuers;
        try {
            proxiedEntities = getSingleHeader(exchange.getRequestHeaders(), PROXIED_ENTITIES_HEADER);
            proxiedIssuers = getSingleHeader(exchange.getRequestHeaders(), PROXIED_ISSUERS_HEADER);
            logger.trace("Authenticating with proxiedEntities={} and proxiedIssuers={}", proxiedEntities, proxiedIssuers);
            if (proxiedEntities != null && proxiedIssuers == null) {
                return notAuthenticated(exchange, securityContext,
                                PROXIED_ENTITIES_HEADER + " supplied, but missing " + PROXIED_ISSUERS_HEADER + " is missing!");
            }
        } catch (MultipleHeaderException e) {
            return notAuthenticated(exchange, securityContext, e.getMessage());
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
        // If we're using JWT authentication, then trust the JWT in the header.
        if (jwtHeaderAuthentication) {
            try {
                String authorizationHeader = getSingleHeader(exchange.getRequestHeaders(), "Authorization");
                if (authorizationHeader != null && authorizationHeader.startsWith("Bearer ")) {
                    credential = new DatawaveCredential(authorizationHeader.substring(7));
                }
            } catch (MultipleHeaderException e) {
                return notAuthenticated(exchange, securityContext, e.getMessage());
            }
        }
        // If we're either not using SSL and/or didn't get user info from the SSL session, then get it from the trusted headers, if we're configured to do so.
        if (credential == null && trustedHeaderAuthentication) {
            try {
                String subjectDN = getSingleHeader(exchange.getRequestHeaders(), SUBJECT_DN_HEADER);
                String issuerDN = getSingleHeader(exchange.getRequestHeaders(), ISSUER_DN_HEADER);
                logger.trace("Authenticating with trusted subject header={} and trusted issuer header={}", subjectDN, issuerDN);
                // If no DN headers supplied, then report that we did not authenticate
                if (subjectDN == null && issuerDN == null) {
                    return notAttempted(exchange);
                }
                // If one of subject or issuer is missing, then report authentication failure.
                if (subjectDN == null || issuerDN == null) {
                    return notAuthenticated(exchange, securityContext,
                                    "Missing trusted subject DN (" + subjectDN + ") or issuer DN (" + issuerDN + ") for trusted header authentication.");
                }
                credential = new DatawaveCredential(subjectDN, issuerDN, proxiedEntities, proxiedIssuers);
            } catch (MultipleHeaderException e) {
                return notAuthenticated(exchange, securityContext, e.getMessage());
            }
        }

        logger.trace("Computed credential = {}", credential);
        if (credential != null) {
            if (dnsToPrune != null) {
                credential.pruneEntities(dnsToPrune);
                logger.trace("Computed credential after pruning = {}", credential);
            }
            String username = credential.getUserName();

            IdentityManager idm = getIdentityManager(securityContext);
            Account account = idm.verify(username, credential);
            if (account != null) {
                return authenticated(exchange, securityContext, account);
            }
        }

        return notAttempted(exchange);
    }

    private AuthenticationMechanismOutcome notAttempted(HttpServerExchange exchange) {
        addTimingRequestHeaders(exchange);
        return AuthenticationMechanismOutcome.NOT_ATTEMPTED;
    }

    private AuthenticationMechanismOutcome notAuthenticated(HttpServerExchange exchange, SecurityContext securityContext, String reason) {
        securityContext.authenticationFailed(reason, name);
        addTimingRequestHeaders(exchange);
        return AuthenticationMechanismOutcome.NOT_AUTHENTICATED;
    }

    private AuthenticationMechanismOutcome authenticated(HttpServerExchange exchange, SecurityContext securityContext, Account account) {
        if (exchange.getRequestHeaders().contains(HEADER_PROXIED_ENTITIES)) {
            exchange.getResponseHeaders().add(HEADER_PROXIED_ENTITIES_ACCEPTED, "true");
        }

        securityContext.authenticationComplete(account, name, false);
        addTimingRequestHeaders(exchange);
        return AuthenticationMechanismOutcome.AUTHENTICATED;
    }

    private void addTimingRequestHeaders(HttpServerExchange exchange) {
        long requestStartTime = exchange.getRequestStartTime();
        long loginTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - requestStartTime);
        HeaderMap headers = exchange.getRequestHeaders();
        headers.add(HEADER_START_TIME, requestStartTime);
        headers.add(HEADER_LOGIN_TIME, loginTime);
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
        // FORBIDDEN (403) was the previous default response code returned when an exception happened
        // in the DatawavePrincipalLoginModule and this method returned ChallengeResult(false)
        int returnCode = HttpStatus.SC_FORBIDDEN;
        org.jboss.security.SecurityContext sc = SecurityContextAssociation.getSecurityContext();
        if (sc != null) {
            // A LoginException is thrown from DatawavePrincipalLoginModule and caught
            // and saved in the SecurityContext in JBossCachedAuthenticationManager.
            Exception e = (Exception) sc.getData().get("org.jboss.security.exception");
            if (e != null) {
                if (returnCodeMap.containsKey(e.getClass())) {
                    returnCode = returnCodeMap.get(e.getClass());
                }
                if (logger.isTraceEnabled()) {
                    logger.trace("exception class: {} returnCode: {}", e.getClass().getCanonicalName(), returnCode);
                }
            }
        }
        // The ChallengeResult is evaluated in SecurityContextImpl.transition()
        return new ChallengeResult(true, returnCode);
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
        @Deprecated
        public AuthenticationMechanism create(String mechanismName, FormParserFactory formParserFactory, Map<String,String> properties) {
            return create(mechanismName, identityManager, formParserFactory, properties);
        }

        @Override
        public AuthenticationMechanism create(String mechanismName, IdentityManager identityManager, FormParserFactory formParserFactory,
                        Map<String,String> properties) {
            String forceRenegotiation = properties.get(ClientCertAuthenticationMechanism.FORCE_RENEGOTIATION);
            return new DatawaveAuthenticationMechanism(mechanismName, (forceRenegotiation == null) || "true".equals(forceRenegotiation), identityManager);
        }
    }
}
