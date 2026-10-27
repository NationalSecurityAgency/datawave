package datawave.security.auth;

import static java.util.Arrays.asList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLSession;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetSystemProperty;
import org.wildfly.security.auth.callback.AnonymousAuthorizationCallback;
import org.wildfly.security.auth.callback.AuthenticationCompleteCallback;
import org.wildfly.security.auth.callback.CachedIdentityAuthorizeCallback;
import org.wildfly.security.auth.callback.EvidenceVerifyCallback;
import org.wildfly.security.auth.callback.PrincipalAuthorizeCallback;
import org.wildfly.security.auth.server.SecurityDomain;
import org.wildfly.security.auth.server.SecurityIdentity;
import org.wildfly.security.authz.RoleDecoder;
import org.wildfly.security.cache.CachedIdentity;
import org.wildfly.security.evidence.Evidence;
import org.wildfly.security.http.HttpAuthenticationException;
import org.wildfly.security.http.HttpScope;
import org.wildfly.security.http.HttpServerRequest;
import org.wildfly.security.http.Scope;

import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.evidence.JWTEvidence;
import datawave.security.evidence.TrustedHeaderEvidence;
import datawave.security.evidence.X509CertificateEvidence;
import datawave.security.test.TestRealm;
import datawave.security.util.SecurityConstants;

/**
 * Unit tests for {@link DatawaveHttpAuthenticationMechanism}.
 */
@SetSystemProperty(key = SecurityConstants.TRUSTED_PROXIED_ENTITIES_SYSTEM_PROPERTY, value = "cn=trustedServer") // Configure the trusted proxied entities.
class DatawaveHttpAuthenticationMechanismTest {

    private final Map<String,Object> mechanismProperties = new HashMap<>();
    private final HttpServerRequestWrapper requestWrapper = new HttpServerRequestWrapper();
    private DatawaveHttpAuthenticationMechanism mechanism;
    private MockCallbackHandler callbackHandler;
    private HttpScopeHandler scopeWrapper;

    private static SecurityDomain securityDomain;

    @BeforeAll
    static void beforeAll() {
        // Create a security domain for creating test security identities.
        // @formatter:off
        securityDomain = SecurityDomain.builder()
                        .addRealm("testRealm", new TestRealm())
                            .setRoleDecoder(RoleDecoder.simple(TestRealm.ROLES_ATTRIBUTE))
                            .build()
                        .build();
        // @formatter:on
    }

    @BeforeEach
    void setUp() {
        this.callbackHandler = new MockCallbackHandler();
        this.mechanismProperties.clear();
        this.mechanism = null;
    }

    /**
     * Covers test scenarios where caching the identity in the session scope is disabled.
     */
    @DisplayName("When caching the identity in the HTTP session is disabled")
    @Nested
    class NonCachingTests {

        @BeforeEach
        void setUp() {
            // Disable session caching features.
            givenIdentityRestorationEnabled(false);
            givenSessionIdChangeEnabled(false);
        }

        /**
         * Verify that given a request with no information that can be turned into evidence for authorization, it fails authentication.
         */
        @DisplayName("A request should succeed as anonymous when evidence cannot be extracted from it")
        @Test
        void givenNoEvidence() throws HttpAuthenticationException {
            callbackHandler.actLikeEvidenceIsVerified();
            callbackHandler.actLikeFirstAuthorizationAttemptSucceeds();

            // Trigger the request.
            evaluateRequest();

            // Verify the request failed.
            requestWrapper.assertRequestSucceeded();

            // Verify two callbacks occurred, one for anonuymous caller, and one for authentication complete with a failed status.
            List<Callback> callbacks = callbackHandler.getCapturedCallbacks();
            assertThat(callbacks).hasSize(2);
            assertThat(callbacks.get(0)).isInstanceOf(AnonymousAuthorizationCallback.class);
            assertTrue(((AnonymousAuthorizationCallback) callbacks.get(0)).isAuthorized());
            assertThat(callbacks.get(1)).isInstanceOf(AuthenticationCompleteCallback.class);
            assertTrue(((AuthenticationCompleteCallback) callbacks.get(1)).succeeded());
        }

        /**
         * Verify that given a request with evidence that failed verification, it fails authentication.
         */
        @DisplayName("A request should fail when evidence verification fails")
        @Test
        void givenEvidenceVerificationFailed() throws HttpAuthenticationException {
            // Ensure JWT evidence is created.
            requestWrapper.setHeader("Authorization", "Bearer iamatoken");

            // Make the evidence fail verification.
            callbackHandler.actLikeEvidenceIsNotVerified();

            // Trigger the request.
            evaluateRequest();

            // Verify the request failed.
            requestWrapper.assertRequestFailed();

            // Verify two callbacks occurred, one for evidence verification, and one for authentication complete with a failed status.
            List<Callback> callbacks = callbackHandler.getCapturedCallbacks();
            assertThat(callbacks).hasSize(2);
            assertThat(callbacks.get(0)).isInstanceOf(EvidenceVerifyCallback.class);
            assertThat(callbacks.get(1)).isInstanceOf(AuthenticationCompleteCallback.class);
            assertTrue(((AuthenticationCompleteCallback) callbacks.get(1)).failed());
        }

        /**
         * Verify that given a request with no information that can be turned into evidence for authorization, it fails authentication.
         */
        @DisplayName("A request should fail when principal authorization fails")
        @Test
        void givenPrincipalAuthorizeFailed() throws HttpAuthenticationException {
            // Ensure JWT evidence is created.
            requestWrapper.setHeader("Authorization", "Bearer iamatoken");

            // Allow the evidence to be verified, but have it fail authorization (for instance, if we had a user with invalid roles).
            callbackHandler.actLikeEvidenceIsVerified();
            callbackHandler.actLikeFirstAuthorizationAttemptFails();

            // Trigger the request.
            evaluateRequest();

            // Verify the request failed.
            requestWrapper.assertRequestFailed();

            // Verify three callbacks occurred, one for evidence verification, one for authorization of a principal, and one for authentication complete with a
            // failed status.
            List<Callback> callbacks = callbackHandler.getCapturedCallbacks();
            assertThat(callbacks).hasSize(3);
            assertThat(callbacks.get(0)).isInstanceOf(EvidenceVerifyCallback.class);
            assertThat(callbacks.get(1)).isInstanceOf(PrincipalAuthorizeCallback.class);
            assertThat(callbacks.get(2)).isInstanceOf(AuthenticationCompleteCallback.class);
            assertTrue(((AuthenticationCompleteCallback) callbacks.get(2)).failed());
        }

        /**
         * Verify a scenario where JWT evidence is given, and authorizations succeeds.
         */
        @DisplayName("A request should succeed with valid JWT")
        @Test
        void givenJwtWithSuccess() throws HttpAuthenticationException {
            requestWrapper.setHeader("Authorization", "Bearer iamatoken");

            // Act like the evidence passes verification, and the final principal is authorized.
            callbackHandler.actLikeEvidenceIsVerified();
            callbackHandler.actLikeFirstAuthorizationAttemptSucceeds();

            // Trigger the request.
            evaluateRequest();

            // Verify the request succeeded.
            requestWrapper.assertRequestSucceeded();

            // Verify three callbacks occurred, one for evidence verification, one for authorization of a principal, and one for authentication complete with a
            // success status.
            List<Callback> callbacks = callbackHandler.getCapturedCallbacks();
            assertThat(callbacks).hasSize(3);
            assertThat(callbacks.get(0)).isInstanceOf(EvidenceVerifyCallback.class);
            assertThat(callbacks.get(1)).isInstanceOf(PrincipalAuthorizeCallback.class);
            assertThat(callbacks.get(2)).isInstanceOf(AuthenticationCompleteCallback.class);
            assertTrue(((AuthenticationCompleteCallback) callbacks.get(2)).succeeded());

            // Validate the evidence that was passed to the EvidenceVerifyCallback.
            EvidenceVerifyCallback evidenceVerifyCallback = (EvidenceVerifyCallback) callbacks.get(0);
            Evidence evidence = evidenceVerifyCallback.getEvidence();
            assertInstanceOf(JWTEvidence.class, evidence);
            assertEquals("iamatoken", ((JWTEvidence) evidence).getToken());
        }

        /**
         * Verify an exception is thrown when proxied entities are given without proxied issuers.
         */
        @DisplayName("A request should fail when proxied entities are given without issuers")
        @Test
        void givenProxiedEntitiesWithoutIssuers() {
            // Configure the trusted headers. Add a trusted entity to the proxy chain to test its removal.
            requestWrapper.setHeader(SecurityConstants.PROXIED_ENTITIES_HEADER, "cn=server1<cn=server2><cn=trustedServer>");

            // @formatter:off
            assertThatThrownBy(DatawaveHttpAuthenticationMechanismTest.this::evaluateRequest)
                            .isInstanceOf(HttpAuthenticationException.class)
                            .hasMessage("Error occurred when obtaining evidence for authentication")
                            .hasRootCauseInstanceOf(MissingHeaderException.class)
                            .hasRootCauseMessage("X-ProxiedEntitiesChain provided, but missing X-ProxiedIssuersChain");
            // @formatter:off
        }

        /**
         * Verify an exception is thrown when a trusted subject is given without a trusted issuer.
         */
        @DisplayName("A request should fail when a trusted subject is given without a trusted issuer")
        @Test
        void givenTrustedSubjectWithoutIssuer() {
            // Configure the trusted headers. Add a trusted entity to the proxy chain to test its removal.
            requestWrapper.setHeader(SecurityConstants.DEFAULT_TRUSTED_SUBJECT_DN_HEADER, "cn=testUser");
            requestWrapper.setHeader(SecurityConstants.PROXIED_ENTITIES_HEADER, "cn=server1<cn=server2><cn=trustedServer>");
            requestWrapper.setHeader(SecurityConstants.PROXIED_ISSUERS_HEADER, "cn=issuer1<cn=issuer2><cn=trustedIssuer>");

            // @formatter:off
            assertThatThrownBy(DatawaveHttpAuthenticationMechanismTest.this::evaluateRequest)
                            .isInstanceOf(HttpAuthenticationException.class)
                            .hasMessage("Error occurred when obtaining evidence for authentication")
                            .hasRootCauseInstanceOf(MissingHeaderException.class)
                            .hasRootCauseMessage("Missing trusted subject DN (cn=testUser) or issuer DN (null) for trusted header authentication");
            // @formatter:off
        }

        /**
         * Verify an exception is thrown when a trusted issuer is given without a trusted subject.
         */
        @DisplayName("A request should fail when a trusted issuer is given without a trusted subject")
        @Test
        void givenTrustedIssuerWithoutSubject() throws HttpAuthenticationException {
            // Configure the trusted headers. Add a trusted entity to the proxy chain to test its removal.
            requestWrapper.setHeader(SecurityConstants.DEFAULT_TRUSTED_ISSUER_DN_HEADER, "cn=testIssuer");
            requestWrapper.setHeader(SecurityConstants.PROXIED_ENTITIES_HEADER, "cn=server1<cn=server2><cn=trustedServer>");
            requestWrapper.setHeader(SecurityConstants.PROXIED_ISSUERS_HEADER, "cn=issuer1<cn=issuer2><cn=trustedIssuer>");

            // @formatter:off
            assertThatThrownBy(DatawaveHttpAuthenticationMechanismTest.this::evaluateRequest)
                    .isInstanceOf(HttpAuthenticationException.class)
                    .hasMessage("Error occurred when obtaining evidence for authentication")
                    .hasRootCauseInstanceOf(MissingHeaderException.class)
                    .hasRootCauseMessage("Missing trusted subject DN (null) or issuer DN (cn=testIssuer) for trusted header authentication");
            // @formatter:off
        }

        /**
         * Verify a scenario where trusted header evidence is given, and authorizations succeeds.
         */
        @DisplayName("A request should succeed with valid trusted headers")
        @Test
        void givenTrustedHeadersWithSuccess() throws HttpAuthenticationException {
            // Configure the trusted headers. Add a trusted entity to the proxy chain to test its removal.
            requestWrapper.setHeader(SecurityConstants.DEFAULT_TRUSTED_SUBJECT_DN_HEADER, "cn=testUser");
            requestWrapper.setHeader(SecurityConstants.DEFAULT_TRUSTED_ISSUER_DN_HEADER, "cn=testIssuer");
            requestWrapper.setHeader(SecurityConstants.PROXIED_ENTITIES_HEADER, "cn=server1<cn=server2><cn=trustedServer>");
            requestWrapper.setHeader(SecurityConstants.PROXIED_ISSUERS_HEADER, "cn=issuer1<cn=issuer2><cn=trustedIssuer>");

            // Act like the evidence passes verification, and the final principal is authorized.
            callbackHandler.actLikeEvidenceIsVerified();
            callbackHandler.actLikeFirstAuthorizationAttemptSucceeds();

            // Trigger the request.
            evaluateRequest();

            // Verify the request succeeded.
            requestWrapper.assertRequestSucceeded();

            // Verify three callbacks occurred, one for evidence verification, one for authorization of a principal, and one for authentication complete with a
            // success status.
            List<Callback> callbacks = callbackHandler.getCapturedCallbacks();
            assertThat(callbacks).hasSize(3);
            assertThat(callbacks.get(0)).isInstanceOf(EvidenceVerifyCallback.class);
            assertThat(callbacks.get(1)).isInstanceOf(PrincipalAuthorizeCallback.class);
            assertThat(callbacks.get(2)).isInstanceOf(AuthenticationCompleteCallback.class);
            assertTrue(((AuthenticationCompleteCallback) callbacks.get(2)).succeeded());

            // Validate the evidence that was passed to the EvidenceVerifyCallback.
            EvidenceVerifyCallback evidenceVerifyCallback = (EvidenceVerifyCallback)callbacks.get(0);
            Evidence evidence = evidenceVerifyCallback.getEvidence();
            assertInstanceOf(TrustedHeaderEvidence.class, evidence);
            TrustedHeaderEvidence trustedHeaderEvidence = (TrustedHeaderEvidence) evidence;
            assertEquals("cn=server1<cn=issuer1><cn=server2><cn=issuer2><cn=testuser><cn=testissuer>", trustedHeaderEvidence.getUsername());
            List<SubjectIssuerDNPair> entities = new ArrayList<>();
            entities.add(SubjectIssuerDNPair.of("cn=server1", "cn=issuer1"));
            entities.add(SubjectIssuerDNPair.of("cn=server2", "cn=issuer2"));
            entities.add(SubjectIssuerDNPair.of("cn=testuser", "cn=testissuer"));
            assertEquals(entities, trustedHeaderEvidence.getEntities());
        }

        /**
         * Verify a scenario where a certificate is given, and authorizations succeeds.
         */
        @DisplayName("A request should succeed with valid certs")
        @Test
        void givenCertificateWithSuccess() throws Exception {
            // Create an X509 certificate.
            Security.addProvider(new BouncyCastleProvider());

            KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA", "BC");
            generator.initialize(2048);
            KeyPair keyPair = generator.generateKeyPair();

            X500Name subjectDn = new X500Name("CN=certUser");
            X500Name issuerDn = new X500Name("CN=certIssuer");
            BigInteger serialNumber = BigInteger.valueOf(new SecureRandom().nextInt());
            Date validFrom = new Date();
            Date validTo = new Date(validFrom.getTime() + 365 * 25 * 60 * 60 * 1000L);

            X509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(issuerDn, serialNumber, validFrom, validTo, subjectDn, keyPair.getPublic());
            ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA").setProvider("BC").build(keyPair.getPrivate());
            X509Certificate certificate = new JcaX509CertificateConverter().setProvider("BC").getCertificate(builder.build(signer));

            // Configure the trusted headers. Add a trusted entity to the proxy chain to test its removal.
            requestWrapper.setCertificate(certificate);
            requestWrapper.setHeader(SecurityConstants.PROXIED_ENTITIES_HEADER, "cn=server1<cn=server2><cn=trustedServer>");
            requestWrapper.setHeader(SecurityConstants.PROXIED_ISSUERS_HEADER, "cn=issuer1<cn=issuer2><cn=trustedIssuer>");

            // Act like the evidence passes verification, and the final principal is authorized.
            callbackHandler.actLikeEvidenceIsVerified();
            callbackHandler.actLikeFirstAuthorizationAttemptSucceeds();

            // Trigger the request.
            evaluateRequest();

            // Verify the request succeeded.
            requestWrapper.assertRequestSucceeded();

            // Verify three callbacks occurred, one for evidence verification, one for authorization of a principal, and one for authentication complete with a
            // success status.
            List<Callback> callbacks = callbackHandler.getCapturedCallbacks();
            assertThat(callbacks).hasSize(3);
            assertThat(callbacks.get(0)).isInstanceOf(EvidenceVerifyCallback.class);
            assertThat(callbacks.get(1)).isInstanceOf(PrincipalAuthorizeCallback.class);
            assertThat(callbacks.get(2)).isInstanceOf(AuthenticationCompleteCallback.class);
            assertTrue(((AuthenticationCompleteCallback) callbacks.get(2)).succeeded());


            // Validate the evidence that was passed to the EvidenceVerifyCallback.
            EvidenceVerifyCallback evidenceVerifyCallback = (EvidenceVerifyCallback)callbacks.get(0);
            Evidence evidence = evidenceVerifyCallback.getEvidence();
            assertInstanceOf(X509CertificateEvidence.class, evidence);
            X509CertificateEvidence x509Evidence = (X509CertificateEvidence) evidence;
            assertEquals(certificate, x509Evidence.getCertificate());
            assertEquals("cn=server1<cn=issuer1><cn=server2><cn=issuer2><cn=certuser><cn=certissuer>", x509Evidence.getUsername());
            List<SubjectIssuerDNPair> entities = new ArrayList<>();
            entities.add(SubjectIssuerDNPair.of("cn=server1", "cn=issuer1"));
            entities.add(SubjectIssuerDNPair.of("cn=server2", "cn=issuer2"));
            entities.add(SubjectIssuerDNPair.of("cn=certuser", "cn=certissuer"));
            assertEquals(entities, x509Evidence.getEntities());
        }
    }

    /**
     * Covers test scenarios where caching the identity in the session scope is enabled.
     */
    @DisplayName("When caching the identity in the HTTP session is enabled")
    @Nested
    class CachingTests {

        @BeforeEach
        void setUp() {
            // Disable session caching features.
            givenIdentityRestorationEnabled(true);
            givenSessionIdChangeEnabled(true);

            // Set up the mock session scope for the request.
            HttpScope scope = mock(HttpScope.class);
            scopeWrapper = new HttpScopeHandler(scope);
            requestWrapper.setSessionScope(scope);
        }

        @DisplayName("The cached identity should be used if already in the scope")
        @Test
        void givenIdentityAlreadyCached()throws HttpAuthenticationException {
            scopeWrapper.supportAttachments();
            scopeWrapper.setExists(true);

            SecurityIdentity identity = securityDomain.createAdHocIdentity("test");
            CachedIdentity cachedIdentity = new CachedIdentity("DATAWAVE-AUTH", false, identity);
            scopeWrapper.setCachedIdentity(cachedIdentity);

            callbackHandler.actLikeFirstAuthorizationAttemptSucceeds();

            evaluateRequest();

            // Verify the request succeeded.
            requestWrapper.assertRequestSucceeded();

            // Verify two callbacks occurred, one for authorizing a cached identity, and one for authentication complete with a success status.
            List<Callback> callbacks = callbackHandler.getCapturedCallbacks();
            assertThat(callbacks).hasSize(2);
            assertThat(callbacks.get(0)).isInstanceOf(CachedIdentityAuthorizeCallback.class);
            assertThat(callbacks.get(1)).isInstanceOf(AuthenticationCompleteCallback.class);
            assertTrue(((AuthenticationCompleteCallback) callbacks.get(1)).succeeded());

            // The session id should not have been changed.
            scopeWrapper.assertSessionIdNotChanged();
        }

        @DisplayName("The identity should not be cached if the scope does not support attachments.")
        @Test
        void givenSessionScopeDoesNotSupportAttachments()throws HttpAuthenticationException {
            // Tell the scope to not support attachments.
            scopeWrapper.doNotSupportAttachments();

            // Configure JWT.
            requestWrapper.setHeader("Authorization", "Bearer iamatoken");

            // The first authorization attempt with the identity cache should fail.
            callbackHandler.actLikeFirstAuthorizationAttemptFails();
            // Act like the evidence passes verification, and the final principal is authorized.
            callbackHandler.actLikeEvidenceIsVerified();
            callbackHandler.actLikeSecondAuthorizationAttemptSucceeds();

            // Trigger the request.
            evaluateRequest();

            // Verify the request succeeded.
            requestWrapper.assertRequestSucceeded();

            // Verify four callbacks occurred, one for at attempt to use the empty identity cache, one for evidence verification, one for authorization of a
            // principal, and one for authentication complete with a success status.
            List<Callback> callbacks = callbackHandler.getCapturedCallbacks();
            assertThat(callbacks).hasSize(4);
            assertThat(callbacks.get(0)).isInstanceOf(CachedIdentityAuthorizeCallback.class);
            assertThat(callbacks.get(1)).isInstanceOf(EvidenceVerifyCallback.class);
            assertThat(callbacks.get(2)).isInstanceOf(CachedIdentityAuthorizeCallback.class);
            assertThat(callbacks.get(3)).isInstanceOf(AuthenticationCompleteCallback.class);
            assertTrue(((AuthenticationCompleteCallback) callbacks.get(3)).succeeded());

            // Validate the evidence that was passed to the EvidenceVerifyCallback.
            EvidenceVerifyCallback evidenceVerifyCallback = (EvidenceVerifyCallback) callbacks.get(1);
            Evidence evidence = evidenceVerifyCallback.getEvidence();
            assertInstanceOf(JWTEvidence.class, evidence);
            assertEquals("iamatoken", ((JWTEvidence) evidence).getToken());

            // Nothing should have been attached to the scope.
            scopeWrapper.assertNothingAttachedToScope();
        }

        @DisplayName("The identity should be cached if the scope support attachments.")
        @Test
        void givenSessionScopeSupportsAttachments()throws HttpAuthenticationException {
            // Tell the scope to support attachments.
            scopeWrapper.supportAttachments();

            // Configure JWT.
            requestWrapper.setHeader("Authorization", "Bearer iamatoken");

            // The first authorization attempt with the identity cache should fail.
            callbackHandler.actLikeFirstAuthorizationAttemptFails();
            // Act like the evidence passes verification, and the final principal is authorized.
            callbackHandler.actLikeEvidenceIsVerified();
            callbackHandler.actLikeSecondAuthorizationAttemptSucceeds();

            // Trigger the request.
            evaluateRequest();

            // Verify the request succeeded.
            requestWrapper.assertRequestSucceeded();

            // Verify four callbacks occurred, one for at attempt to use the empty identity cache, one for evidence verification, one for authorization of a
            // principal, and one for authentication complete with a success status.
            List<Callback> callbacks = callbackHandler.getCapturedCallbacks();
            assertThat(callbacks).hasSize(4);
            assertThat(callbacks.get(0)).isInstanceOf(CachedIdentityAuthorizeCallback.class);
            assertThat(callbacks.get(1)).isInstanceOf(EvidenceVerifyCallback.class);
            assertThat(callbacks.get(2)).isInstanceOf(CachedIdentityAuthorizeCallback.class);
            assertThat(callbacks.get(3)).isInstanceOf(AuthenticationCompleteCallback.class);
            assertTrue(((AuthenticationCompleteCallback) callbacks.get(3)).succeeded());

            // Validate the evidence that was passed to the EvidenceVerifyCallback.
            EvidenceVerifyCallback evidenceVerifyCallback = (EvidenceVerifyCallback) callbacks.get(1);
            Evidence evidence = evidenceVerifyCallback.getEvidence();
            assertInstanceOf(JWTEvidence.class, evidence);
            assertEquals("iamatoken", ((JWTEvidence) evidence).getToken());

            // The identity should have been attached to the scope.
            scopeWrapper.assertIdentityAttachedToScope();

            // The session id should have been changed.
            scopeWrapper.assertSessionIdChanged();
        }

        @DisplayName("The session ID should not be changed if disabled.")
        @Test
        void givenSessionIdChangeDisabled()throws HttpAuthenticationException {
            givenSessionIdChangeEnabled(false);

            // Tell the scope to support attachments.
            scopeWrapper.supportAttachments();

            // Configure JWT.
            requestWrapper.setHeader("Authorization", "Bearer iamatoken");

            // The first authorization attempt with the identity cache should fail.
            callbackHandler.actLikeFirstAuthorizationAttemptFails();
            // Act like the evidence passes verification, and the final principal is authorized.
            callbackHandler.actLikeEvidenceIsVerified();
            callbackHandler.actLikeSecondAuthorizationAttemptSucceeds();

            // Trigger the request.
            evaluateRequest();

            // Verify the request succeeded.
            requestWrapper.assertRequestSucceeded();

            // Verify four callbacks occurred, one for at attempt to use the empty identity cache, one for evidence verification, one for authorization of a
            // principal, and one for authentication complete with a success status.
            List<Callback> callbacks = callbackHandler.getCapturedCallbacks();
            assertThat(callbacks).hasSize(4);
            assertThat(callbacks.get(0)).isInstanceOf(CachedIdentityAuthorizeCallback.class);
            assertThat(callbacks.get(1)).isInstanceOf(EvidenceVerifyCallback.class);
            assertThat(callbacks.get(2)).isInstanceOf(CachedIdentityAuthorizeCallback.class);
            assertThat(callbacks.get(3)).isInstanceOf(AuthenticationCompleteCallback.class);
            assertTrue(((AuthenticationCompleteCallback) callbacks.get(3)).succeeded());

            // Validate the evidence that was passed to the EvidenceVerifyCallback.
            EvidenceVerifyCallback evidenceVerifyCallback = (EvidenceVerifyCallback) callbacks.get(1);
            Evidence evidence = evidenceVerifyCallback.getEvidence();
            assertInstanceOf(JWTEvidence.class, evidence);
            assertEquals("iamatoken", ((JWTEvidence) evidence).getToken());

            // The identity should have been attached to the scope.
            scopeWrapper.assertIdentityAttachedToScope();

            // The session id should not have been changed.
            scopeWrapper.assertSessionIdNotChanged();
        }
    }

    /**
     * Configures whether the mechanism should support identity restoration from the request's session scope.
     */
    private void givenIdentityRestorationEnabled(boolean enabled) {
        mechanismProperties.put(DatawaveHttpAuthenticationMechanism.Config.OPTION_ENABLE_RESTORE_IDENTITY, String.valueOf(enabled));
    }

    /**
     * Configures whether the mechanism should support changing the session ID after attaching a cached identity to the session scope.
     */
    private void givenSessionIdChangeEnabled(boolean enabled) {
        mechanismProperties.put(DatawaveHttpAuthenticationMechanism.Config.OPTION_ENABLE_SESSION_ID_CHANGE, String.valueOf(enabled));
    }

    /**
     * Creates the mechanism and evaluates the mock request.
     */
    protected void evaluateRequest() throws HttpAuthenticationException {
        mechanism = new DatawaveHttpAuthenticationMechanism(mechanismProperties, callbackHandler);
        mechanism.evaluateRequest(requestWrapper.request);
    }

    /**
     * A mock implementation of {@link CallbackHandler} that allows us to handle and set attributes in {@link Callback} during the request evaluation.
     */
    private static class MockCallbackHandler implements CallbackHandler {
        // The callbacks passed to this handler.
        private final List<Callback> capturedCallbacks = new ArrayList<>();

        // Whether to act like evidence verification succeeded.
        private boolean actLikeEvidenceIsVerified;

        // Whether to act like caller authorization succeeded. In the case of supporting cached identities, the handler may receive up to two separate
        // authorization callbacks.
        private final boolean[] actLikeCallerIsAuthorized = {false, false};
        // The number of authorization attempts we've seen. Corresponds to the index in the actLikeCallerIsAuthorized array.
        private int authorizationCallbackAttempts = 0;


        public void actLikeEvidenceIsVerified() {
            this.actLikeEvidenceIsVerified = true;
        }

        public void actLikeEvidenceIsNotVerified() {
            this.actLikeEvidenceIsVerified = false;
        }

        public void actLikeFirstAuthorizationAttemptSucceeds() {
            this.actLikeCallerIsAuthorized[0] = true;
        }

        public void actLikeFirstAuthorizationAttemptFails() {
            this.actLikeCallerIsAuthorized[0] = false;
        }

        public void actLikeSecondAuthorizationAttemptSucceeds() {
            this.actLikeCallerIsAuthorized[1] = true;
        }

        @Override
        public void handle(Callback[] callbacks) {
            Callback callback = callbacks[0];
            capturedCallbacks.add(callback);
            if (callback instanceof EvidenceVerifyCallback) {
                handleEvidenceVerifyCallback((EvidenceVerifyCallback) callback);
            } else if (callback instanceof CachedIdentityAuthorizeCallback) {
                handleCachedIdentityAuthorizeCallback((CachedIdentityAuthorizeCallback) callback);
            } else if (callback instanceof PrincipalAuthorizeCallback) {
                handlePrincipalAuthorizeCallback((PrincipalAuthorizeCallback) callback);
            } else if (callback instanceof AnonymousAuthorizationCallback ){
                handleAnonymousAuthorizationCallback((AnonymousAuthorizationCallback) callback);
            } else if (!(callback instanceof AuthenticationCompleteCallback)) {
                throw new IllegalStateException("Unknown callback type: " + callback.getClass().getName());
            }
        }

        private void handleEvidenceVerifyCallback(EvidenceVerifyCallback callback) {
            if(actLikeEvidenceIsVerified) {
                callback.getEvidence().setDecodedPrincipal(new DatawavePrincipal("principal"));
                callback.setVerified(true);
            }
        }

        private void handleCachedIdentityAuthorizeCallback(CachedIdentityAuthorizeCallback callback) {
            if(actLikeCallerIsAuthorized[authorizationCallbackAttempts]){
                callback.setSecurityDomain(securityDomain);
                SecurityIdentity identity = callback.getIdentity();
                if(identity == null) {
                    identity = securityDomain.createAdHocIdentity("test");
                }
                callback.setAuthorized(identity);
            }
            authorizationCallbackAttempts++;
        }

        private void handlePrincipalAuthorizeCallback(PrincipalAuthorizeCallback callback) {
            callback.setAuthorized(actLikeCallerIsAuthorized[authorizationCallbackAttempts]);
            authorizationCallbackAttempts++;
        }

        private void handleAnonymousAuthorizationCallback(AnonymousAuthorizationCallback callback) {
            callback.setAuthorized(actLikeCallerIsAuthorized[authorizationCallbackAttempts]);
            authorizationCallbackAttempts++;
        }

        public List<Callback> getCapturedCallbacks() {
            return capturedCallbacks;
        }

    }

    /**
     * Wrapper around a mock {@link HttpServerRequest} that allows us to configure expected behavior and assert method calls.
     */
    private static class HttpServerRequestWrapper {

        private final HttpServerRequest request;

        public HttpServerRequestWrapper() {
            this.request = mock(HttpServerRequest.class);
        }

        public void setSessionScope(HttpScope scope) {
            when(request.getScope(Scope.SESSION)).thenReturn(scope);
        }

        public void setHeader(String header, String... values) {
            when(request.getRequestHeaderValues(header)).thenReturn(asList(values));
        }

        public void setCertificate(X509Certificate certificate) {
            SSLSession session = mock(SSLSession.class);
            when(request.getSSLSession()).thenReturn(session);
            Certificate[] peerCertificates = new Certificate[] {certificate};
            when(request.getPeerCertificates()).thenReturn(peerCertificates);
        }

        public void assertRequestSucceeded() {
            verify(request).authenticationComplete(isNull(), any());
        }

        public void assertRequestFailed() {
            verify(request).authenticationFailed(any());
        }
    }

    /**
     * Wrapper around a mock {@link HttpScope} that allows us to configure expected behavior and assert method calls.
     */
    private static class HttpScopeHandler {

        private final HttpScope scope;
        private final Map<String, Object> attachments = new HashMap<>();
        private boolean exists;

        public HttpScopeHandler(HttpScope scope) {
            this.scope = scope;

            // When the method HttpScope.exists() is called, return the value of exists.
            doAnswer(invocation -> exists).when(scope).exists();

            // When the method HttpScope.create() is called, set exists to true.
            doAnswer(invocation -> {
                exists = true;
                return null;
            }).when(scope).create();

            // When the method HttpScope.setAttachment() is called, set the mapping in the attachment map.
            doAnswer(invocation -> {
                Object[] args = invocation.getArguments();
                String key = (String) args[0];
                Object value = args[1];
                attachments.put(key, value);
                return null;
            }).when(scope).setAttachment(any(), any());

            // When the method HttpScope.getAttachment() is called, return the mapping from the attachment map.
            doAnswer(invocation -> {
                Object[] args = invocation.getArguments();
                String key = (String) args[0];
                return attachments.get(key);
            }).when(scope).getAttachment(any());
        }

        public void setExists(boolean exists) {
            this.exists = exists;
        }

        public void doNotSupportAttachments() {
            when(scope.supportsAttachments()).thenReturn(false);
        }

        public void supportAttachments() {
            when(scope.supportsAttachments()).thenReturn(true);
        }

        public void setCachedIdentity(CachedIdentity identity){
            attachments.put(DatawaveHttpAuthenticationMechanism.CACHED_IDENTITY_KEY, identity);
        }

        public void assertNothingAttachedToScope() {
            verify(scope, never()).setAttachment(any(), any());
        }

        public void assertIdentityAttachedToScope() {
            verify(scope, atLeastOnce()).setAttachment(eq(DatawaveHttpAuthenticationMechanism.CACHED_IDENTITY_KEY), isA(CachedIdentity.class));
        }

        public void assertSessionIdChanged() {
            verify(scope, atMostOnce()).changeID();
        }

        public void assertSessionIdNotChanged() {
            verify(scope, never()).changeID();
        }
    }
}
