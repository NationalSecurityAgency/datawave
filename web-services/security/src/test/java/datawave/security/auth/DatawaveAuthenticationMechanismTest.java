package datawave.security.auth;

import io.undertow.security.api.SecurityContext;
import io.undertow.security.idm.Account;
import io.undertow.security.idm.Credential;
import io.undertow.security.idm.IdentityManager;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.SSLSessionInfo;
import io.undertow.server.ServerConnection;
import io.undertow.util.HeaderMap;
import io.undertow.util.HttpString;
import org.easymock.Capture;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.annotation.MockStrict;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import javax.net.ssl.SSLPeerUnverifiedException;
import java.security.InvalidKeyException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Enumeration;

import static io.undertow.security.api.AuthenticationMechanism.AuthenticationMechanismOutcome;
import static datawave.security.util.DnUtils.normalizeDN;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.newCapture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.powermock.api.easymock.PowerMock.replayAll;
import static org.powermock.api.easymock.PowerMock.verifyAll;

@RunWith(PowerMockRunner.class)
@PrepareForTest(HttpServerExchange.class)
public class DatawaveAuthenticationMechanismTest {
    private static final HttpString PROXIED_ENTITIES_HEADER = new HttpString(DatawaveAuthenticationMechanism.PROXIED_ENTITIES_HEADER);
    private static final HttpString PROXIED_ISSUERS_HEADER = new HttpString(DatawaveAuthenticationMechanism.PROXIED_ISSUERS_HEADER);
    private static final HttpString SUBJECT_DN_HEADER = new HttpString("X-SSL-ClientCert-Subject");
    private static final HttpString ISSUER_DN_HEADER = new HttpString("X-SSL-ClientCert-Issuer");

    private DatawaveAuthenticationMechanism datawaveAuthenticationMechanism;

    private HeaderMap httpRequestHeaders;
    private HeaderMap httpResponseHeaders;

    @MockStrict
    private SecurityContext securityContext;

    @MockStrict
    private HttpServerExchange httpServerExchange;

    @MockStrict
    private ServerConnection serverConnection;

    @MockStrict
    private SSLSessionInfo sslSessionInfo;

    @MockStrict
    private IdentityManager identityManager;

    @MockStrict
    private Account account;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private KeyStore truststore;
    private KeyStore keystore;
    private KeyStore serverKeystore;
    private X509Certificate testUserCert;
    private X509Certificate testServerCert;
    private X509Certificate[] testUserCertChain;
    private X509Certificate[] testServerCertChain;

    @Before
    public void setUp() throws Exception {
        System.setProperty("dw.trusted.header.authentication", "true");
        datawaveAuthenticationMechanism = new DatawaveAuthenticationMechanism();
        httpRequestHeaders = new HeaderMap();
        httpResponseHeaders = new HeaderMap();

        truststore = KeyStore.getInstance("PKCS12");
        truststore.load(getClass().getResourceAsStream("/ca.pkcs12"), "secret".toCharArray());
        keystore = KeyStore.getInstance("PKCS12");
        keystore.load(getClass().getResourceAsStream("/testUser.pkcs12"), "secret".toCharArray());
        serverKeystore = KeyStore.getInstance("PKCS12");
        serverKeystore.load(getClass().getResourceAsStream("/testServer.pkcs12"), "secret".toCharArray());
        testUserCert = (X509Certificate) keystore.getCertificate("testuser");
        testServerCert = (X509Certificate) serverKeystore.getCertificate("testserver");

        testUserCertChain = new X509Certificate[2];
        testUserCertChain[0] = testUserCert;
        for (Enumeration<String> e = truststore.aliases(); e.hasMoreElements();) {
            X509Certificate cert = (X509Certificate) truststore.getCertificate(e.nextElement());
            if (cert.getSubjectDN().getName().equals(testUserCert.getIssuerDN().getName())) {
                testUserCertChain[1] = cert;
                break;
            }
        }

        testServerCertChain = new X509Certificate[2];
        testServerCertChain[0] = testServerCert;
        for (Enumeration<String> e = truststore.aliases(); e.hasMoreElements();) {
            X509Certificate cert = (X509Certificate) truststore.getCertificate(e.nextElement());
            if (cert.getSubjectDN().getName().equals(testServerCert.getIssuerDN().getName())) {
                testServerCertChain[1] = cert;
                break;
            }
        }

        expect(httpServerExchange.getRequestHeaders()).andReturn(httpRequestHeaders).times(2);
    }

    @Test
    public void testSSLSimpleLogin() throws Exception {
        String expectedID = normalizeDN(testUserCert.getSubjectDN().getName()) + "<" + normalizeDN(testUserCert.getIssuerDN().getName()) + ">";

        Capture<DatawaveCredential> credentialCapture = newCapture();
        expect(httpServerExchange.getConnection()).andReturn(serverConnection);
        expect(serverConnection.getSslSessionInfo()).andReturn(sslSessionInfo);
        expect(sslSessionInfo.getPeerCertificates()).andReturn(new X509Certificate[] {testUserCert});
        expect(securityContext.getIdentityManager()).andReturn(identityManager);
        expect(identityManager.verify(eq(expectedID), capture(credentialCapture))).andReturn(account);
        securityContext.authenticationComplete(account, "DATAWAVE-AUTH", false);
        long requestStartTime = System.nanoTime();
        expect(httpServerExchange.getRequestHeaders()).andReturn(httpRequestHeaders);
        expect(httpServerExchange.getRequestStartTime()).andReturn(requestStartTime);
        expect(httpServerExchange.getRequestHeaders()).andReturn(httpRequestHeaders);

        replayAll();

        AuthenticationMechanismOutcome outcome = datawaveAuthenticationMechanism.authenticate(httpServerExchange, securityContext);
        assertEquals(AuthenticationMechanismOutcome.AUTHENTICATED, outcome);
        assertEquals(testUserCert, credentialCapture.getValue().getCertificate());
        assertFalse(httpResponseHeaders.contains(DatawaveAuthenticationMechanism.HEADER_PROXIED_ENTITIES_ACCEPTED));

        verifyAll();
    }

    @Test
    public void testSSLProxiedLogin() throws Exception {
        String expectedID = normalizeDN(testUserCert.getSubjectDN().getName()) + "<" + normalizeDN(testUserCert.getIssuerDN().getName()) + "><"
                        + normalizeDN(testServerCert.getSubjectDN().getName()) + "><" + normalizeDN(testServerCert.getIssuerDN().getName()) + ">";
        httpRequestHeaders.add(PROXIED_ENTITIES_HEADER, testUserCert.getSubjectDN().toString());
        httpRequestHeaders.add(PROXIED_ISSUERS_HEADER, testUserCert.getIssuerDN().toString());

        Capture<DatawaveCredential> credentialCapture = newCapture();
        expect(httpServerExchange.getConnection()).andReturn(serverConnection);
        expect(serverConnection.getSslSessionInfo()).andReturn(sslSessionInfo);
        expect(sslSessionInfo.getPeerCertificates()).andReturn(new X509Certificate[] {testServerCert});
        expect(securityContext.getIdentityManager()).andReturn(identityManager);
        expect(identityManager.verify(eq(expectedID), capture(credentialCapture))).andReturn(account);
        securityContext.authenticationComplete(account, "DATAWAVE-AUTH", false);
        long requestStartTime = System.nanoTime();
        expect(httpServerExchange.getRequestHeaders()).andReturn(httpRequestHeaders);
        expect(httpServerExchange.getResponseHeaders()).andReturn(httpResponseHeaders);
        expect(httpServerExchange.getRequestStartTime()).andReturn(requestStartTime);
        expect(httpServerExchange.getRequestHeaders()).andReturn(httpRequestHeaders);

        replayAll();

        AuthenticationMechanismOutcome outcome = datawaveAuthenticationMechanism.authenticate(httpServerExchange, securityContext);
        assertEquals(AuthenticationMechanismOutcome.AUTHENTICATED, outcome);
        assertEquals(testServerCert, credentialCapture.getValue().getCertificate());
        assertEquals("true", httpResponseHeaders.getFirst(DatawaveAuthenticationMechanism.HEADER_PROXIED_ENTITIES_ACCEPTED));

        verifyAll();
    }

    @Test
    public void testSSLWithoutPeerCerts() throws Exception {
        httpRequestHeaders.add(SUBJECT_DN_HEADER, testUserCert.getSubjectDN().toString());
        httpRequestHeaders.add(ISSUER_DN_HEADER, testUserCert.getIssuerDN().toString());

        String expectedID = normalizeDN(testUserCert.getSubjectDN().getName()) + "<" + normalizeDN(testUserCert.getIssuerDN().getName()) + ">";

        expect(httpServerExchange.getConnection()).andReturn(serverConnection);
        expect(serverConnection.getSslSessionInfo()).andReturn(sslSessionInfo);
        expect(sslSessionInfo.getPeerCertificates()).andThrow(new SSLPeerUnverifiedException("no client cert"));
        expect(httpServerExchange.getRequestHeaders()).andReturn(httpRequestHeaders).times(2);
        expect(securityContext.getIdentityManager()).andReturn(identityManager);
        expect(identityManager.verify(eq(expectedID), isA(Credential.class))).andReturn(account);
        securityContext.authenticationComplete(account, "DATAWAVE-AUTH", false);
        long requestStartTime = System.nanoTime();
        expect(httpServerExchange.getRequestHeaders()).andReturn(httpRequestHeaders);
        expect(httpServerExchange.getRequestStartTime()).andReturn(requestStartTime);
        expect(httpServerExchange.getRequestHeaders()).andReturn(httpRequestHeaders);

        replayAll();

        AuthenticationMechanismOutcome outcome = datawaveAuthenticationMechanism.authenticate(httpServerExchange, securityContext);
        assertEquals(AuthenticationMechanismOutcome.AUTHENTICATED, outcome);
        assertFalse(httpResponseHeaders.contains(DatawaveAuthenticationMechanism.HEADER_PROXIED_ENTITIES_ACCEPTED));

        verifyAll();
    }

    @Test
    public void testSSLWithoutPeerCertsNoTrustedHeaderAuthentication() throws Exception {
        Whitebox.setInternalState(datawaveAuthenticationMechanism, "trustedHeaderAuthentication", false);
        Certificate cert = new Certificate("DUMMY") {
            @Override
            public byte[] getEncoded() throws CertificateEncodingException {
                return new byte[0];
            }

            @Override
            public void verify(PublicKey key)
                            throws CertificateException, NoSuchAlgorithmException, InvalidKeyException, NoSuchProviderException, SignatureException {}

            @Override
            public void verify(PublicKey key, String sigProvider)
                            throws CertificateException, NoSuchAlgorithmException, InvalidKeyException, NoSuchProviderException, SignatureException {}

            @Override
            public String toString() {
                return null;
            }

            @Override
            public PublicKey getPublicKey() {
                return null;
            }
        };

        expect(httpServerExchange.getConnection()).andReturn(serverConnection);
        expect(serverConnection.getSslSessionInfo()).andReturn(sslSessionInfo);
        expect(sslSessionInfo.getPeerCertificates()).andReturn(new Certificate[] {cert});
        long requestStartTime = System.nanoTime();
        expect(httpServerExchange.getRequestStartTime()).andReturn(requestStartTime);
        expect(httpServerExchange.getRequestHeaders()).andReturn(httpRequestHeaders);

        replayAll();

        AuthenticationMechanismOutcome outcome = datawaveAuthenticationMechanism.authenticate(httpServerExchange, securityContext);
        assertEquals(AuthenticationMechanismOutcome.NOT_ATTEMPTED, outcome);

        verifyAll();
    }

    @Test
    public void testMissingIssuersHeader() {
        httpRequestHeaders.add(PROXIED_ENTITIES_HEADER, "foo");

        securityContext.authenticationFailed("X-ProxiedEntitiesChain supplied, but missing X-ProxiedIssuersChain is missing!", "DATAWAVE-AUTH");
        expect(httpServerExchange.getRequestStartTime()).andReturn(System.nanoTime());
        expect(httpServerExchange.getRequestHeaders()).andReturn(httpRequestHeaders);

        replayAll();

        AuthenticationMechanismOutcome outcome = datawaveAuthenticationMechanism.authenticate(httpServerExchange, securityContext);
        assertEquals(AuthenticationMechanismOutcome.NOT_AUTHENTICATED, outcome);

        verifyAll();
    }

    @Test
    public void testNonSSLSimpleLogin() throws Exception {
        httpRequestHeaders.add(SUBJECT_DN_HEADER, testUserCert.getSubjectDN().toString());
        httpRequestHeaders.add(ISSUER_DN_HEADER, testUserCert.getIssuerDN().toString());

        String expectedID = normalizeDN(testUserCert.getSubjectDN().getName()) + "<" + normalizeDN(testUserCert.getIssuerDN().getName()) + ">";

        expect(httpServerExchange.getConnection()).andReturn(serverConnection);
        expect(serverConnection.getSslSessionInfo()).andReturn(null);
        expect(httpServerExchange.getRequestHeaders()).andReturn(httpRequestHeaders).times(2);
        expect(securityContext.getIdentityManager()).andReturn(identityManager);
        expect(identityManager.verify(eq(expectedID), isA(Credential.class))).andReturn(account);
        securityContext.authenticationComplete(account, "DATAWAVE-AUTH", false);
        long requestStartTime = System.nanoTime();
        expect(httpServerExchange.getRequestHeaders()).andReturn(httpRequestHeaders);
        expect(httpServerExchange.getRequestStartTime()).andReturn(requestStartTime);
        expect(httpServerExchange.getRequestHeaders()).andReturn(httpRequestHeaders);

        replayAll();

        AuthenticationMechanismOutcome outcome = datawaveAuthenticationMechanism.authenticate(httpServerExchange, securityContext);
        assertEquals(AuthenticationMechanismOutcome.AUTHENTICATED, outcome);

        verifyAll();
    }

    @Test
    public void testNonSSLProxiedLogin() throws Exception {
        httpRequestHeaders.add(PROXIED_ENTITIES_HEADER, testUserCert.getSubjectDN().toString());
        httpRequestHeaders.add(PROXIED_ISSUERS_HEADER, testUserCert.getIssuerDN().toString());
        httpRequestHeaders.add(SUBJECT_DN_HEADER, testServerCert.getSubjectDN().toString());
        httpRequestHeaders.add(ISSUER_DN_HEADER, testServerCert.getIssuerDN().toString());

        String expectedID = normalizeDN(testUserCert.getSubjectDN().getName()) + "<" + normalizeDN(testUserCert.getIssuerDN().getName()) + "><"
                        + normalizeDN(testServerCert.getSubjectDN().getName()) + "><" + normalizeDN(testServerCert.getIssuerDN().getName()) + ">";

        expect(httpServerExchange.getConnection()).andReturn(serverConnection);
        expect(serverConnection.getSslSessionInfo()).andReturn(null);
        expect(httpServerExchange.getRequestHeaders()).andReturn(httpRequestHeaders).times(2);
        expect(securityContext.getIdentityManager()).andReturn(identityManager);
        expect(identityManager.verify(eq(expectedID), isA(Credential.class))).andReturn(account);
        securityContext.authenticationComplete(account, "DATAWAVE-AUTH", false);
        long requestStartTime = System.nanoTime();
        expect(httpServerExchange.getRequestHeaders()).andReturn(httpRequestHeaders);
        expect(httpServerExchange.getResponseHeaders()).andReturn(httpResponseHeaders);
        expect(httpServerExchange.getRequestStartTime()).andReturn(requestStartTime);
        expect(httpServerExchange.getRequestHeaders()).andReturn(httpRequestHeaders);

        replayAll();

        AuthenticationMechanismOutcome outcome = datawaveAuthenticationMechanism.authenticate(httpServerExchange, securityContext);
        assertEquals(AuthenticationMechanismOutcome.AUTHENTICATED, outcome);
        assertEquals("true", httpResponseHeaders.getFirst(DatawaveAuthenticationMechanism.HEADER_PROXIED_ENTITIES_ACCEPTED));

        verifyAll();
    }

    @Test
    public void testNonSSLoginWithoutIssuerHeaderFails() throws Exception {
        httpRequestHeaders.add(SUBJECT_DN_HEADER, testUserCert.getSubjectDN().toString());

        expect(httpServerExchange.getConnection()).andReturn(serverConnection);
        expect(serverConnection.getSslSessionInfo()).andReturn(null);
        expect(httpServerExchange.getRequestHeaders()).andReturn(httpRequestHeaders).times(2);
        securityContext.authenticationFailed(
                        "Missing trusted subject DN (" + testUserCert.getSubjectDN() + ") or issuer DN (null) for trusted header authentication.",
                        "DATAWAVE-AUTH");
        expect(httpServerExchange.getRequestStartTime()).andReturn(System.nanoTime());
        expect(httpServerExchange.getRequestHeaders()).andReturn(httpRequestHeaders);

        replayAll();

        AuthenticationMechanismOutcome outcome = datawaveAuthenticationMechanism.authenticate(httpServerExchange, securityContext);
        assertEquals(AuthenticationMechanismOutcome.NOT_AUTHENTICATED, outcome);

        verifyAll();
    }

    @Test
    public void testNonSSLoginWithoutSubjectHeaderFails() throws Exception {
        httpRequestHeaders.add(ISSUER_DN_HEADER, testUserCert.getIssuerDN().toString());

        expect(httpServerExchange.getConnection()).andReturn(serverConnection);
        expect(serverConnection.getSslSessionInfo()).andReturn(null);
        expect(httpServerExchange.getRequestHeaders()).andReturn(httpRequestHeaders).times(2);
        securityContext.authenticationFailed(
                        "Missing trusted subject DN (null) or issuer DN (" + testUserCert.getIssuerDN() + ") for trusted header authentication.",
                        "DATAWAVE-AUTH");
        expect(httpServerExchange.getRequestStartTime()).andReturn(System.nanoTime());
        expect(httpServerExchange.getRequestHeaders()).andReturn(httpRequestHeaders);

        replayAll();

        AuthenticationMechanismOutcome outcome = datawaveAuthenticationMechanism.authenticate(httpServerExchange, securityContext);
        assertEquals(AuthenticationMechanismOutcome.NOT_AUTHENTICATED, outcome);

        verifyAll();
    }

    @Test
    public void testNonSSLSimpleLoginFails() throws Exception {
        System.clearProperty("dw.trusted.header.authentication");
        datawaveAuthenticationMechanism = new DatawaveAuthenticationMechanism();

        httpRequestHeaders.add(SUBJECT_DN_HEADER, testUserCert.getSubjectDN().toString());
        httpRequestHeaders.add(ISSUER_DN_HEADER, testUserCert.getIssuerDN().toString());

        expect(httpServerExchange.getConnection()).andReturn(serverConnection);
        expect(serverConnection.getSslSessionInfo()).andReturn(null);
        long requestStartTime = System.nanoTime();
        expect(httpServerExchange.getRequestStartTime()).andReturn(requestStartTime);
        expect(httpServerExchange.getRequestHeaders()).andReturn(httpRequestHeaders);

        replayAll();

        AuthenticationMechanismOutcome outcome = datawaveAuthenticationMechanism.authenticate(httpServerExchange, securityContext);
        assertEquals(AuthenticationMechanismOutcome.NOT_ATTEMPTED, outcome);

        verifyAll();
    }

    @Test
    public void testJWTHeaderAuthentication() throws Exception {
        Whitebox.setInternalState(datawaveAuthenticationMechanism, "trustedHeaderAuthentication", false);
        Whitebox.setInternalState(datawaveAuthenticationMechanism, "jwtHeaderAuthentication", true);

        httpRequestHeaders.add(new HttpString("Authorization"), "Bearer 1234");

        String expectedID = "1234";

        expect(httpServerExchange.getConnection()).andReturn(serverConnection);
        expect(serverConnection.getSslSessionInfo()).andReturn(null);
        expect(httpServerExchange.getRequestHeaders()).andReturn(httpRequestHeaders);
        expect(httpServerExchange.getRequestHeaders()).andReturn(httpRequestHeaders);
        expect(securityContext.getIdentityManager()).andReturn(identityManager);
        expect(identityManager.verify(eq(expectedID), isA(Credential.class))).andReturn(account);
        securityContext.authenticationComplete(account, "DATAWAVE-AUTH", false);
        expect(httpServerExchange.getRequestStartTime()).andReturn(System.nanoTime());
        expect(httpServerExchange.getRequestHeaders()).andReturn(httpRequestHeaders);

        replayAll();

        AuthenticationMechanismOutcome outcome = datawaveAuthenticationMechanism.authenticate(httpServerExchange, securityContext);
        assertEquals(AuthenticationMechanismOutcome.AUTHENTICATED, outcome);

        verifyAll();
    }
}
