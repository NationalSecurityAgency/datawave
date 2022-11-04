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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

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

import static datawave.security.util.DnUtils.normalizeDN;
import static io.undertow.security.api.AuthenticationMechanism.AuthenticationMechanismOutcome;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DatawaveAuthenticationMechanismTest {
    private static final HttpString PROXIED_ENTITIES_HEADER = new HttpString(DatawaveAuthenticationMechanism.PROXIED_ENTITIES_HEADER);
    private static final HttpString PROXIED_ISSUERS_HEADER = new HttpString(DatawaveAuthenticationMechanism.PROXIED_ISSUERS_HEADER);
    private static final HttpString SUBJECT_DN_HEADER = new HttpString("X-SSL-ClientCert-Subject");
    private static final HttpString ISSUER_DN_HEADER = new HttpString("X-SSL-ClientCert-Issuer");
    
    private DatawaveAuthenticationMechanism datawaveAuthenticationMechanism;
    
    private HeaderMap httpRequestHeaders;
    private HeaderMap httpResponseHeaders;
    
    @Mock
    private SecurityContext securityContext;
    
    @Mock
    private HttpServerExchange httpServerExchange;
    
    @Mock
    private ServerConnection serverConnection;
    
    @Mock
    private SSLSessionInfo sslSessionInfo;
    
    @Mock
    private IdentityManager identityManager;
    
    @Mock
    private Account account;
    
    @Captor
    ArgumentCaptor<DatawaveCredential> credentialCapture;
    
    private KeyStore truststore;
    private KeyStore keystore;
    private KeyStore serverKeystore;
    private X509Certificate testUserCert;
    private X509Certificate testServerCert;
    private X509Certificate[] testUserCertChain;
    private X509Certificate[] testServerCertChain;
    
    @BeforeEach
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
        
        when(httpServerExchange.getRequestHeaders()).thenReturn(httpRequestHeaders); // .times(2);
    }
    
    @Test
    public void testSSLSimpleLogin() throws Exception {
        String whenedID = normalizeDN(testUserCert.getSubjectDN().getName()) + "<" + normalizeDN(testUserCert.getIssuerDN().getName()) + ">";
        
        when(httpServerExchange.getConnection()).thenReturn(serverConnection);
        when(serverConnection.getSslSessionInfo()).thenReturn(sslSessionInfo);
        when(sslSessionInfo.getPeerCertificates()).thenReturn(new X509Certificate[] {testUserCert});
        when(securityContext.getIdentityManager()).thenReturn(identityManager);
        when(identityManager.verify(eq(whenedID), credentialCapture.capture())).thenReturn(account);
        securityContext.authenticationComplete(account, "DATAWAVE-AUTH", false);
        long requestStartTime = System.nanoTime();
        when(httpServerExchange.getRequestHeaders()).thenReturn(httpRequestHeaders);
        when(httpServerExchange.getRequestStartTime()).thenReturn(requestStartTime);
        when(httpServerExchange.getRequestHeaders()).thenReturn(httpRequestHeaders);
        
        AuthenticationMechanismOutcome outcome = datawaveAuthenticationMechanism.authenticate(httpServerExchange, securityContext);
        assertEquals(AuthenticationMechanismOutcome.AUTHENTICATED, outcome);
        assertEquals(testUserCert, credentialCapture.getValue().getCertificate());
        assertFalse(httpResponseHeaders.contains(DatawaveAuthenticationMechanism.HEADER_PROXIED_ENTITIES_ACCEPTED));
        
    }
    
    @Test
    public void testSSLProxiedLogin() throws Exception {
        String whenedID = normalizeDN(testUserCert.getSubjectDN().getName()) + "<" + normalizeDN(testUserCert.getIssuerDN().getName()) + "><"
                        + normalizeDN(testServerCert.getSubjectDN().getName()) + "><" + normalizeDN(testServerCert.getIssuerDN().getName()) + ">";
        httpRequestHeaders.add(PROXIED_ENTITIES_HEADER, testUserCert.getSubjectDN().toString());
        httpRequestHeaders.add(PROXIED_ISSUERS_HEADER, testUserCert.getIssuerDN().toString());
        
        when(httpServerExchange.getConnection()).thenReturn(serverConnection);
        when(serverConnection.getSslSessionInfo()).thenReturn(sslSessionInfo);
        when(sslSessionInfo.getPeerCertificates()).thenReturn(new X509Certificate[] {testServerCert});
        when(securityContext.getIdentityManager()).thenReturn(identityManager);
        when(identityManager.verify(eq(whenedID), credentialCapture.capture())).thenReturn(account);
        securityContext.authenticationComplete(account, "DATAWAVE-AUTH", false);
        long requestStartTime = System.nanoTime();
        when(httpServerExchange.getRequestHeaders()).thenReturn(httpRequestHeaders);
        when(httpServerExchange.getResponseHeaders()).thenReturn(httpResponseHeaders);
        when(httpServerExchange.getRequestStartTime()).thenReturn(requestStartTime);
        when(httpServerExchange.getRequestHeaders()).thenReturn(httpRequestHeaders);
        
        AuthenticationMechanismOutcome outcome = datawaveAuthenticationMechanism.authenticate(httpServerExchange, securityContext);
        assertEquals(AuthenticationMechanismOutcome.AUTHENTICATED, outcome);
        assertEquals(testServerCert, credentialCapture.getValue().getCertificate());
        assertEquals("true", httpResponseHeaders.getFirst(DatawaveAuthenticationMechanism.HEADER_PROXIED_ENTITIES_ACCEPTED));
    }
    
    @Test
    public void testSSLWithoutPeerCerts() throws Exception {
        httpRequestHeaders.add(SUBJECT_DN_HEADER, testUserCert.getSubjectDN().toString());
        httpRequestHeaders.add(ISSUER_DN_HEADER, testUserCert.getIssuerDN().toString());
        
        String whenedID = normalizeDN(testUserCert.getSubjectDN().getName()) + "<" + normalizeDN(testUserCert.getIssuerDN().getName()) + ">";
        
        when(httpServerExchange.getConnection()).thenReturn(serverConnection);
        when(serverConnection.getSslSessionInfo()).thenReturn(sslSessionInfo);
        when(sslSessionInfo.getPeerCertificates()).thenThrow(new SSLPeerUnverifiedException("no client cert"));
        when(httpServerExchange.getRequestHeaders()).thenReturn(httpRequestHeaders);
        when(securityContext.getIdentityManager()).thenReturn(identityManager);
        when(identityManager.verify(eq(whenedID), isA(Credential.class))).thenReturn(account);
        securityContext.authenticationComplete(account, "DATAWAVE-AUTH", false);
        long requestStartTime = System.nanoTime();
        when(httpServerExchange.getRequestHeaders()).thenReturn(httpRequestHeaders);
        when(httpServerExchange.getRequestStartTime()).thenReturn(requestStartTime);
        when(httpServerExchange.getRequestHeaders()).thenReturn(httpRequestHeaders);
        
        AuthenticationMechanismOutcome outcome = datawaveAuthenticationMechanism.authenticate(httpServerExchange, securityContext);
        assertEquals(AuthenticationMechanismOutcome.AUTHENTICATED, outcome);
        assertFalse(httpResponseHeaders.contains(DatawaveAuthenticationMechanism.HEADER_PROXIED_ENTITIES_ACCEPTED));
    }
    
    @Test
    public void testSSLWithoutPeerCertsNoTrustedHeaderAuthentication() throws Exception {
        ReflectionTestUtils.setField(datawaveAuthenticationMechanism, "trustedHeaderAuthentication", false);
        Certificate cert = new Certificate("DUMMY") {
            @Override
            public byte[] getEncoded() throws CertificateEncodingException {
                return new byte[0];
            }
            
            @Override
            public void verify(PublicKey key) throws CertificateException, NoSuchAlgorithmException, InvalidKeyException, NoSuchProviderException,
                            SignatureException {}
            
            @Override
            public void verify(PublicKey key, String sigProvider) throws CertificateException, NoSuchAlgorithmException, InvalidKeyException,
                            NoSuchProviderException, SignatureException {}
            
            @Override
            public String toString() {
                return null;
            }
            
            @Override
            public PublicKey getPublicKey() {
                return null;
            }
        };
        
        when(httpServerExchange.getConnection()).thenReturn(serverConnection);
        when(serverConnection.getSslSessionInfo()).thenReturn(sslSessionInfo);
        when(sslSessionInfo.getPeerCertificates()).thenReturn(new Certificate[] {cert});
        long requestStartTime = System.nanoTime();
        when(httpServerExchange.getRequestStartTime()).thenReturn(requestStartTime);
        when(httpServerExchange.getRequestHeaders()).thenReturn(httpRequestHeaders);
        
        AuthenticationMechanismOutcome outcome = datawaveAuthenticationMechanism.authenticate(httpServerExchange, securityContext);
        assertEquals(AuthenticationMechanismOutcome.NOT_ATTEMPTED, outcome);
    }
    
    @Test
    public void testMissingIssuersHeader() {
        httpRequestHeaders.add(PROXIED_ENTITIES_HEADER, "foo");
        
        securityContext.authenticationFailed("X-ProxiedEntitiesChain supplied, but missing X-ProxiedIssuersChain is missing!", "DATAWAVE-AUTH");
        when(httpServerExchange.getRequestStartTime()).thenReturn(System.nanoTime());
        when(httpServerExchange.getRequestHeaders()).thenReturn(httpRequestHeaders);
        
        AuthenticationMechanismOutcome outcome = datawaveAuthenticationMechanism.authenticate(httpServerExchange, securityContext);
        assertEquals(AuthenticationMechanismOutcome.NOT_AUTHENTICATED, outcome);
    }
    
    @Test
    public void testNonSSLSimpleLogin() throws Exception {
        httpRequestHeaders.add(SUBJECT_DN_HEADER, testUserCert.getSubjectDN().toString());
        httpRequestHeaders.add(ISSUER_DN_HEADER, testUserCert.getIssuerDN().toString());
        
        String whenedID = normalizeDN(testUserCert.getSubjectDN().getName()) + "<" + normalizeDN(testUserCert.getIssuerDN().getName()) + ">";
        
        when(httpServerExchange.getConnection()).thenReturn(serverConnection);
        when(serverConnection.getSslSessionInfo()).thenReturn(null);
        when(httpServerExchange.getRequestHeaders()).thenReturn(httpRequestHeaders);
        when(securityContext.getIdentityManager()).thenReturn(identityManager);
        when(identityManager.verify(eq(whenedID), isA(Credential.class))).thenReturn(account);
        securityContext.authenticationComplete(account, "DATAWAVE-AUTH", false);
        long requestStartTime = System.nanoTime();
        when(httpServerExchange.getRequestHeaders()).thenReturn(httpRequestHeaders);
        when(httpServerExchange.getRequestStartTime()).thenReturn(requestStartTime);
        when(httpServerExchange.getRequestHeaders()).thenReturn(httpRequestHeaders);
        
        AuthenticationMechanismOutcome outcome = datawaveAuthenticationMechanism.authenticate(httpServerExchange, securityContext);
        assertEquals(AuthenticationMechanismOutcome.AUTHENTICATED, outcome);
    }
    
    @Test
    public void testNonSSLProxiedLogin() throws Exception {
        httpRequestHeaders.add(PROXIED_ENTITIES_HEADER, testUserCert.getSubjectDN().toString());
        httpRequestHeaders.add(PROXIED_ISSUERS_HEADER, testUserCert.getIssuerDN().toString());
        httpRequestHeaders.add(SUBJECT_DN_HEADER, testServerCert.getSubjectDN().toString());
        httpRequestHeaders.add(ISSUER_DN_HEADER, testServerCert.getIssuerDN().toString());
        
        String whenedID = normalizeDN(testUserCert.getSubjectDN().getName()) + "<" + normalizeDN(testUserCert.getIssuerDN().getName()) + "><"
                        + normalizeDN(testServerCert.getSubjectDN().getName()) + "><" + normalizeDN(testServerCert.getIssuerDN().getName()) + ">";
        
        when(httpServerExchange.getConnection()).thenReturn(serverConnection);
        when(serverConnection.getSslSessionInfo()).thenReturn(null);
        when(httpServerExchange.getRequestHeaders()).thenReturn(httpRequestHeaders);
        when(securityContext.getIdentityManager()).thenReturn(identityManager);
        when(identityManager.verify(eq(whenedID), isA(Credential.class))).thenReturn(account);
        securityContext.authenticationComplete(account, "DATAWAVE-AUTH", false);
        long requestStartTime = System.nanoTime();
        when(httpServerExchange.getRequestHeaders()).thenReturn(httpRequestHeaders);
        when(httpServerExchange.getResponseHeaders()).thenReturn(httpResponseHeaders);
        when(httpServerExchange.getRequestStartTime()).thenReturn(requestStartTime);
        when(httpServerExchange.getRequestHeaders()).thenReturn(httpRequestHeaders);
        
        AuthenticationMechanismOutcome outcome = datawaveAuthenticationMechanism.authenticate(httpServerExchange, securityContext);
        assertEquals(AuthenticationMechanismOutcome.AUTHENTICATED, outcome);
        assertEquals("true", httpResponseHeaders.getFirst(DatawaveAuthenticationMechanism.HEADER_PROXIED_ENTITIES_ACCEPTED));
    }
    
    @Test
    public void testNonSSLoginWithoutIssuerHeaderFails() throws Exception {
        httpRequestHeaders.add(SUBJECT_DN_HEADER, testUserCert.getSubjectDN().toString());
        
        when(httpServerExchange.getConnection()).thenReturn(serverConnection);
        when(serverConnection.getSslSessionInfo()).thenReturn(null);
        when(httpServerExchange.getRequestHeaders()).thenReturn(httpRequestHeaders);
        securityContext.authenticationFailed("Missing trusted subject DN (" + testUserCert.getSubjectDN()
                        + ") or issuer DN (null) for trusted header authentication.", "DATAWAVE-AUTH");
        when(httpServerExchange.getRequestStartTime()).thenReturn(System.nanoTime());
        when(httpServerExchange.getRequestHeaders()).thenReturn(httpRequestHeaders);
        
        AuthenticationMechanismOutcome outcome = datawaveAuthenticationMechanism.authenticate(httpServerExchange, securityContext);
        assertEquals(AuthenticationMechanismOutcome.NOT_AUTHENTICATED, outcome);
    }
    
    @Test
    public void testNonSSLoginWithoutSubjectHeaderFails() throws Exception {
        httpRequestHeaders.add(ISSUER_DN_HEADER, testUserCert.getIssuerDN().toString());
        
        when(httpServerExchange.getConnection()).thenReturn(serverConnection);
        when(serverConnection.getSslSessionInfo()).thenReturn(null);
        when(httpServerExchange.getRequestHeaders()).thenReturn(httpRequestHeaders);
        securityContext.authenticationFailed("Missing trusted subject DN (null) or issuer DN (" + testUserCert.getIssuerDN()
                        + ") for trusted header authentication.", "DATAWAVE-AUTH");
        when(httpServerExchange.getRequestStartTime()).thenReturn(System.nanoTime());
        when(httpServerExchange.getRequestHeaders()).thenReturn(httpRequestHeaders);
        
        AuthenticationMechanismOutcome outcome = datawaveAuthenticationMechanism.authenticate(httpServerExchange, securityContext);
        assertEquals(AuthenticationMechanismOutcome.NOT_AUTHENTICATED, outcome);
    }
    
    @Test
    public void testNonSSLSimpleLoginFails() throws Exception {
        System.clearProperty("dw.trusted.header.authentication");
        datawaveAuthenticationMechanism = new DatawaveAuthenticationMechanism();
        
        httpRequestHeaders.add(SUBJECT_DN_HEADER, testUserCert.getSubjectDN().toString());
        httpRequestHeaders.add(ISSUER_DN_HEADER, testUserCert.getIssuerDN().toString());
        
        when(httpServerExchange.getConnection()).thenReturn(serverConnection);
        when(serverConnection.getSslSessionInfo()).thenReturn(null);
        long requestStartTime = System.nanoTime();
        when(httpServerExchange.getRequestStartTime()).thenReturn(requestStartTime);
        when(httpServerExchange.getRequestHeaders()).thenReturn(httpRequestHeaders);
        
        AuthenticationMechanismOutcome outcome = datawaveAuthenticationMechanism.authenticate(httpServerExchange, securityContext);
        assertEquals(AuthenticationMechanismOutcome.NOT_ATTEMPTED, outcome);
    }
    
    @Test
    public void testJWTHeaderAuthentication() throws Exception {
        ReflectionTestUtils.setField(datawaveAuthenticationMechanism, "trustedHeaderAuthentication", false);
        ReflectionTestUtils.setField(datawaveAuthenticationMechanism, "jwtHeaderAuthentication", true);
        
        httpRequestHeaders.add(new HttpString("Authorization"), "Bearer 1234");
        
        String whenedID = "1234";
        
        when(httpServerExchange.getConnection()).thenReturn(serverConnection);
        when(serverConnection.getSslSessionInfo()).thenReturn(null);
        when(httpServerExchange.getRequestHeaders()).thenReturn(httpRequestHeaders);
        when(httpServerExchange.getRequestHeaders()).thenReturn(httpRequestHeaders);
        when(securityContext.getIdentityManager()).thenReturn(identityManager);
        when(identityManager.verify(eq(whenedID), isA(Credential.class))).thenReturn(account);
        securityContext.authenticationComplete(account, "DATAWAVE-AUTH", false);
        when(httpServerExchange.getRequestStartTime()).thenReturn(System.nanoTime());
        when(httpServerExchange.getRequestHeaders()).thenReturn(httpRequestHeaders);
        
        AuthenticationMechanismOutcome outcome = datawaveAuthenticationMechanism.authenticate(httpServerExchange, securityContext);
        assertEquals(AuthenticationMechanismOutcome.AUTHENTICATED, outcome);
    }
}
