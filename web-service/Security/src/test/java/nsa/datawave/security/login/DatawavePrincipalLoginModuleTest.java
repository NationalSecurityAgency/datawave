package nsa.datawave.security.login;

import static org.easymock.EasyMock.expect;
import static org.easymock.MockType.STRICT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.security.KeyStore;
import java.security.Principal;
import java.security.acl.Group;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;

import javax.security.auth.Subject;
import javax.security.auth.login.AccountLockedException;
import javax.security.auth.login.FailedLoginException;

import nsa.datawave.security.authorization.DatawavePrincipal;
import nsa.datawave.security.authorization.DatawavePrincipalLookupBean;
import nsa.datawave.security.util.DnUtils;
import nsa.datawave.security.util.MockCallbackHandler;
import nsa.datawave.security.util.MockDatawaveCertVerifier;

import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.easymock.TestSubject;
import org.jboss.security.JSSESecurityDomain;
import org.jboss.security.SimpleGroup;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.reflect.Whitebox;

@RunWith(EasyMockRunner.class)
public class DatawavePrincipalLoginModuleTest extends EasyMockSupport {
    private static final String BLACKLIST_ROLE = "BLACKLIST_ROLE";
    @TestSubject
    private DatawavePrincipalLoginModule datawaveLoginModule = new TestDatawavePrincipalLoginModule();
    @Mock(type = STRICT)
    private JSSESecurityDomain securityDomain;
    @Mock(type = STRICT)
    private DatawavePrincipalLookupBean datawavePrincipalLookupBean;
    private MockCallbackHandler callbackHandler;
    
    private KeyStore truststore;
    private KeyStore keystore;
    private KeyStore serverKeystore;
    private X509Certificate testUserCert;
    private X509Certificate testServerCert;
    
    private DatawavePrincipal datawavePrincipal;
    
    @Before
    public void setUp() throws Exception {
        MockDatawaveCertVerifier.issuerSupported = true;
        MockDatawaveCertVerifier.verify = true;
        
        callbackHandler = new MockCallbackHandler("Username: ", "Credentials: ");
        
        replayAll();
        
        HashMap<String,String> sharedState = new HashMap<>();
        HashMap<String,String> options = new HashMap<>();
        options.put("principalClass", "nsa.datawave.security.authorization.DatawavePrincipal");
        options.put("verifier", MockDatawaveCertVerifier.class.getName());
        options.put("passwordStacking", "useFirstPass");
        options.put("ocspLevel", "required");
        options.put("blacklistUserRole", BLACKLIST_ROLE);
        
        Whitebox.setInternalState(datawaveLoginModule, DatawavePrincipalLookupBean.class, datawavePrincipalLookupBean);
        Whitebox.setInternalState(datawaveLoginModule, JSSESecurityDomain.class, securityDomain);
        datawaveLoginModule.initialize(new Subject(), callbackHandler, sharedState, options);
        
        verifyAll();
        resetAll();
        
        truststore = KeyStore.getInstance("PKCS12");
        truststore.load(getClass().getResourceAsStream("/ca.pkcs12"), "secret".toCharArray());
        keystore = KeyStore.getInstance("PKCS12");
        keystore.load(getClass().getResourceAsStream("/testUser.pkcs12"), "secret".toCharArray());
        serverKeystore = KeyStore.getInstance("PKCS12");
        serverKeystore.load(getClass().getResourceAsStream("/testServer.pkcs12"), "secret".toCharArray());
        testUserCert = (X509Certificate) keystore.getCertificate("testuser");
        testServerCert = (X509Certificate) serverKeystore.getCertificate("testserver");
        
        datawavePrincipal = new DatawavePrincipal(new String[] {DnUtils.normalizeDN(testUserCert.getSubjectDN().getName()),
                DnUtils.normalizeDN(testUserCert.getIssuerDN().getName())});
    }
    
    @Test
    public void testValidLogin() throws Exception {
        callbackHandler.name = DnUtils.buildProxiedDN(datawavePrincipal.getDNs());
        callbackHandler.credential = testUserCert;
        
        expect(securityDomain.getKeyStore()).andReturn(keystore);
        expect(securityDomain.getTrustStore()).andReturn(truststore);
        expect(datawavePrincipalLookupBean.lookupPrincipal(datawavePrincipal.getDNs())).andReturn(datawavePrincipal);
        
        replayAll();
        
        boolean success = datawaveLoginModule.login();
        assertTrue("Login did not succeed.", success);
        
        verifyAll();
    }
    
    @Test
    public void testValidLoginWithCertChain() throws Exception {
        callbackHandler.name = DnUtils.buildProxiedDN(datawavePrincipal.getDNs());
        callbackHandler.credential = new X509Certificate[] {testUserCert, (X509Certificate) truststore.getCertificate("ca")};
        
        expect(securityDomain.getKeyStore()).andReturn(keystore);
        expect(securityDomain.getTrustStore()).andReturn(truststore);
        expect(datawavePrincipalLookupBean.lookupPrincipal(datawavePrincipal.getDNs())).andReturn(datawavePrincipal);
        
        replayAll();
        
        boolean success = datawaveLoginModule.login();
        assertTrue("Login did not succeed.", success);
        
        verifyAll();
    }
    
    @Test
    public void testGetRoleSets() throws Exception {
        callbackHandler.name = DnUtils.buildProxiedDN(datawavePrincipal.getDNs());
        callbackHandler.credential = testUserCert;
        
        String[] expectedAuthServiceRoles = new String[] {"Role1", "Role2", "Role3"};
        String[] expectedRoleSets = new String[] {"Role1", "Role2"};
        datawavePrincipal.setUserRoles(callbackHandler.name, Arrays.asList(expectedAuthServiceRoles));
        datawavePrincipal.setAuthorizations(callbackHandler.name, Arrays.asList("a", "b", "c"));
        datawavePrincipal.setRoleSets(Arrays.asList(expectedRoleSets));
        
        expect(securityDomain.getKeyStore()).andReturn(keystore);
        expect(securityDomain.getTrustStore()).andReturn(truststore);
        expect(datawavePrincipalLookupBean.lookupPrincipal(datawavePrincipal.getDNs())).andReturn(datawavePrincipal);
        
        replayAll();
        
        boolean success = datawaveLoginModule.login();
        assertTrue("Login did not succeed.", success);
        
        Group[] roleSets = datawaveLoginModule.getRoleSets();
        assertEquals(2, roleSets.length);
        
        SimpleGroup roles = (SimpleGroup) roleSets[0];
        assertEquals("Roles", roles.getName());
        ArrayList<String> rolesList = new ArrayList<>();
        for (Enumeration<Principal> members = roles.members(); members.hasMoreElements(); /* empty */) {
            rolesList.add(members.nextElement().getName());
        }
        Collections.sort(rolesList);
        assertEquals(2, rolesList.size());
        assertArrayEquals(expectedRoleSets, rolesList.toArray());
        
        SimpleGroup callerPrincipal = (SimpleGroup) roleSets[1];
        assertEquals("CallerPrincipal", callerPrincipal.getName());
        Enumeration<Principal> members = callerPrincipal.members();
        assertTrue("CallerPrincipal group has no members", members.hasMoreElements());
        Principal p = members.nextElement();
        assertEquals(datawavePrincipal, p);
        assertFalse("CallerPrincipal group has too many members", members.hasMoreElements());
        
        verifyAll();
    }
    
    @Test(expected = AccountLockedException.class)
    public void testBlacklistedUser() throws Exception {
        callbackHandler.name = DnUtils.buildProxiedDN(datawavePrincipal.getDNs());
        callbackHandler.credential = testUserCert;
        
        List<String> roles = Collections.singletonList(BLACKLIST_ROLE);
        datawavePrincipal.setRoleSets(roles);
        datawavePrincipal.setRawRoles(datawavePrincipal.getName(), roles);
        
        expect(securityDomain.getKeyStore()).andReturn(keystore);
        expect(securityDomain.getTrustStore()).andReturn(truststore);
        expect(datawavePrincipalLookupBean.lookupPrincipal(datawavePrincipal.getDNs())).andReturn(datawavePrincipal);
        
        replayAll();
        
        boolean success = datawaveLoginModule.login();
        assertFalse("Login should not have succeeded.", success);
        
        verifyAll();
    }
    
    @Test(expected = AccountLockedException.class)
    public void testBlacklistedProxiedUser() throws Exception {
        // Proxied entities has the original user DN, plus it came through a server and
        // the request is being made by a second server. Make sure that the resulting
        // principal has all 3 server DNs in its list, and the user DN is not one of the
        // server DNs.
        String issuerDN = DnUtils.normalizeDN(testServerCert.getIssuerDN().getName());
        String serverDN = DnUtils.normalizeDN("CN=testServer.example.com, OU=iamnotaperson, OU=acme");
        String otherServerDN = DnUtils.normalizeDN("CN=otherServer.example.com, OU=iamnotaperson, OU=acme");
        String proxiedEntities = "<" + serverDN + "><" + issuerDN + "><" + otherServerDN + "><" + issuerDN + "><" + datawavePrincipal.getUserDN() + "><"
                        + issuerDN + ">";
        callbackHandler.name = proxiedEntities;
        callbackHandler.credential = testServerCert;
        
        DatawavePrincipal expected = new DatawavePrincipal(proxiedEntities);
        List<String> blacklistRoles = Arrays.asList(BLACKLIST_ROLE, "TEST_ROLE");
        List<String> otherRoles = Collections.singletonList("TEST_ROLE");
        expected.setRoleSets(otherRoles);
        expected.setRawRoles(DnUtils.buildNormalizedProxyDN(datawavePrincipal.getUserDN(), issuerDN, null, null), otherRoles);
        expected.setRawRoles(DnUtils.buildNormalizedProxyDN(serverDN, issuerDN, null, null), otherRoles);
        expected.setRawRoles(DnUtils.buildNormalizedProxyDN(otherServerDN, issuerDN, null, null), blacklistRoles);
        
        expect(securityDomain.getKeyStore()).andReturn(serverKeystore);
        expect(securityDomain.getTrustStore()).andReturn(truststore);
        expect(datawavePrincipalLookupBean.lookupPrincipal(expected.getDNs())).andReturn(expected);
        
        replayAll();
        
        boolean success = datawaveLoginModule.login();
        assertFalse("Login should not have succeeded.", success);
        
        verifyAll();
    }
    
    @Test
    public void testProxiedEntitiesLogin() throws Exception {
        // Proxied entities has the original user DN, plus it came through a server and
        // the request is being made by a second server. Make sure that the resulting
        // principal has all 3 server DNs in its list, and the user DN is not one of the
        // server DNs.
        String issuerDN = DnUtils.normalizeDN(testServerCert.getIssuerDN().getName());
        String serverDN = DnUtils.normalizeDN("CN=testServer.example.com, OU=iamnotaperson, OU=acme");
        String otherServerDN = DnUtils.normalizeDN("CN=otherServer.example.com, OU=iamnotaperson, OU=acme");
        String proxiedEntities = "<" + serverDN + "><" + issuerDN + "><" + otherServerDN + "><" + issuerDN + "><" + datawavePrincipal.getUserDN() + "><"
                        + issuerDN + ">";
        callbackHandler.name = proxiedEntities;
        callbackHandler.credential = testServerCert;
        
        DatawavePrincipal expected = new DatawavePrincipal(proxiedEntities);
        
        expect(securityDomain.getKeyStore()).andReturn(serverKeystore);
        expect(securityDomain.getTrustStore()).andReturn(truststore);
        expect(datawavePrincipalLookupBean.lookupPrincipal(expected.getDNs())).andReturn(expected);
        
        replayAll();
        
        boolean success = datawaveLoginModule.login();
        assertTrue("Login did not succeed.", success);
        assertEquals(datawavePrincipal.getUserDN(), expected.getUserDN());
        
        verifyAll();
    }
    
    @Test(expected = FailedLoginException.class)
    public void testInvalidProxiedEntitiesLogin() throws Exception {
        // Proxied entities has the original user DN, plus it came through a server and
        // the request is being made by a second server. Make sure that the resulting
        // principal has all 3 server DNs in its list, and the user DN is not one of the
        // server DNs.
        String issuerDN = testServerCert.getIssuerDN().getName();
        String serverDN = "CN=testServer.example.com, OU=iamnotaperson, OU=acme";
        String otherUserDN = "CN=Other User Name ouser, OU=acme";
        String proxiedEntities = "<" + serverDN + "><" + issuerDN + "><" + otherUserDN + "><" + issuerDN + ">";
        for (String dn : datawavePrincipal.getDNs()) {
            proxiedEntities += "<" + dn + ">";
        }
        callbackHandler.name = proxiedEntities;
        callbackHandler.credential = testServerCert;
        
        expect(securityDomain.getKeyStore()).andReturn(serverKeystore);
        expect(securityDomain.getTrustStore()).andReturn(truststore);
        
        replayAll();
        
        try {
            datawaveLoginModule.login();
        } finally {
            verifyAll();
        }
    }
    
    @Test
    public void testAllowedUserUserProxiedEntitiesLogin() throws Exception {
        Whitebox.setInternalState(datawaveLoginModule, "allowUserProxying", true);
        
        // Proxied entities has the original user DN, plus it came through a server and
        // the request is being made by a second server. Make sure that the resulting
        // principal has all 3 server DNs in its list, and the user DN is not one of the
        // server DNs.
        String issuerDN = DnUtils.normalizeDN(testUserCert.getIssuerDN().getName());
        String serverDN = DnUtils.normalizeDN("CN=testServer.example.com, OU=iamnotaperson, OU=acme");
        String otherUserDN = DnUtils.normalizeDN("CN=Other User Name ouser, OU=acme");
        String proxiedEntities = "<" + serverDN + "><" + issuerDN + "><" + otherUserDN + "><" + issuerDN + "><" + datawavePrincipal.getUserDN() + "><"
                        + issuerDN + ">";
        callbackHandler.name = proxiedEntities;
        callbackHandler.credential = testServerCert;
        
        DatawavePrincipal expected = new DatawavePrincipal(proxiedEntities);
        
        expect(securityDomain.getKeyStore()).andReturn(serverKeystore);
        expect(securityDomain.getTrustStore()).andReturn(truststore);
        expect(datawavePrincipalLookupBean.lookupPrincipal(expected.getDNs())).andReturn(expected);
        
        replayAll();
        
        try {
            boolean success = datawaveLoginModule.login();
            assertTrue("Login did not succeed.", success);
            assertEquals(otherUserDN, expected.getUserDN());
        } finally {
            verifyAll();
        }
    }
    
    @Test(expected = FailedLoginException.class)
    public void testInvalidLoginCertIssuerDenied() throws Exception {
        MockDatawaveCertVerifier.issuerSupported = false;
        callbackHandler.name = datawavePrincipal.getUserDN();
        callbackHandler.credential = testUserCert;
        
        expect(securityDomain.getKeyStore()).andReturn(keystore);
        expect(securityDomain.getTrustStore()).andReturn(truststore);
        
        replayAll();
        
        try {
            datawaveLoginModule.login();
        } finally {
            verifyAll();
        }
    }
    
    @Test(expected = FailedLoginException.class)
    public void testInvalidLoginCertVerificationFailed() throws Exception {
        MockDatawaveCertVerifier.verify = false;
        callbackHandler.name = datawavePrincipal.getUserDN();
        callbackHandler.credential = testUserCert;
        
        expect(securityDomain.getKeyStore()).andReturn(keystore);
        expect(securityDomain.getTrustStore()).andReturn(truststore);
        
        replayAll();
        
        try {
            datawaveLoginModule.login();
        } finally {
            verifyAll();
        }
    }
    
    @Test(expected = FailedLoginException.class)
    public void testInvalidLoginAuthorizationLookupFailed() throws Exception {
        callbackHandler.name = datawavePrincipal.getUserDN();
        callbackHandler.credential = testUserCert;
        
        expect(securityDomain.getKeyStore()).andReturn(keystore);
        expect(securityDomain.getTrustStore()).andReturn(truststore);
        expect(datawavePrincipalLookupBean.lookupPrincipal(new String[] {datawavePrincipal.getUserDN()})).andThrow(
                        new FailedLoginException("Unable to authenticate"));
        
        replayAll();
        
        try {
            datawaveLoginModule.login();
        } finally {
            verifyAll();
        }
    }
    
    private static class TestDatawavePrincipalLoginModule extends DatawavePrincipalLoginModule {
        @Override
        protected void performFieldInjection() {
            // do nothing - we're using @TestSubject to inject
        }
    }
}
