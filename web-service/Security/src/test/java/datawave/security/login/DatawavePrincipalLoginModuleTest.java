package datawave.security.login;

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

import com.google.common.collect.Lists;
import datawave.security.auth.DatawaveCredential;
import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUserService;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnUtils;
import datawave.security.util.DnUtils.NpeUtils;
import datawave.security.util.MockCallbackHandler;
import datawave.security.util.MockDatawaveCertVerifier;

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
    private DatawaveUserService datawaveUserService;
    private MockCallbackHandler callbackHandler;
    
    private KeyStore truststore;
    private KeyStore keystore;
    private KeyStore serverKeystore;
    private X509Certificate testUserCert;
    private X509Certificate testServerCert;
    
    private SubjectIssuerDNPair userDN;
    private DatawavePrincipal defaultPrincipal;
    
    @Before
    public void setUp() throws Exception {
        System.setProperty(NpeUtils.NPE_OU_PROPERTY, "iamnotaperson");
        
        MockDatawaveCertVerifier.issuerSupported = true;
        MockDatawaveCertVerifier.verify = true;
        
        callbackHandler = new MockCallbackHandler("Username: ", "Credentials: ");
        
        replayAll();
        
        HashMap<String,String> sharedState = new HashMap<>();
        HashMap<String,String> options = new HashMap<>();
        options.put("principalClass", "datawave.security.authorization.DatawavePrincipal");
        options.put("verifier", MockDatawaveCertVerifier.class.getName());
        options.put("passwordStacking", "useFirstPass");
        options.put("ocspLevel", "required");
        options.put("blacklistUserRole", BLACKLIST_ROLE);
        
        Whitebox.setInternalState(datawaveLoginModule, DatawaveUserService.class, datawaveUserService);
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
        
        userDN = SubjectIssuerDNPair.of(testUserCert.getSubjectDN().getName(), testUserCert.getIssuerDN().getName());
        DatawaveUser defaultUser = new DatawaveUser(userDN, UserType.USER, null, null, null, System.currentTimeMillis());
        defaultPrincipal = new DatawavePrincipal(Lists.newArrayList(defaultUser));
    }
    
    @Test
    public void testValidLogin() throws Exception {
        DatawaveCredential datawaveCredential = new DatawaveCredential(testUserCert, null, null);
        callbackHandler.name = datawaveCredential.getUserName();
        callbackHandler.credential = datawaveCredential;
        
        expect(securityDomain.getKeyStore()).andReturn(keystore);
        expect(securityDomain.getTrustStore()).andReturn(truststore);
        expect(datawaveUserService.lookup(datawaveCredential.getEntities())).andReturn(defaultPrincipal.getProxiedUsers());
        
        replayAll();
        
        boolean success = datawaveLoginModule.login();
        assertTrue("Login did not succeed.", success);
        
        verifyAll();
    }
    
    @Test
    public void testGetRoleSets() throws Exception {
        DatawaveCredential datawaveCredential = new DatawaveCredential(testUserCert, null, null);
        callbackHandler.name = datawaveCredential.getUserName();
        callbackHandler.credential = datawaveCredential;
        
        String[] expectedRoles = new String[] {"Role1", "Role2", "Role3"};
        
        DatawaveUser user = new DatawaveUser(userDN, UserType.USER, Arrays.asList("a", "b", "c"), Arrays.asList(expectedRoles), null,
                        System.currentTimeMillis());
        DatawavePrincipal expected = new DatawavePrincipal(Lists.newArrayList(user));
        
        expect(securityDomain.getKeyStore()).andReturn(keystore);
        expect(securityDomain.getTrustStore()).andReturn(truststore);
        expect(datawaveUserService.lookup(datawaveCredential.getEntities())).andReturn(expected.getProxiedUsers());
        
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
        assertEquals(3, rolesList.size());
        assertArrayEquals(expectedRoles, rolesList.toArray());
        
        SimpleGroup callerPrincipal = (SimpleGroup) roleSets[1];
        assertEquals("CallerPrincipal", callerPrincipal.getName());
        Enumeration<Principal> members = callerPrincipal.members();
        assertTrue("CallerPrincipal group has no members", members.hasMoreElements());
        Principal p = members.nextElement();
        assertEquals(expected, p);
        assertFalse("CallerPrincipal group has too many members", members.hasMoreElements());
        
        verifyAll();
    }
    
    @Test(expected = AccountLockedException.class)
    public void testBlacklistedUser() throws Exception {
        DatawaveCredential datawaveCredential = new DatawaveCredential(testUserCert, null, null);
        callbackHandler.name = datawaveCredential.getUserName();
        callbackHandler.credential = datawaveCredential;
        
        List<String> roles = Collections.singletonList(BLACKLIST_ROLE);
        DatawaveUser user = new DatawaveUser(userDN, UserType.USER, null, roles, null, System.currentTimeMillis());
        DatawavePrincipal expected = new DatawavePrincipal(Lists.newArrayList(user));
        
        expect(securityDomain.getKeyStore()).andReturn(keystore);
        expect(securityDomain.getTrustStore()).andReturn(truststore);
        expect(datawaveUserService.lookup(datawaveCredential.getEntities())).andReturn(expected.getProxiedUsers());
        
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
        SubjectIssuerDNPair server1 = SubjectIssuerDNPair.of(serverDN, issuerDN);
        String otherServerDN = DnUtils.normalizeDN("CN=otherServer.example.com, OU=iamnotaperson, OU=acme");
        SubjectIssuerDNPair server2 = SubjectIssuerDNPair.of(otherServerDN, issuerDN);
        String proxiedSubjects = "<" + serverDN + "><" + otherServerDN + "><" + userDN.subjectDN() + ">";
        String proxiedIssuers = "<" + issuerDN + "><" + issuerDN + "><" + userDN.issuerDN() + ">";
        DatawaveCredential datawaveCredential = new DatawaveCredential(testServerCert, proxiedSubjects, proxiedIssuers);
        callbackHandler.name = datawaveCredential.getUserName();
        callbackHandler.credential = datawaveCredential;
        
        List<String> blacklistRoles = Arrays.asList(BLACKLIST_ROLE, "TEST_ROLE");
        List<String> otherRoles = Collections.singletonList("TEST_ROLE");
        
        DatawaveUser user = new DatawaveUser(userDN, UserType.USER, null, otherRoles, null, System.currentTimeMillis());
        DatawaveUser s1 = new DatawaveUser(server1, UserType.SERVER, null, otherRoles, null, System.currentTimeMillis());
        DatawaveUser s2 = new DatawaveUser(server2, UserType.SERVER, null, blacklistRoles, null, System.currentTimeMillis());
        
        DatawavePrincipal expected = new DatawavePrincipal(Lists.newArrayList(user, s1, s2));
        
        expect(securityDomain.getKeyStore()).andReturn(serverKeystore);
        expect(securityDomain.getTrustStore()).andReturn(truststore);
        expect(datawaveUserService.lookup(datawaveCredential.getEntities())).andReturn(expected.getProxiedUsers());
        
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
        SubjectIssuerDNPair server1 = SubjectIssuerDNPair.of(serverDN, issuerDN);
        String otherServerDN = DnUtils.normalizeDN("CN=otherServer.example.com, OU=iamnotaperson, OU=acme");
        SubjectIssuerDNPair server2 = SubjectIssuerDNPair.of(otherServerDN, issuerDN);
        String proxiedSubjects = "<" + serverDN + "><" + otherServerDN + "><" + userDN.subjectDN() + ">";
        String proxiedIssuers = "<" + issuerDN + "><" + issuerDN + "><" + userDN.issuerDN() + ">";
        DatawaveCredential datawaveCredential = new DatawaveCredential(testServerCert, proxiedSubjects, proxiedIssuers);
        callbackHandler.name = datawaveCredential.getUserName();
        callbackHandler.credential = datawaveCredential;
        
        DatawaveUser s1 = new DatawaveUser(server1, UserType.SERVER, null, null, null, System.currentTimeMillis());
        DatawaveUser s2 = new DatawaveUser(server2, UserType.SERVER, null, null, null, System.currentTimeMillis());
        DatawavePrincipal expected = new DatawavePrincipal(Lists.newArrayList(defaultPrincipal.getPrimaryUser(), s1, s2));
        
        expect(securityDomain.getKeyStore()).andReturn(serverKeystore);
        expect(securityDomain.getTrustStore()).andReturn(truststore);
        expect(datawaveUserService.lookup(datawaveCredential.getEntities())).andReturn(expected.getProxiedUsers());
        
        replayAll();
        
        boolean success = datawaveLoginModule.login();
        assertTrue("Login did not succeed.", success);
        assertEquals(userDN, expected.getUserDN());
        
        verifyAll();
    }
    
    @Test(expected = FailedLoginException.class)
    public void testInvalidLoginCertIssuerDenied() throws Exception {
        MockDatawaveCertVerifier.issuerSupported = false;
        DatawaveCredential datawaveCredential = new DatawaveCredential(testUserCert, null, null);
        callbackHandler.name = datawaveCredential.getUserName();
        callbackHandler.credential = datawaveCredential;
        
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
        DatawaveCredential datawaveCredential = new DatawaveCredential(testUserCert, null, null);
        callbackHandler.name = datawaveCredential.getUserName();
        callbackHandler.credential = datawaveCredential;
        
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
        DatawaveCredential datawaveCredential = new DatawaveCredential(testUserCert, null, null);
        callbackHandler.name = datawaveCredential.getUserName();
        callbackHandler.credential = datawaveCredential;
        
        expect(securityDomain.getKeyStore()).andReturn(keystore);
        expect(securityDomain.getTrustStore()).andReturn(truststore);
        expect(datawaveUserService.lookup(datawaveCredential.getEntities())).andThrow(new AuthorizationException("Unable to authenticate"));
        
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
