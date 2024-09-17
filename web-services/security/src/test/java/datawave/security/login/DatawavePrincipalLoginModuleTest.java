package datawave.security.login;

import static org.easymock.EasyMock.expect;
import static org.easymock.MockType.STRICT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.Socket;
import java.net.UnknownHostException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.acl.Group;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import javax.net.ssl.KeyManager;
import javax.net.ssl.X509KeyManager;
import javax.security.auth.Subject;
import javax.security.auth.login.AccountLockedException;
import javax.security.auth.login.CredentialException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;

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

import com.google.common.collect.Lists;

import datawave.security.auth.DatawaveCredential;
import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.authorization.DatawaveUserService;
import datawave.security.authorization.JWTTokenHandler;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnUtils;
import datawave.security.util.MockCallbackHandler;
import datawave.security.util.MockDatawaveCertVerifier;

@RunWith(EasyMockRunner.class)
public class DatawavePrincipalLoginModuleTest extends EasyMockSupport {
    private static final String DISALLOWLIST_ROLE = "DISALLOWLIST_ROLE";
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
        System.setProperty(DnUtils.NPE_OU_PROPERTY, "iamnotaperson");
        MockDatawaveCertVerifier.issuerSupported = true;
        MockDatawaveCertVerifier.verify = true;

        callbackHandler = new MockCallbackHandler("Username: ", "Credentials: ");

        truststore = KeyStore.getInstance("PKCS12");
        truststore.load(getClass().getResourceAsStream("/ca.pkcs12"), "secret".toCharArray());
        keystore = KeyStore.getInstance("PKCS12");
        keystore.load(getClass().getResourceAsStream("/testUser.pkcs12"), "secret".toCharArray());
        serverKeystore = KeyStore.getInstance("PKCS12");
        serverKeystore.load(getClass().getResourceAsStream("/testServer.pkcs12"), "secret".toCharArray());
        testUserCert = (X509Certificate) keystore.getCertificate("testuser");
        testServerCert = (X509Certificate) serverKeystore.getCertificate("testserver");

        KeyManager keyManager = new X509KeyManager() {
            @Override
            public String[] getClientAliases(String s, Principal[] principals) {
                return new String[0];
            }

            @Override
            public String chooseClientAlias(String[] strings, Principal[] principals, Socket socket) {
                return null;
            }

            @Override
            public String[] getServerAliases(String s, Principal[] principals) {
                return new String[0];
            }

            @Override
            public String chooseServerAlias(String s, Principal[] principals, Socket socket) {
                return null;
            }

            @Override
            public X509Certificate[] getCertificateChain(String s) {
                try {
                    return Arrays.stream(keystore.getCertificateChain(s)).map(X509Certificate.class::cast).toArray(X509Certificate[]::new);
                } catch (KeyStoreException e) {
                    fail(e.getMessage());
                    return null;
                }
            }

            @Override
            public PrivateKey getPrivateKey(String s) {
                try {
                    return (PrivateKey) keystore.getKey(s, "secret".toCharArray());
                } catch (KeyStoreException | NoSuchAlgorithmException | UnrecoverableKeyException e) {
                    fail(e.getMessage());
                    return null;
                }
            }
        };

        expect(securityDomain.getKeyStore()).andReturn(keystore);
        expect(securityDomain.getKeyManagers()).andReturn(new KeyManager[] {keyManager});

        replayAll();

        HashMap<String,String> sharedState = new HashMap<>();
        HashMap<String,String> options = new HashMap<>();
        options.put("principalClass", "datawave.security.authorization.DatawavePrincipal");
        options.put("verifier", MockDatawaveCertVerifier.class.getName());
        options.put("passwordStacking", "useFirstPass");
        options.put("ocspLevel", "required");
        options.put("disallowlistUserRole", DISALLOWLIST_ROLE);
        options.put("requiredRoles", "AuthorizedUser:AuthorizedServer:AuthorizedQueryServer:OtherRequiredRole");
        options.put("directRoles", "AuthorizedQueryServer:AuthorizedServer");

        Whitebox.setInternalState(datawaveLoginModule, DatawaveUserService.class, datawaveUserService);
        Whitebox.setInternalState(datawaveLoginModule, JSSESecurityDomain.class, securityDomain);
        datawaveLoginModule.initialize(new Subject(), callbackHandler, sharedState, options);

        verifyAll();
        resetAll();

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

    @Test
    public void testGetRoleSetsLeavesRequiredRoles() throws Exception {
        // Proxied entities has the original user DN, plus it came through a server and
        // the request is being made by a second server. Make sure that the resulting
        // principal has all 3 server DNs in its list, and the user DN is not one of the
        // server DNs.
        String issuerDN = DnUtils.normalizeDN(testServerCert.getIssuerDN().getName());
        String serverDN = DnUtils.normalizeDN("CN=testServer.example.com, OU=iamnotaperson, OU=acme");
        SubjectIssuerDNPair server1 = SubjectIssuerDNPair.of(serverDN, issuerDN);
        String otherServerDN = DnUtils.normalizeDN("CN=otherServer.example.com, OU=iamnotaperson, OU=acme");
        SubjectIssuerDNPair server2 = SubjectIssuerDNPair.of(otherServerDN, issuerDN);
        String proxiedSubjects = "<" + userDN.subjectDN() + "><" + otherServerDN + ">";
        String proxiedIssuers = "<" + userDN.issuerDN() + "><" + issuerDN + ">";
        DatawaveCredential datawaveCredential = new DatawaveCredential(testServerCert, proxiedSubjects, proxiedIssuers);
        callbackHandler.name = datawaveCredential.getUserName();
        callbackHandler.credential = datawaveCredential;

        List<String> userRoles = Arrays.asList("Role1", "AuthorizedUser");
        List<String> s1Roles = Arrays.asList("Role2", "AuthorizedServer");
        List<String> s2Roles = Arrays.asList("Role3", "OtherRequiredRole");

        DatawaveUser user = new DatawaveUser(userDN, UserType.USER, null, userRoles, null, System.currentTimeMillis());
        DatawaveUser s1 = new DatawaveUser(server1, UserType.SERVER, null, s1Roles, null, System.currentTimeMillis());
        DatawaveUser s2 = new DatawaveUser(server2, UserType.SERVER, null, s2Roles, null, System.currentTimeMillis());
        DatawavePrincipal expected = new DatawavePrincipal(Lists.newArrayList(user, s2, s1));

        expect(securityDomain.getKeyStore()).andReturn(serverKeystore);
        expect(securityDomain.getTrustStore()).andReturn(truststore);
        expect(datawaveUserService.lookup(datawaveCredential.getEntities())).andReturn(expected.getProxiedUsers());

        replayAll();

        boolean success = datawaveLoginModule.login();
        assertTrue("Login did not succeed.", success);
        assertEquals(userDN, expected.getUserDN());

        Group[] roleSets = datawaveLoginModule.getRoleSets();
        assertEquals(2, roleSets.length);
        assertEquals("Roles", roleSets[0].getName());
        List<String> groupSetRoles = Collections.list(roleSets[0].members()).stream().map(Principal::getName).collect(Collectors.toList());
        assertEquals(Lists.newArrayList("Role1", "AuthorizedUser"), groupSetRoles);

        verifyAll();
    }

    @Test(expected = FailedLoginException.class)
    public void testProxiedEntitiesLoginNoRole() throws Exception {
        // Call Chain is U -> S1 -> S2. S2 will have no role. This test case tests
        // the case of no role for the terminal service. This should fail with
        // CredentialException thrown.
        String issuerDN = DnUtils.normalizeDN(testServerCert.getIssuerDN().getName());
        String serverDN = DnUtils.normalizeDN("CN=testServer.example.com, OU=iamnotaperson, OU=acme");
        SubjectIssuerDNPair server1 = SubjectIssuerDNPair.of(serverDN, issuerDN);
        String otherServerDN = DnUtils.normalizeDN("CN=otherServer.example.com, OU=iamnotaperson, OU=acme");
        SubjectIssuerDNPair server2 = SubjectIssuerDNPair.of(otherServerDN, issuerDN);
        String proxiedSubjects = "<" + userDN.subjectDN() + "><" + otherServerDN + ">";
        String proxiedIssuers = "<" + userDN.issuerDN() + "><" + issuerDN + ">";
        DatawaveCredential datawaveCredential = new DatawaveCredential(testServerCert, proxiedSubjects, proxiedIssuers);
        callbackHandler.name = datawaveCredential.getUserName();
        callbackHandler.credential = datawaveCredential;

        List<String> s1Roles = Arrays.asList("AuthorizedServer");

        DatawaveUser s1 = new DatawaveUser(server1, UserType.SERVER, null, s1Roles, null, System.currentTimeMillis());
        DatawaveUser s2 = new DatawaveUser(server2, UserType.SERVER, null, null, null, System.currentTimeMillis());
        DatawavePrincipal expected = new DatawavePrincipal(Lists.newArrayList(defaultPrincipal.getPrimaryUser(), s1, s2));

        expect(securityDomain.getKeyStore()).andReturn(serverKeystore);
        expect(securityDomain.getTrustStore()).andReturn(truststore);
        expect(datawaveUserService.lookup(datawaveCredential.getEntities())).andReturn(expected.getProxiedUsers());

        replayAll();
        boolean success = datawaveLoginModule.login();
        assertFalse("Login should fail, but succeeded.", success);
        assertEquals(userDN, expected.getUserDN());

        verifyAll();
    }

    @Test(expected = FailedLoginException.class)
    public void testDirectRolesFailServer() throws Exception {
        /**
         * Chain is User -> S1 -> S2. S2 is terminal server. Verified that s2 does not have the appropriate authorized role for terminal server (directRole).
         * This will fail the check in the #DatawavePrincipalLoginModule.login() This will prevent the chain from accessing any endpoint which requires
         * authorized roles
         */

        String issuerDN = DnUtils.normalizeDN(testServerCert.getIssuerDN().getName());
        String serverDN = DnUtils.normalizeDN("CN=testServer.example.com, OU=iamnotaperson, OU=acme");
        SubjectIssuerDNPair server1 = SubjectIssuerDNPair.of(serverDN, issuerDN);
        String otherServerDN = DnUtils.normalizeDN("CN=otherServer.example.com, OU=iamnotaperson, OU=acme");
        SubjectIssuerDNPair server2 = SubjectIssuerDNPair.of(otherServerDN, issuerDN);
        String proxiedSubjects = "<" + userDN.subjectDN() + "><" + otherServerDN + ">";
        String proxiedIssuers = "<" + userDN.issuerDN() + "><" + issuerDN + ">";
        DatawaveCredential datawaveCredential = new DatawaveCredential(testServerCert, proxiedSubjects, proxiedIssuers);
        callbackHandler.name = datawaveCredential.getUserName();
        callbackHandler.credential = datawaveCredential;

        List<String> userRoles = Arrays.asList("Role1", "AuthorizedUser");
        List<String> s1Roles = Arrays.asList("Role2", "AuthorizedServer");
        List<String> s2Roles = Arrays.asList("Role3", "OtherRequiredRole");

        DatawaveUser user = new DatawaveUser(userDN, UserType.USER, null, userRoles, null, System.currentTimeMillis());
        DatawaveUser s1 = new DatawaveUser(server1, UserType.SERVER, null, s1Roles, null, System.currentTimeMillis());
        DatawaveUser s2 = new DatawaveUser(server2, UserType.SERVER, null, s2Roles, null, System.currentTimeMillis());

        /**
         * s2 has OtherRequiredRole is an auhtorizedRole, but not a directRole so this will fail the check in #DatawavePrincipalLoginModule.login() so chain
         * User -> S1 -> S2 will fail
         */

        DatawavePrincipal expected = new DatawavePrincipal(Lists.newArrayList(user, s1, s2));

        expect(securityDomain.getKeyStore()).andReturn(serverKeystore);
        expect(securityDomain.getTrustStore()).andReturn(truststore);
        expect(datawaveUserService.lookup(datawaveCredential.getEntities())).andReturn(expected.getProxiedUsers());

        replayAll();

        boolean success = datawaveLoginModule.login();
        assertFalse("Login should fail, but succeeded.", success);

        verifyAll();
    }

    @Test
    public void testDirectRolesSuccesServer() throws Exception {
        /**
         * Chain is User -> S1 -> S2. S2 is terminal server. Verified that s2 does have the appropriate authorized role for terminal server. This will pass the
         * check in #DatawavePrincipalLoginModule.login(). This will allow the chain from accessing any endpoint which requires authorized roles
         */

        String issuerDN = DnUtils.normalizeDN(testServerCert.getIssuerDN().getName());
        String serverDN = DnUtils.normalizeDN("CN=testServer.example.com, OU=iamnotaperson, OU=acme");
        SubjectIssuerDNPair server1 = SubjectIssuerDNPair.of(serverDN, issuerDN);
        String otherServerDN = DnUtils.normalizeDN("CN=otherServer.example.com, OU=iamnotaperson, OU=acme");
        SubjectIssuerDNPair server2 = SubjectIssuerDNPair.of(otherServerDN, issuerDN);
        String proxiedSubjects = "<" + userDN.subjectDN() + "><" + otherServerDN + ">";
        String proxiedIssuers = "<" + userDN.issuerDN() + "><" + issuerDN + ">";
        DatawaveCredential datawaveCredential = new DatawaveCredential(testServerCert, proxiedSubjects, proxiedIssuers);
        callbackHandler.name = datawaveCredential.getUserName();
        callbackHandler.credential = datawaveCredential;

        List<String> userRoles = Arrays.asList("Role1", "AuthorizedUser");
        List<String> s1Roles = Arrays.asList("Role3", "OtherRequiredRole");
        List<String> s2Roles = Arrays.asList("Role2", "AuthorizedServer");

        DatawaveUser user = new DatawaveUser(userDN, UserType.USER, null, userRoles, null, System.currentTimeMillis());
        DatawaveUser s1 = new DatawaveUser(server1, UserType.SERVER, null, s1Roles, null, System.currentTimeMillis());
        DatawaveUser s2 = new DatawaveUser(server2, UserType.SERVER, null, s2Roles, null, System.currentTimeMillis());

        /**
         * s2 has AuthorizedServer role which is a directRole s1 has OtherRequiredRole which is not a directRole. This is the chain we want to make sure passes.
         * so this will pass the check in #DatawavePrincipalLoginModule.login() so chain User -> S1 -> S2 will pass
         */

        DatawavePrincipal expected = new DatawavePrincipal(Lists.newArrayList(user, s1, s2));

        expect(securityDomain.getKeyStore()).andReturn(serverKeystore);
        expect(securityDomain.getTrustStore()).andReturn(truststore);
        expect(datawaveUserService.lookup(datawaveCredential.getEntities())).andReturn(expected.getProxiedUsers());

        replayAll();

        boolean success = datawaveLoginModule.login();
        assertTrue("Login did not succeed.", success);

        verifyAll();
    }

    @Test
    public void testDirectRolesSuccesUser() throws Exception {
        /**
         * Chain is just User. This will not get hit by the terminal server check as it only runs on UserType.SERVER
         *
         */

        String issuerDN = DnUtils.normalizeDN(testServerCert.getIssuerDN().getName());
        String serverDN = DnUtils.normalizeDN("CN=testServer.example.com, OU=iamnotaperson, OU=acme");
        SubjectIssuerDNPair server1 = SubjectIssuerDNPair.of(serverDN, issuerDN);
        String otherServerDN = DnUtils.normalizeDN("CN=otherServer.example.com, OU=iamnotaperson, OU=acme");
        SubjectIssuerDNPair server2 = SubjectIssuerDNPair.of(otherServerDN, issuerDN);
        String proxiedSubjects = "<" + userDN.subjectDN() + "><" + otherServerDN + ">";
        String proxiedIssuers = "<" + userDN.issuerDN() + "><" + issuerDN + ">";
        DatawaveCredential datawaveCredential = new DatawaveCredential(testServerCert, proxiedSubjects, proxiedIssuers);
        callbackHandler.name = datawaveCredential.getUserName();
        callbackHandler.credential = datawaveCredential;

        List<String> userRoles = Arrays.asList("Role1", "AuthorizedUser");

        DatawaveUser user = new DatawaveUser(userDN, UserType.USER, null, userRoles, null, System.currentTimeMillis());

        DatawavePrincipal expected = new DatawavePrincipal(Lists.newArrayList(user));

        expect(securityDomain.getKeyStore()).andReturn(serverKeystore);
        expect(securityDomain.getTrustStore()).andReturn(truststore);
        expect(datawaveUserService.lookup(datawaveCredential.getEntities())).andReturn(expected.getProxiedUsers());

        replayAll();

        boolean success = datawaveLoginModule.login();
        assertTrue("Login did not succeed.", success);

        verifyAll();
    }

    @Test
    public void testGetRoleSetsFiltersRequiredRoles() throws Exception {
        // Proxied entities has the original user DN, plus it came through a server and
        // the request is being made by a second server. Make sure that the resulting
        // principal has all 3 server DNs in its list, and the user DN is not one of the
        // server DNs.
        String issuerDN = DnUtils.normalizeDN(testServerCert.getIssuerDN().getName());
        String serverDN = DnUtils.normalizeDN("CN=testServer.example.com, OU=iamnotaperson, OU=acme");
        SubjectIssuerDNPair server1 = SubjectIssuerDNPair.of(serverDN, issuerDN);
        String otherServerDN = DnUtils.normalizeDN("CN=otherServer.example.com, OU=iamnotaperson, OU=acme");
        SubjectIssuerDNPair server2 = SubjectIssuerDNPair.of(otherServerDN, issuerDN);
        String proxiedSubjects = "<" + userDN.subjectDN() + "><" + otherServerDN + ">";
        String proxiedIssuers = "<" + userDN.issuerDN() + "><" + issuerDN + ">";
        DatawaveCredential datawaveCredential = new DatawaveCredential(testServerCert, proxiedSubjects, proxiedIssuers);
        callbackHandler.name = datawaveCredential.getUserName();
        callbackHandler.credential = datawaveCredential;

        List<String> userRoles = Arrays.asList("Role1", "AuthorizedUser");
        List<String> s1Roles = Arrays.asList("Role3", "AuthorizedServer");
        List<String> s2Roles = Arrays.asList("Role2");

        DatawaveUser user = new DatawaveUser(userDN, UserType.USER, null, userRoles, null, System.currentTimeMillis());
        DatawaveUser s1 = new DatawaveUser(server1, UserType.SERVER, null, s1Roles, null, System.currentTimeMillis());
        DatawaveUser s2 = new DatawaveUser(server2, UserType.SERVER, null, s2Roles, null, System.currentTimeMillis());
        /**
         * changed order of roles for the servers so this test will pass the directRole check
         */
        DatawavePrincipal expected = new DatawavePrincipal(Lists.newArrayList(user, s2, s1));

        expect(securityDomain.getKeyStore()).andReturn(serverKeystore);
        expect(securityDomain.getTrustStore()).andReturn(truststore);
        expect(datawaveUserService.lookup(datawaveCredential.getEntities())).andReturn(expected.getProxiedUsers());

        replayAll();

        boolean success = datawaveLoginModule.login();
        assertTrue("Login did not succeed.", success);
        assertEquals(userDN, expected.getUserDN());

        Group[] roleSets = datawaveLoginModule.getRoleSets();
        assertEquals(2, roleSets.length);
        assertEquals("Roles", roleSets[0].getName());
        List<String> groupSetRoles = Collections.list(roleSets[0].members()).stream().map(Principal::getName).collect(Collectors.toList());
        assertEquals(Lists.newArrayList("Role1"), groupSetRoles);

        verifyAll();
    }

    @Test(expected = AccountLockedException.class)
    public void testDisallowlistedUser() throws Exception {
        DatawaveCredential datawaveCredential = new DatawaveCredential(testUserCert, null, null);
        callbackHandler.name = datawaveCredential.getUserName();
        callbackHandler.credential = datawaveCredential;

        List<String> roles = Collections.singletonList(DISALLOWLIST_ROLE);
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
    public void testDisallowlistedProxiedUser() throws Exception {
        // Proxied entities has the original user DN, plus it came through a server and
        // the request is being made by a second server. Make sure that the resulting
        // principal has all 3 server DNs in its list, and the user DN is not one of the
        // server DNs.
        String issuerDN = DnUtils.normalizeDN(testServerCert.getIssuerDN().getName());
        String serverDN = DnUtils.normalizeDN("CN=testServer.example.com, OU=iamnotaperson, OU=acme");
        SubjectIssuerDNPair server1 = SubjectIssuerDNPair.of(serverDN, issuerDN);
        String otherServerDN = DnUtils.normalizeDN("CN=otherServer.example.com, OU=iamnotaperson, OU=acme");
        SubjectIssuerDNPair server2 = SubjectIssuerDNPair.of(otherServerDN, issuerDN);
        String proxiedSubjects = "<" + userDN.subjectDN() + "><" + otherServerDN + ">";
        String proxiedIssuers = "<" + userDN.issuerDN() + "><" + issuerDN + ">";
        DatawaveCredential datawaveCredential = new DatawaveCredential(testServerCert, proxiedSubjects, proxiedIssuers);
        callbackHandler.name = datawaveCredential.getUserName();
        callbackHandler.credential = datawaveCredential;

        List<String> disallowlistRoles = Arrays.asList(DISALLOWLIST_ROLE, "TEST_ROLE");
        List<String> otherRoles = Collections.singletonList("TEST_ROLE");

        DatawaveUser user = new DatawaveUser(userDN, UserType.USER, null, otherRoles, null, System.currentTimeMillis());
        DatawaveUser s1 = new DatawaveUser(server1, UserType.SERVER, null, otherRoles, null, System.currentTimeMillis());
        DatawaveUser s2 = new DatawaveUser(server2, UserType.SERVER, null, disallowlistRoles, null, System.currentTimeMillis());

        DatawavePrincipal expected = new DatawavePrincipal(Lists.newArrayList(user, s2, s1));

        expect(securityDomain.getKeyStore()).andReturn(serverKeystore);
        expect(securityDomain.getTrustStore()).andReturn(truststore);
        expect(datawaveUserService.lookup(datawaveCredential.getEntities())).andReturn(expected.getProxiedUsers());

        replayAll();

        boolean success = datawaveLoginModule.login();
        assertFalse("Login should not have succeeded.", success);

        verifyAll();
    }

    @Test
    public void testAuthorizationExceptionOnLookup() throws Exception {
        // Ensure that an AuthorizationException from the DatawaveUserService results
        // in a LoginException being thrown from DatawavePrincipalLOginModule.login()
        String issuerDN = DnUtils.normalizeDN(testServerCert.getIssuerDN().getName());
        String otherServerDN = DnUtils.normalizeDN("CN=otherServer.example.com, OU=iamnotaperson, OU=acme");
        String proxiedSubjects = "<" + userDN.subjectDN() + "><" + otherServerDN + ">";
        String proxiedIssuers = "<" + userDN.issuerDN() + "><" + issuerDN + ">";
        DatawaveCredential datawaveCredential = new DatawaveCredential(testServerCert, proxiedSubjects, proxiedIssuers);
        callbackHandler.name = datawaveCredential.getUserName();
        callbackHandler.credential = datawaveCredential;

        expect(securityDomain.getKeyStore()).andReturn(serverKeystore);
        expect(securityDomain.getTrustStore()).andReturn(truststore);
        expect(datawaveUserService.lookup(datawaveCredential.getEntities())).andThrow(new AuthorizationException());

        replayAll();

        try {
            datawaveLoginModule.login();
            fail("Login should not have succeeded");
        } catch (Exception e) {
            // this type of check is used because there are many subclasses of LoginException
            // Using a @Test(expected = LoginException.class) would succeed if any of these were caught
            assertTrue(e.getClass().equals(LoginException.class));
        }

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
        String proxiedSubjects = "<" + userDN.subjectDN() + "><" + otherServerDN + ">";
        String proxiedIssuers = "<" + userDN.issuerDN() + "><" + issuerDN + ">";
        DatawaveCredential datawaveCredential = new DatawaveCredential(testServerCert, proxiedSubjects, proxiedIssuers);
        callbackHandler.name = datawaveCredential.getUserName();
        callbackHandler.credential = datawaveCredential;

        /**
         * Need to add an directRole for s1 to allow the login check to pass
         */
        List<String> s1Roles = Arrays.asList("AuthorizedServer");

        DatawaveUser s1 = new DatawaveUser(server1, UserType.SERVER, null, s1Roles, null, System.currentTimeMillis());
        DatawaveUser s2 = new DatawaveUser(server2, UserType.SERVER, null, null, null, System.currentTimeMillis());
        DatawavePrincipal expected = new DatawavePrincipal(Lists.newArrayList(defaultPrincipal.getPrimaryUser(), s2, s1));

        expect(securityDomain.getKeyStore()).andReturn(serverKeystore);
        expect(securityDomain.getTrustStore()).andReturn(truststore);
        expect(datawaveUserService.lookup(datawaveCredential.getEntities())).andReturn(expected.getProxiedUsers());

        replayAll();

        boolean success = datawaveLoginModule.login();
        assertTrue("Login did not succeed.", success);
        assertEquals(userDN, expected.getUserDN());

        verifyAll();
    }

    @Test
    public void testJWTLogin() throws Exception {
        Whitebox.setInternalState(datawaveLoginModule, "jwtHeaderLogin", true);
        JWTTokenHandler tokenHandler = Whitebox.getInternalState(datawaveLoginModule, JWTTokenHandler.class);

        // Proxied entities has the original user DN, plus it came through a server and
        // the request is being made by a second server. Make sure that the resulting
        // principal has all 3 server DNs in its list, and the user DN is not one of the
        // server DNs.
        String issuerDN = DnUtils.normalizeDN(testServerCert.getIssuerDN().getName());
        String serverDN = DnUtils.normalizeDN("CN=testServer.example.com, OU=iamnotaperson, OU=acme");
        SubjectIssuerDNPair server1 = SubjectIssuerDNPair.of(serverDN, issuerDN);
        String otherServerDN = DnUtils.normalizeDN("CN=otherServer.example.com, OU=iamnotaperson, OU=acme");
        SubjectIssuerDNPair server2 = SubjectIssuerDNPair.of(otherServerDN, issuerDN);

        DatawaveUser s1 = new DatawaveUser(server1, UserType.SERVER, null, null, null, System.currentTimeMillis());
        DatawaveUser s2 = new DatawaveUser(server2, UserType.SERVER, null, null, null, System.currentTimeMillis());
        DatawavePrincipal expected = new DatawavePrincipal(Lists.newArrayList(s1, s2, defaultPrincipal.getPrimaryUser()));

        String token = tokenHandler.createTokenFromUsers(expected.getName(), expected.getProxiedUsers());
        DatawaveCredential datawaveCredential = new DatawaveCredential(token);
        callbackHandler.name = datawaveCredential.getUserName();
        callbackHandler.credential = datawaveCredential;

        replayAll();

        boolean success = datawaveLoginModule.login();
        assertTrue("Login did not succeed.", success);
        assertEquals(userDN, expected.getUserDN());

        verifyAll();
    }

    @Test(expected = CredentialException.class)
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

    @Test(expected = CredentialException.class)
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

    @Test
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
            fail("Login should not have succeeded");
        } catch (LoginException e) {
            // this type of check is used because there are many subclasses of LoginException
            // Using a @Test(expected = LoginException.class) would succeed if any of these were caught
            assertTrue(e.getClass().equals(LoginException.class));
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
