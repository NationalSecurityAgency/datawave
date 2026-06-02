package datawave.security.login;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;

import java.net.Socket;
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

import org.jboss.security.JSSESecurityDomain;
import org.jboss.security.SimpleGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

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

@ExtendWith(MockitoExtension.class)
public class DatawavePrincipalLoginModuleTest {

    private static final String DISALLOWLIST_ROLE = "DISALLOWLIST_ROLE";

    @InjectMocks
    private DatawavePrincipalLoginModule datawaveLoginModule = new TestDatawavePrincipalLoginModule();

    @Mock
    private JSSESecurityDomain securityDomain;

    @Mock
    private DatawaveUserService datawaveUserService;
    private MockCallbackHandler callbackHandler;

    private KeyStore truststore;
    private KeyStore keystore;
    private KeyStore serverKeystore;
    private X509Certificate testUserCert;
    private X509Certificate testServerCert;

    private SubjectIssuerDNPair userDN;
    private DatawavePrincipal defaultPrincipal;

    @BeforeEach
    public void beforeEach() throws Exception {
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

        when(securityDomain.getKeyStore()).thenReturn(keystore);
        when(securityDomain.getKeyManagers()).thenReturn(new KeyManager[] {keyManager});

        HashMap<String,String> sharedState = new HashMap<>();
        HashMap<String,String> options = new HashMap<>();
        options.put("principalClass", "datawave.security.authorization.DatawavePrincipal");
        options.put("verifier", MockDatawaveCertVerifier.class.getName());
        options.put("passwordStacking", "useFirstPass");
        options.put("ocspLevel", "required");
        options.put("disallowlistUserRole", DISALLOWLIST_ROLE);
        options.put("requiredRoles", "AuthorizedUser:AuthorizedServer:AuthorizedQueryServer:OtherRequiredRole");
        options.put("directRoles", "AuthorizedQueryServer:AuthorizedServer");

        ReflectionTestUtils.setField(datawaveLoginModule, "datawaveUserService", datawaveUserService);
        ReflectionTestUtils.setField(datawaveLoginModule, "domain", securityDomain);
        datawaveLoginModule.initialize(new Subject(), callbackHandler, sharedState, options);

        userDN = SubjectIssuerDNPair.of(testUserCert.getSubjectDN().getName(), testUserCert.getIssuerDN().getName());
        DatawaveUser defaultUser = new DatawaveUser(userDN, UserType.USER, null, null, null, System.currentTimeMillis());
        defaultPrincipal = new DatawavePrincipal(Lists.newArrayList(defaultUser));
    }

    @Test
    public void testValidLogin() throws Exception {
        DatawaveCredential datawaveCredential = new DatawaveCredential(testUserCert, null, null);
        callbackHandler.name = datawaveCredential.getUserName();
        callbackHandler.credential = datawaveCredential;

        when(securityDomain.getKeyStore()).thenReturn(keystore);
        when(securityDomain.getTrustStore()).thenReturn(truststore);
        when(datawaveUserService.lookup(datawaveCredential.getEntities())).thenReturn(defaultPrincipal.getProxiedUsers());

        boolean result = datawaveLoginModule.login();
        assertTrue(result, "Login did not succeed.");
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

        when(securityDomain.getKeyStore()).thenReturn(keystore);
        when(securityDomain.getTrustStore()).thenReturn(truststore);
        when(datawaveUserService.lookup(datawaveCredential.getEntities())).thenReturn(expected.getProxiedUsers());

        boolean result = datawaveLoginModule.login();
        assertTrue(result, "Login did not succeed.");

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
        assertTrue(members.hasMoreElements(), "CallerPrincipal group has no members");
        Principal p = members.nextElement();
        assertEquals(expected, p);
        assertFalse(members.hasMoreElements(), "CallerPrincipal group has too many members");
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

        when(securityDomain.getKeyStore()).thenReturn(serverKeystore);
        when(securityDomain.getTrustStore()).thenReturn(truststore);
        when(datawaveUserService.lookup(datawaveCredential.getEntities())).thenReturn(expected.getProxiedUsers());

        boolean result = datawaveLoginModule.login();
        assertTrue(result, "Login did not succeed.");
        assertEquals(userDN, expected.getUserDN());

        Group[] roleSets = datawaveLoginModule.getRoleSets();
        assertEquals(2, roleSets.length);
        assertEquals("Roles", roleSets[0].getName());
        List<String> groupSetRoles = Collections.list(roleSets[0].members()).stream().map(Principal::getName).collect(Collectors.toList());
        assertEquals(Lists.newArrayList("Role1", "AuthorizedUser"), groupSetRoles);
    }

    @Test
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

        List<String> s1Roles = List.of("AuthorizedServer");

        DatawaveUser s1 = new DatawaveUser(server1, UserType.SERVER, null, s1Roles, null, System.currentTimeMillis());
        DatawaveUser s2 = new DatawaveUser(server2, UserType.SERVER, null, null, null, System.currentTimeMillis());
        DatawavePrincipal expected = new DatawavePrincipal(Lists.newArrayList(defaultPrincipal.getPrimaryUser(), s1, s2));

        when(securityDomain.getKeyStore()).thenReturn(serverKeystore);
        when(securityDomain.getTrustStore()).thenReturn(truststore);
        when(datawaveUserService.lookup(datawaveCredential.getEntities())).thenReturn(expected.getProxiedUsers());

        assertThrows(FailedLoginException.class, datawaveLoginModule::login);
    }

    @Test
    public void testDirectRolesFailServer() throws Exception {
        /*
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

        /*
         * s2 has OtherRequiredRole is an authorizedRole, but not a directRole so this will fail the check in #DatawavePrincipalLoginModule.login() so chain
         * User -> S1 -> S2 will fail
         */

        DatawavePrincipal expected = new DatawavePrincipal(Lists.newArrayList(user, s1, s2));

        when(securityDomain.getKeyStore()).thenReturn(serverKeystore);
        when(securityDomain.getTrustStore()).thenReturn(truststore);
        when(datawaveUserService.lookup(datawaveCredential.getEntities())).thenReturn(expected.getProxiedUsers());

        assertThrows(FailedLoginException.class, datawaveLoginModule::login);
    }

    @Test
    public void testDirectRolesSuccessServer() throws Exception {
        /*
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

        /*
         * s2 has AuthorizedServer role which is a directRole s1 has OtherRequiredRole which is not a directRole. This is the chain we want to make sure passes.
         * so this will pass the check in #DatawavePrincipalLoginModule.login() so chain User -> S1 -> S2 will pass
         */

        DatawavePrincipal expected = new DatawavePrincipal(Lists.newArrayList(user, s1, s2));

        when(securityDomain.getKeyStore()).thenReturn(serverKeystore);
        when(securityDomain.getTrustStore()).thenReturn(truststore);
        when(datawaveUserService.lookup(datawaveCredential.getEntities())).thenReturn(expected.getProxiedUsers());

        boolean result = datawaveLoginModule.login();
        assertTrue(result, "Login did not succeed.");
    }

    @Test
    public void testDirectRolesSuccessUser() throws Exception {
        /*
         * Chain is just User. This will not get hit by the terminal server check as it only runs on UserType.SERVER
         */
        String issuerDN = DnUtils.normalizeDN(testServerCert.getIssuerDN().getName());
        String otherServerDN = DnUtils.normalizeDN("CN=otherServer.example.com, OU=iamnotaperson, OU=acme");
        String proxiedSubjects = "<" + userDN.subjectDN() + "><" + otherServerDN + ">";
        String proxiedIssuers = "<" + userDN.issuerDN() + "><" + issuerDN + ">";
        DatawaveCredential datawaveCredential = new DatawaveCredential(testServerCert, proxiedSubjects, proxiedIssuers);
        callbackHandler.name = datawaveCredential.getUserName();
        callbackHandler.credential = datawaveCredential;

        List<String> userRoles = Arrays.asList("Role1", "AuthorizedUser");

        DatawaveUser user = new DatawaveUser(userDN, UserType.USER, null, userRoles, null, System.currentTimeMillis());

        DatawavePrincipal expected = new DatawavePrincipal(Lists.newArrayList(user));

        when(securityDomain.getKeyStore()).thenReturn(serverKeystore);
        when(securityDomain.getTrustStore()).thenReturn(truststore);
        when(datawaveUserService.lookup(datawaveCredential.getEntities())).thenReturn(expected.getProxiedUsers());

        boolean result = datawaveLoginModule.login();
        assertTrue(result, "Login did not succeed.");
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
        /*
         * changed order of roles for the servers so this test will pass the directRole check
         */
        DatawavePrincipal expected = new DatawavePrincipal(Lists.newArrayList(user, s2, s1));

        when(securityDomain.getKeyStore()).thenReturn(serverKeystore);
        when(securityDomain.getTrustStore()).thenReturn(truststore);
        when(datawaveUserService.lookup(datawaveCredential.getEntities())).thenReturn(expected.getProxiedUsers());

        boolean result = datawaveLoginModule.login();
        assertTrue(result, "Login did not succeed.");
        assertEquals(userDN, expected.getUserDN());

        Group[] roleSets = datawaveLoginModule.getRoleSets();
        assertEquals(2, roleSets.length);
        assertEquals("Roles", roleSets[0].getName());
        List<String> groupSetRoles = Collections.list(roleSets[0].members()).stream().map(Principal::getName).collect(Collectors.toList());
        assertEquals(Lists.newArrayList("Role1"), groupSetRoles);
    }

    @Test
    public void testDisAllowlistedUser() throws Exception {
        DatawaveCredential datawaveCredential = new DatawaveCredential(testUserCert, null, null);
        callbackHandler.name = datawaveCredential.getUserName();
        callbackHandler.credential = datawaveCredential;

        List<String> roles = Collections.singletonList(DISALLOWLIST_ROLE);
        DatawaveUser user = new DatawaveUser(userDN, UserType.USER, null, roles, null, System.currentTimeMillis());
        DatawavePrincipal expected = new DatawavePrincipal(Lists.newArrayList(user));

        when(securityDomain.getKeyStore()).thenReturn(keystore);
        when(securityDomain.getTrustStore()).thenReturn(truststore);
        when(datawaveUserService.lookup(datawaveCredential.getEntities())).thenReturn(expected.getProxiedUsers());

        assertThrows(AccountLockedException.class, datawaveLoginModule::login);
    }

    @Test
    public void testDisAllowlistedProxiedUser() throws Exception {
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

        when(securityDomain.getKeyStore()).thenReturn(serverKeystore);
        when(securityDomain.getTrustStore()).thenReturn(truststore);
        when(datawaveUserService.lookup(datawaveCredential.getEntities())).thenReturn(expected.getProxiedUsers());

        assertThrows(AccountLockedException.class, datawaveLoginModule::login);
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

        when(securityDomain.getKeyStore()).thenReturn(serverKeystore);
        when(securityDomain.getTrustStore()).thenReturn(truststore);
        when(datawaveUserService.lookup(datawaveCredential.getEntities())).thenThrow(new AuthorizationException());

        // there are many subclasses of LoginException
        // but JUnit5's assertThrowsExactly will fail the exception is a subclass
        assertThrowsExactly(LoginException.class, datawaveLoginModule::login);
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

        /*
         * Need to add an directRole for s1 to allow the login check to pass
         */
        List<String> s1Roles = List.of("AuthorizedServer");

        DatawaveUser s1 = new DatawaveUser(server1, UserType.SERVER, null, s1Roles, null, System.currentTimeMillis());
        DatawaveUser s2 = new DatawaveUser(server2, UserType.SERVER, null, null, null, System.currentTimeMillis());
        DatawavePrincipal expected = new DatawavePrincipal(Lists.newArrayList(defaultPrincipal.getPrimaryUser(), s2, s1));

        when(securityDomain.getKeyStore()).thenReturn(serverKeystore);
        when(securityDomain.getTrustStore()).thenReturn(truststore);
        when(datawaveUserService.lookup(datawaveCredential.getEntities())).thenReturn(expected.getProxiedUsers());

        boolean result = datawaveLoginModule.login();
        assertTrue(result, "Login did not succeed.");
        assertEquals(userDN, expected.getUserDN());
    }

    @Test
    public void testJWTLogin() throws Exception {
        ReflectionTestUtils.setField(datawaveLoginModule, "jwtHeaderLogin", true);

        JWTTokenHandler tokenHandler = (JWTTokenHandler) ReflectionTestUtils.getField(datawaveLoginModule, "jwtTokenHandler");

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

        boolean result = datawaveLoginModule.login();
        assertTrue(result, "Login did not succeed.");
        assertEquals(userDN, expected.getUserDN());
    }

    @Test
    public void testInvalidLoginCertIssuerDenied() {
        MockDatawaveCertVerifier.issuerSupported = false;
        DatawaveCredential datawaveCredential = new DatawaveCredential(testUserCert, null, null);
        callbackHandler.name = datawaveCredential.getUserName();
        callbackHandler.credential = datawaveCredential;

        when(securityDomain.getKeyStore()).thenReturn(keystore);
        when(securityDomain.getTrustStore()).thenReturn(truststore);

        assertThrows(CredentialException.class, datawaveLoginModule::login);
    }

    @Test
    public void testInvalidLoginCertVerificationFailed() {
        MockDatawaveCertVerifier.verify = false;
        DatawaveCredential datawaveCredential = new DatawaveCredential(testUserCert, null, null);
        callbackHandler.name = datawaveCredential.getUserName();
        callbackHandler.credential = datawaveCredential;

        when(securityDomain.getKeyStore()).thenReturn(keystore);
        when(securityDomain.getTrustStore()).thenReturn(truststore);

        assertThrows(CredentialException.class, datawaveLoginModule::login);
    }

    @Test
    public void testInvalidLoginAuthorizationLookupFailed() throws Exception {
        DatawaveCredential datawaveCredential = new DatawaveCredential(testUserCert, null, null);
        callbackHandler.name = datawaveCredential.getUserName();
        callbackHandler.credential = datawaveCredential;

        when(securityDomain.getKeyStore()).thenReturn(keystore);
        when(securityDomain.getTrustStore()).thenReturn(truststore);
        when(datawaveUserService.lookup(datawaveCredential.getEntities())).thenThrow(new AuthorizationException("Unable to authenticate"));

        // there are many subclasses of LoginException
        // but JUnit5's assertThrowsExactly will fail the exception is a subclass
        assertThrowsExactly(LoginException.class, datawaveLoginModule::login);
    }

    private static class TestDatawavePrincipalLoginModule extends DatawavePrincipalLoginModule {
        @Override
        protected void performFieldInjection() {
            // do nothing - we're using @TestSubject to inject
        }
    }
}
