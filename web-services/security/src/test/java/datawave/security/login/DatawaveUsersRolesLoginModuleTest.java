package datawave.security.login;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.easymock.PowerMock.field;

import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.HashMap;

import javax.security.auth.Subject;
import javax.security.auth.login.FailedLoginException;

import org.jboss.security.SimplePrincipal;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.picketbox.plugins.PicketBoxCallbackHandler;

import datawave.security.auth.DatawaveCredential;
import datawave.security.authorization.DatawavePrincipal;

public class DatawaveUsersRolesLoginModuleTest {
    private static final String NORMALIZED_SUBJECT_DN = "cn=testuser, ou=my department, o=my company, st=some-state, c=us";
    private static final String NORMALIZED_SUBJECT_DN_WITH_ISSUER_DN = "cn=testuser, ou=my department, o=my company, st=some-state, c=us<cn=test ca, ou=my department, o=my company, st=some-state, c=us>";
    private static final String SUBJECT_DN_WITH_CN_FIRST = "CN=testUser, OU=My Department, O=My Company, ST=Some-State, C=US";
    private static final String SUBJECT_DN_WITH_CN_LAST = "C=US, ST=Some-State, O=My Company, OU=My Department, CN=testUser";
    private static final String ISSUER_DN_WITH_CN_FIRST = "CN=TEST CA, OU=My Department, O=My Company, ST=Some-State, C=US";
    private static final String ISSUER_DN_WITH_CN_LAST = "C=US, ST=Some-State, O=My Company, OU=My Department, CN=TEST CA";

    private DatawaveUsersRolesLoginModule loginModule;
    private PicketBoxCallbackHandler callbackHandler;

    private X509Certificate testUserCert;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        callbackHandler = new PicketBoxCallbackHandler();

        HashMap<String,String> sharedState = new HashMap<>();
        HashMap<String,String> options = new HashMap<>();
        options.put("usersProperties", "users.properties");
        options.put("rolesProperties", "roles.properties");
        options.put("principalClass", "datawave.security.authorization.DatawavePrincipal");

        loginModule = new DatawaveUsersRolesLoginModule();
        loginModule.initialize(new Subject(), callbackHandler, sharedState, options);

        KeyStore truststore = KeyStore.getInstance("PKCS12");
        truststore.load(getClass().getResourceAsStream("/ca.pkcs12"), "secret".toCharArray());
        KeyStore keystore = KeyStore.getInstance("PKCS12");
        keystore.load(getClass().getResourceAsStream("/testUser.pkcs12"), "secret".toCharArray());
        testUserCert = (X509Certificate) keystore.getCertificate("testuser");
    }

    @Test
    public void testSuccessfulLogin() throws Exception {
        String name = testUserCert.getSubjectDN().getName() + "<" + testUserCert.getIssuerDN().getName() + ">";
        callbackHandler.setSecurityInfo(new SimplePrincipal(name),
                        new DatawaveCredential(testUserCert.getSubjectDN().getName(), testUserCert.getIssuerDN().getName(), null, null).toString());

        boolean success = loginModule.login();
        assertTrue("Login didn't succeed for alias in users/roles.properties", success);
        DatawavePrincipal principal = (DatawavePrincipal) field(DatawaveUsersRolesLoginModule.class, "identity").get(loginModule);
        assertEquals(NORMALIZED_SUBJECT_DN_WITH_ISSUER_DN, principal.getName());
    }

    @Test
    public void testReverseDnSuccessfulLogin() throws Exception {
        String name = SUBJECT_DN_WITH_CN_LAST + "<" + ISSUER_DN_WITH_CN_LAST + ">";
        callbackHandler.setSecurityInfo(new SimplePrincipal(name),
                        new DatawaveCredential(SUBJECT_DN_WITH_CN_LAST, ISSUER_DN_WITH_CN_LAST, null, null).toString());

        boolean success = loginModule.login();
        assertTrue("Login didn't succeed for alias in users/roles.properties", success);
        DatawavePrincipal principal = (DatawavePrincipal) field(DatawaveUsersRolesLoginModule.class, "identity").get(loginModule);
        assertEquals(NORMALIZED_SUBJECT_DN_WITH_ISSUER_DN, principal.getName());
    }

    @Test
    public void testFailedLoginBadPassword() throws Exception {
        expectedException.expect(FailedLoginException.class);
        expectedException.expectMessage("Password invalid/Password required");

        callbackHandler.setSecurityInfo(new SimplePrincipal("testUser<testIssuer>"), new DatawaveCredential("testUser", "testIssuer", null, null).toString());

        boolean success = loginModule.login();
        assertFalse("Login succeed for alias in users.properties with bad password", success);
    }

    @Test
    public void normalizeDnWithCnLast() {
        assertEquals(NORMALIZED_SUBJECT_DN, DatawaveUsersRolesLoginModule.normalizeUsername(SUBJECT_DN_WITH_CN_LAST));
    }

    @Test
    public void normalizeDnWithCnFirst() {
        assertEquals(NORMALIZED_SUBJECT_DN, DatawaveUsersRolesLoginModule.normalizeUsername(SUBJECT_DN_WITH_CN_FIRST));
    }

    @Test
    public void normalizeSubjectIssuerCombinations() {
        assertEquals(NORMALIZED_SUBJECT_DN_WITH_ISSUER_DN,
                        DatawaveUsersRolesLoginModule.normalizeUsername(SUBJECT_DN_WITH_CN_FIRST + "<" + ISSUER_DN_WITH_CN_FIRST + ">"));
        assertEquals(NORMALIZED_SUBJECT_DN_WITH_ISSUER_DN,
                        DatawaveUsersRolesLoginModule.normalizeUsername(SUBJECT_DN_WITH_CN_FIRST + "<" + ISSUER_DN_WITH_CN_LAST + ">"));
        assertEquals(NORMALIZED_SUBJECT_DN_WITH_ISSUER_DN,
                        DatawaveUsersRolesLoginModule.normalizeUsername(SUBJECT_DN_WITH_CN_LAST + "<" + ISSUER_DN_WITH_CN_FIRST + ">"));
        assertEquals(NORMALIZED_SUBJECT_DN_WITH_ISSUER_DN,
                        DatawaveUsersRolesLoginModule.normalizeUsername(SUBJECT_DN_WITH_CN_LAST + "<" + ISSUER_DN_WITH_CN_LAST + ">"));
    }
}
