package datawave.security.login;

import datawave.security.authorization.DatawavePrincipal;
import datawave.security.util.MockCallbackHandler;
import datawave.security.util.MockDatawaveCertVerifier;
import org.jboss.security.SimplePrincipal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import javax.security.auth.Subject;
import java.lang.reflect.Field;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DatawaveCertRolesLoginModuleTest {
    private DatawaveCertRolesLoginModule loginModule;
    private MockCallbackHandler callbackHandler;
    
    private X509Certificate testUserCert;
    
    @BeforeEach
    public void setUp() throws Exception {
        callbackHandler = new MockCallbackHandler("Alias: ", "Certificate: ");
        
        HashMap<String,String> sharedState = new HashMap<>();
        HashMap<String,String> options = new HashMap<>();
        options.put("rolesProperties", "roles.properties");
        options.put("principalClass", "datawave.security.authorization.DatawavePrincipal");
        options.put("verifier", MockDatawaveCertVerifier.class.getName());
        
        loginModule = new DatawaveCertRolesLoginModule();
        loginModule.initialize(new Subject(), callbackHandler, sharedState, options);
        
        KeyStore truststore = KeyStore.getInstance("PKCS12");
        truststore.load(getClass().getResourceAsStream("/ca.pkcs12"), "secret".toCharArray());
        KeyStore keystore = KeyStore.getInstance("PKCS12");
        keystore.load(getClass().getResourceAsStream("/testUser.pkcs12"), "secret".toCharArray());
        testUserCert = (X509Certificate) keystore.getCertificate("testuser");
    }
    
    @Disabled
    @Test
    public void testSuccessfulLogin() throws Exception {
        String name = testUserCert.getSubjectDN().getName() + "<" + testUserCert.getIssuerDN().getName() + ">";
        callbackHandler.name = name;
        callbackHandler.credential = testUserCert;
        
        boolean success = loginModule.login();
        assertTrue(success, "Login didn't succeed for alias in roles.properties");
        Field f = loginModule.getClass().getDeclaredField("identity");
        f.setAccessible(true);
        DatawavePrincipal principal = (DatawavePrincipal) f.get(loginModule);
        assertEquals(name.toLowerCase(), principal.getName());
    }
    
    @Test
    public void testFailedLogin() throws Exception {
        callbackHandler.name = "fakeUser";
        callbackHandler.credential = testUserCert;
        
        boolean success = loginModule.login();
        assertFalse(success, "Login succeed for alias not in roles.properties");
    }
    
    @Disabled
    @Test
    public void testSuccessfulLoginNoIssuer() throws Exception {
        HashMap<String,String> sharedState = new HashMap<>();
        HashMap<String,String> options = new HashMap<>();
        options.put("rolesProperties", "rolesNoIssuer.properties");
        options.put("principalClass", SimplePrincipal.class.getName());
        options.put("verifier", MockDatawaveCertVerifier.class.getName());
        options.put("addIssuerDN", Boolean.FALSE.toString());
        
        loginModule = new DatawaveCertRolesLoginModule();
        loginModule.initialize(new Subject(), callbackHandler, sharedState, options);
        
        callbackHandler.name = testUserCert.getSubjectDN().getName();
        callbackHandler.credential = testUserCert;
        
        boolean success = loginModule.login();
        assertTrue(success, "Login didn't succeed for alias in rolesNoIssuer.properties");
        Field f = loginModule.getClass().getDeclaredField("identity");
        f.setAccessible(true);
        SimplePrincipal principal = (SimplePrincipal) f.get(loginModule);
        assertEquals(testUserCert.getSubjectDN().getName().toLowerCase(), principal.getName());
    }
    
    @Test
    public void testTrustedHeaderLogin() throws Exception {
        HashMap<String,String> sharedState = new HashMap<>();
        HashMap<String,String> options = new HashMap<>();
        options.put("trustedHeaderLogin", Boolean.TRUE.toString());
        
        loginModule = new DatawaveCertRolesLoginModule();
        loginModule.initialize(new Subject(), callbackHandler, sharedState, options);
        
        boolean success = loginModule.login();
        assertFalse(success);
    }
}
