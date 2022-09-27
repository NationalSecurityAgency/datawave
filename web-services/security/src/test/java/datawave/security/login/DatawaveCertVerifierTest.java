package datawave.security.login;

import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.security.KeyStore;
import java.security.cert.X509Certificate;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class DatawaveCertVerifierTest {
    
    private DatawaveCertVerifier verifier;
    
    private KeyStore truststore;
    private KeyStore keystore;
    private X509Certificate testUserCert;
    
    @BeforeEach
    public void setUp() throws Exception {
        verifier = new DatawaveCertVerifier();
        
        truststore = KeyStore.getInstance("PKCS12");
        truststore.load(getClass().getResourceAsStream("/ca.pkcs12"), "secret".toCharArray());
        keystore = KeyStore.getInstance("PKCS12");
        keystore.load(getClass().getResourceAsStream("/testUser.pkcs12"), "secret".toCharArray());
        testUserCert = (X509Certificate) keystore.getCertificate("testuser");
        
        verifier.setLogger(Logger.getLogger(DatawaveCertVerifier.class));
    }
    
    @Test
    public void testVerifyNoOCSP() throws Exception {
        verifier.setOcspLevel(DatawaveCertVerifier.OcspLevel.OFF.name());
        boolean valid = verifier.verify(testUserCert, testUserCert.getSubjectDN().getName(), keystore, truststore);
        assertTrue(valid, "Verify failed unexpectedly.");
    }
}
