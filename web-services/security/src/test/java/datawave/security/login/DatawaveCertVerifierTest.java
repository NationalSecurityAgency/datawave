package datawave.security.login;

import static org.junit.Assert.assertTrue;
import static org.powermock.api.easymock.PowerMock.replayAll;
import static org.powermock.api.easymock.PowerMock.resetAll;
import static org.powermock.api.easymock.PowerMock.verifyAll;

import java.security.KeyStore;
import java.security.cert.X509Certificate;

import org.jboss.logging.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(DatawaveCertVerifier.class)
@PowerMockIgnore("javax.security.auth.*")
public class DatawaveCertVerifierTest {

    private DatawaveCertVerifier verifier;

    private KeyStore truststore;
    private KeyStore keystore;
    private X509Certificate testUserCert;

    @Before
    public void setUp() throws Exception {
        verifier = new DatawaveCertVerifier();

        truststore = KeyStore.getInstance("PKCS12");
        truststore.load(getClass().getResourceAsStream("/ca.pkcs12"), "secret".toCharArray());
        keystore = KeyStore.getInstance("PKCS12");
        keystore.load(getClass().getResourceAsStream("/testUser.pkcs12"), "secret".toCharArray());
        testUserCert = (X509Certificate) keystore.getCertificate("testuser");

        replayAll();

        verifier.setLogger(Logger.getLogger(DatawaveCertVerifier.class));

        verifyAll();
        resetAll();
    }

    @Test
    public void testVerifyNoOCSP() throws Exception {
        replayAll();

        verifier.setOcspLevel(DatawaveCertVerifier.OcspLevel.OFF.name());
        boolean valid = verifier.verify(testUserCert, testUserCert.getSubjectDN().getName(), keystore, truststore);
        assertTrue("Verify failed unexpectedly.", valid);

        verifyAll();
    }
}
