package datawave.security.cert;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.net.URL;

import org.junit.jupiter.api.Test;

class SSLStoresImplTest {

    private static final String JKS = "JKS";
    private static final String PKCS12 = "PKCS12";

    private static final String jksKeyStorePath = "/datawave-server-keystore.jks";
    private static final String jksTrustStorePath = "/datawave-server-truststore.jks";
    private static final String pkcs12KeyStorePath = "/datawave-server-keystore.p12";
    private static final String pkcs12TrustStorePath = "/datawave-server-truststore.p12";

    private static final String password = "ChangeIt";

    @Test
    void testJKSKeyStore() throws Exception {
        SSLStoresImpl sslContext = SSLStoresImpl.builder().withKeystore(getResource(jksKeyStorePath), password, JKS).build();

        assertNotNull(sslContext.getKeyStore(), "Keystore is null");
        assertNotNull(sslContext.getTrustStore(), "Truststore is null");
        assertSame(sslContext.getKeyStore(), sslContext.getTrustStore(), "Keystore and truststore are not the same instance");
    }

    @Test
    void testJKSKeyStoreWithPKCS12TrustStore() throws Exception {
        SSLStoresImpl sslContext = SSLStoresImpl.builder().withKeystore(getResource(jksKeyStorePath), password, JKS)
                        .withTruststore(getResource(pkcs12TrustStorePath), password, PKCS12).build();

        assertNotNull(sslContext.getKeyStore(), "Keystore is null");
        assertEquals(JKS, sslContext.getKeyStore().getType(), "Keystore is incorrect type");
        assertNotNull(sslContext.getTrustStore(), "Truststore is null");
        assertEquals(PKCS12, sslContext.getTrustStore().getType(), "Truststore is incorrect type");
        assertNotSame(sslContext.getKeyStore(), sslContext.getTrustStore(), "Keystore and truststore are the same instance");
    }

    @Test
    void testPKCS12KeyStore() throws Exception {
        SSLStoresImpl sslContext = SSLStoresImpl.builder().withKeystore(getResource(pkcs12KeyStorePath), password, PKCS12).build();

        assertNotNull(sslContext.getKeyStore(), "Keystore is null");
        assertNotNull(sslContext.getTrustStore(), "Truststore is null");
        assertSame(sslContext.getKeyStore(), sslContext.getTrustStore(), "Keystore and truststore are not the same instance");
    }

    @Test
    void testPKCS12KeyStoreWithJKSTrustStore() throws Exception {
        SSLStoresImpl sslContext = SSLStoresImpl.builder().withKeystore(getResource(pkcs12KeyStorePath), password, PKCS12)
                        .withTruststore(getResource(jksTrustStorePath), password, JKS).build();

        assertNotNull(sslContext.getKeyStore(), "Keystore is null");
        assertEquals(PKCS12, sslContext.getKeyStore().getType(), "Keystore is incorrect type");
        assertNotNull(sslContext.getTrustStore(), "Truststore is null");
        assertEquals(JKS, sslContext.getTrustStore().getType(), "Truststore is incorrect type");
        assertNotSame(sslContext.getKeyStore(), sslContext.getTrustStore(), "Keystore and truststore are the same instance");
    }

    private String getResource(String resource) {
        URL url = getClass().getResource(resource);
        if (url != null) {
            return url.toExternalForm();
        } else {
            throw new NullPointerException("Could not load resource " + resource);
        }
    }
}
