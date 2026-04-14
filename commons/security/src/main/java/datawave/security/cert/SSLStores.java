package datawave.security.cert;

import java.security.KeyStore;
import java.security.cert.Certificate;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;

/**
 * This interface replaces usages of the org.jboss.security.JSSESecurityDomain interface provided by the legacy picketbox library. A corresponding
 * implementation can be found in the datawave.security.ssl.SSLContextInfoImpl class in the datawave-ws-security module.
 */
public interface SSLStores {

    /**
     * Return the key store
     *
     * @return the keystore
     */
    default KeyStore getKeyStore() {
        return null;
    }

    /**
     * Return the key managers
     *
     * @return the key managers
     */
    default KeyManager[] getKeyManagers() {
        return new KeyManager[0];
    }

    /**
     * Return the trust store
     *
     * @return the truststore
     */
    default KeyStore getTrustStore() {
        return null;
    }

    /**
     * Return the trust managers
     *
     * @return the trust managers
     */
    default TrustManager[] getTrustManagers() {
        return new TrustManager[0];
    }

    default Certificate getCertificate(String alias) throws Exception {
        return null;
    }
}
