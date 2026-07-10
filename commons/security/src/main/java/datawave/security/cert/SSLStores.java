package datawave.security.cert;

import java.security.KeyStore;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;

/**
 * Represents a key store/trust store pair.
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

}
