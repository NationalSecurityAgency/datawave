package datawave.security.cert;

import java.security.KeyStore;
import java.security.cert.X509Certificate;

/**
 * A base X509 certificate verifier.
 */
public interface X509CertificateVerifier {

    /**
     * Validate a cert.
     *
     * @param cert
     *            the X509Certificate to verify
     * @param alias
     *            the expected keystore alias
     * @param keyStore
     *            the keystore for the cert
     * @param trustStore
     *            the truststore for the cert
     * @return true if the cert is valid, false otherwise
     */
    boolean verify(X509Certificate cert, String alias, KeyStore keyStore, KeyStore trustStore);
}
