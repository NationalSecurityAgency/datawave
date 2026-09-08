package datawave.security.cert;

import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Objects;

import org.slf4j.Logger;

/**
 * A Datawave-specific {@link X509CertificateVerifier} implementation.
 */
public class DatawaveCertVerifier implements X509CertificateVerifier {

    public enum OcspLevel {
        OFF, OPTIONAL, REQUIRED
    }

    protected Logger log;
    protected boolean trace;
    protected OcspLevel ocspLevel = OcspLevel.OFF;

    /**
     * Verify the given certificate
     *
     * @param cert
     *            the X509Certificate to verify
     * @param alias
     *            the certificate alias
     * @param keystore
     *            the keystore for the cert
     * @param truststore
     *            the truststore for the cert
     * @return whether the certificate is considered valid
     */
    @Override
    public boolean verify(X509Certificate cert, String alias, KeyStore keystore, KeyStore truststore) {
        boolean validity = false;
        try {
            cert.checkValidity();
            validity = checkOCSP(cert, alias, truststore);
        } catch (Exception e) {
            if (trace)
                log.trace("Validity exception", e);
        }
        return validity;

    }

    /**
     * Handle OSCP initialization.
     */
    protected void initOcsp() {}

    /**
     * Return the OSCP level set for this verifier is supported for the given certificate.
     *
     * @param cert
     *            the certificate
     * @param alias
     *            the certificate alias
     * @param truststore
     *            the truststore
     * @return true if the OSCP level is supported, or false otherwise
     */
    protected boolean checkOCSP(X509Certificate cert, String alias, KeyStore truststore) {
        if (Objects.requireNonNull(ocspLevel) == OcspLevel.OFF) {
            return true;
        } else {
            log.error("OCSP level {} is not supported!", ocspLevel);
            throw new IllegalArgumentException("OCSP level " + ocspLevel + " is not supported!");
        }
    }

    /**
     * Return whether the given issuer is supported.
     *
     * @param issuerSubjectDn
     *            the issuer DN
     * @param trustStore
     *            the truststore
     * @return true if the issuer is supported, or false otherwise
     */
    public boolean isIssuerSupported(String issuerSubjectDn, KeyStore trustStore) {
        return true;
    }

    /**
     * Set the delegate logger for this {@link DatawaveCertVerifier}.
     *
     * @param log
     *            the logger
     */
    public void setLogger(Logger log) {
        this.log = log;
        this.trace = log.isTraceEnabled();
    }

    /**
     * Return the OSCP level.
     *
     * @return the OSCP level
     */
    public OcspLevel getOcspLevel() {
        return ocspLevel;
    }

    /**
     * Set the OSCP level.
     *
     * @param level
     *            the OSCP level
     */
    public void setOcspLevel(String level) {
        ocspLevel = OcspLevel.valueOf(level.toUpperCase());
        switch (ocspLevel) {
            case REQUIRED:
            case OPTIONAL:
                initOcsp();
                break;
        }
    }
}
