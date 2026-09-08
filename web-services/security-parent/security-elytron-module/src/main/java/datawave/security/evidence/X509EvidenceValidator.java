package datawave.security.evidence;

import java.security.KeyStore;
import java.security.cert.X509Certificate;

import datawave.security.cert.X509CertificateVerifier;

/**
 * A validator for {@link X509CertificateEvidence} that will validate the certificate of the evidence using a configured certificate verifier.
 */
public class X509EvidenceValidator {

    private final X509CertificateVerifier certVerifier;
    private final KeyStore keyStore;
    private final KeyStore trustStore;

    public X509EvidenceValidator(X509CertificateVerifier certVerifier, KeyStore keyStore, KeyStore trustStore) {
        this.certVerifier = certVerifier;
        this.keyStore = keyStore;
        this.trustStore = trustStore;
    }

    /**
     * Return whether the given evidence has a valid certificate.
     *
     * @param evidence
     *            the evidence
     * @return true if the evidence certificate is valid, or false otherwise
     */
    public boolean validate(X509CertificateEvidence evidence) {
        X509Certificate certificate = evidence.getCertificate();
        String alias = certificate.getIssuerX500Principal().getName();
        return certVerifier.verify(certificate, alias, keyStore, trustStore);
    }
}
