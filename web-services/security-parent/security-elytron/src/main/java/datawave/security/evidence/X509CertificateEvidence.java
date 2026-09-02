package datawave.security.evidence;

import java.security.cert.X509Certificate;
import java.util.List;
import java.util.StringJoiner;

import datawave.security.authorization.SubjectIssuerDNPair;

/**
 * Represents evidence extracted from a PKI certificate.
 */
public class X509CertificateEvidence extends DatawaveEvidence {

    /**
     * The username.
     */
    private final String username;

    /**
     * The set of entities consisting of the user entity and any proxied entities.
     */
    private final List<SubjectIssuerDNPair> entities;

    /**
     * The certificate
     */
    private final X509Certificate certificate;

    public X509CertificateEvidence(String username, List<SubjectIssuerDNPair> entities, X509Certificate certificate) {
        this.username = username;
        this.entities = entities;
        this.certificate = certificate;
    }

    @Override
    public String getUsername() {
        return username;
    }

    public List<SubjectIssuerDNPair> getEntities() {
        return entities;
    }

    public X509Certificate getCertificate() {
        return certificate;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", X509CertificateEvidence.class.getSimpleName() + "[", "]").add("username='" + username + "'").add("entities=" + entities)
                        .add("certificate=" + formatCert()).add("decodedPrincipal=" + decodedPrincipal).toString();
    }

    private String formatCert() {
        return certificate == null ? null : "<" + certificate.getSubjectDN().getName() + ":" + certificate.getIssuerDN().getName() + ">";
    }
}
