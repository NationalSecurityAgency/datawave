package datawave.security.login;

import java.security.KeyStore;
import java.security.cert.X509Certificate;

import org.jboss.logging.Logger;
import org.jboss.security.auth.certs.X509CertificateVerifier;

public class DatawaveCertVerifier implements X509CertificateVerifier {

    public enum OcspLevel {
        OFF, OPTIONAL, REQUIRED
    }

    protected Logger log;
    protected boolean trace;
    protected OcspLevel ocspLevel = OcspLevel.OFF;

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

    protected void initOcsp() {}

    protected boolean checkOCSP(X509Certificate cert, String alias, KeyStore truststore) {
        switch (ocspLevel) {
            case OFF:
                break;
            default:
                log.error("OCSP level " + ocspLevel + " is not supported!");
                throw new IllegalArgumentException("OCSP level " + ocspLevel + " is not supported!");
        }
        return true;
    }

    public boolean isIssuerSupported(String issuerSubjectDn, KeyStore trustStore) {
        return true;
    }

    public void setLogger(Logger log) {
        this.log = log;
        if (log.isTraceEnabled())
            trace = true;
    }

    public OcspLevel getOcspLevel() {
        return ocspLevel;
    }

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
