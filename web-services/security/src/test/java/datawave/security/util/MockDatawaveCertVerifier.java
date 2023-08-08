package datawave.security.util;

import java.security.KeyStore;
import java.security.cert.X509Certificate;

import datawave.security.login.DatawaveCertVerifier;

public class MockDatawaveCertVerifier extends DatawaveCertVerifier {
    public static boolean issuerSupported = true;
    public static boolean verify = true;

    @Override
    public boolean isIssuerSupported(String issuerSubjectDn, KeyStore trustStore) {
        return issuerSupported;
    }

    @Override
    public boolean verify(X509Certificate cert, String alias, KeyStore keystore, KeyStore truststore) {
        return verify;
    }
}
