package datawave.webservice.common.remote;

import java.io.File;
import java.io.FileOutputStream;
import java.net.Socket;
import java.security.Key;
import java.security.KeyStore;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Properties;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509KeyManager;

import org.jboss.security.JSSESecurityDomain;

public class TestJSSESecurityDomain implements JSSESecurityDomain {
    private final PrivateKey privKey;
    private final X509Certificate[] chain;
    private final String alias;
    private final char[] keyPass;

    public TestJSSESecurityDomain(String alias, PrivateKey privKey, char[] keyPass, X509Certificate[] chain) {
        this.privKey = privKey;
        this.chain = chain;
        this.alias = alias;
        this.keyPass = keyPass;
    }

    @Override
    public KeyStore getKeyStore() throws SecurityException {
        try {
            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(null, null);
            keyStore.setKeyEntry(alias, privKey, keyPass, chain);
            File file = File.createTempFile("keystore", ".jks");
            file.deleteOnExit();
            keyStore.store(new FileOutputStream(file), keyPass);
            return keyStore;
        } catch (Exception e) {
            throw new SecurityException(e);
        }
    }

    @Override
    public KeyManager[] getKeyManagers() throws SecurityException {
        KeyManager[] managers = new KeyManager[1];
        managers[0] = new X509KeyManager() {
            @Override
            public String[] getClientAliases(String keyType, Principal[] issuers) {
                return new String[0];
            }

            @Override
            public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket) {
                return null;
            }

            @Override
            public String[] getServerAliases(String keyType, Principal[] issuers) {
                return new String[0];
            }

            @Override
            public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
                return null;
            }

            @Override
            public X509Certificate[] getCertificateChain(String alias) {
                return chain;
            }

            @Override
            public PrivateKey getPrivateKey(String alias) {
                return privKey;
            }
        };
        return managers;
    }

    @Override
    public KeyStore getTrustStore() throws SecurityException {
        try {
            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(null, null);
            keyStore.setKeyEntry(alias, privKey, keyPass, chain);
            keyStore.store(new FileOutputStream(".keystore"), keyPass);
            return keyStore;
        } catch (Exception e) {
            throw new SecurityException(e);
        }
    }

    @Override
    public TrustManager[] getTrustManagers() throws SecurityException {
        return new TrustManager[0];
    }

    @Override
    public void reloadKeyAndTrustStore() throws Exception {

    }

    @Override
    public String getServerAlias() {
        return null;
    }

    @Override
    public String getClientAlias() {
        return null;
    }

    @Override
    public boolean isClientAuth() {
        return false;
    }

    @Override
    public Key getKey(String s, String s1) throws Exception {
        return null;
    }

    @Override
    public Certificate getCertificate(String s) throws Exception {
        return null;
    }

    @Override
    public String[] getCipherSuites() {
        return new String[0];
    }

    @Override
    public String[] getProtocols() {
        return new String[0];
    }

    @Override
    public Properties getAdditionalProperties() {
        return null;
    }

    @Override
    public String getSecurityDomain() {
        return null;
    }
}
