package datawave.webservice.common.remote;

import java.io.File;
import java.io.FileOutputStream;
import java.net.Socket;
import java.security.KeyStore;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509KeyManager;

import datawave.security.cert.SSLStores;

public class TestSSLStores implements SSLStores {

    private final PrivateKey privateKey;
    private final X509Certificate[] chain;
    private final String alias;
    private final char[] keyPass;

    public TestSSLStores(String alias, PrivateKey privateKey, char[] keyPass, X509Certificate[] chain) {
        this.privateKey = privateKey;
        this.chain = chain;
        this.alias = alias;
        this.keyPass = keyPass;
    }

    @Override
    public KeyStore getKeyStore() throws SecurityException {
        try {
            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(null, null);
            keyStore.setKeyEntry(alias, privateKey, keyPass, chain);
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
                return privateKey;
            }
        };
        return managers;
    }

    @Override
    public KeyStore getTrustStore() throws SecurityException {
        try {
            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(null, null);
            keyStore.setKeyEntry(alias, privateKey, keyPass, chain);
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
}
