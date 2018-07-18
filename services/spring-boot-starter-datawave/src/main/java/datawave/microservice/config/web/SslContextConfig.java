package datawave.microservice.config.web;

import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.boot.context.embedded.Ssl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.ResourceUtils;

@Configuration
public class SslContextConfig {
    
    @Bean
    public SSLContext sslContext(ServerProperties serverProperties) throws NoSuchAlgorithmException, KeyManagementException {
        final Ssl ssl = serverProperties.getSsl();
        SSLContext sslContext = SSLContext.getInstance(ssl.getProtocol());
        sslContext.init(getKeyManagers(ssl), getTrustManagers(ssl), null);
        return sslContext;
    }
    
    private KeyManager[] getKeyManagers(Ssl ssl) {
        try {
            String keyStoreType = ssl.getKeyStoreType();
            if (keyStoreType == null) {
                keyStoreType = "JKS";
            }
            KeyStore keyStore = KeyStore.getInstance(keyStoreType);
            URL url = ResourceUtils.getURL(ssl.getKeyStore());
            keyStore.load(url.openStream(), ssl.getKeyStorePassword().toCharArray());
            
            // Get key manager to provide client credentials.
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            char[] keyPassword = ssl.getKeyPassword() != null ? ssl.getKeyPassword().toCharArray() : ssl.getKeyStorePassword().toCharArray();
            keyManagerFactory.init(keyStore, keyPassword);
            return keyManagerFactory.getKeyManagers();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
    
    private TrustManager[] getTrustManagers(Ssl ssl) {
        try {
            String trustStoreType = ssl.getTrustStoreType();
            if (trustStoreType == null) {
                trustStoreType = "JKS";
            }
            String trustStore = ssl.getTrustStore();
            if (trustStore == null) {
                return null;
            }
            KeyStore trustedKeyStore = KeyStore.getInstance(trustStoreType);
            URL url = ResourceUtils.getURL(trustStore);
            trustedKeyStore.load(url.openStream(), ssl.getTrustStorePassword().toCharArray());
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustedKeyStore);
            return trustManagerFactory.getTrustManagers();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
