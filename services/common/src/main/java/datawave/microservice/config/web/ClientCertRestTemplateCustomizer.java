package datawave.microservice.config.web;

import org.apache.http.client.HttpClient;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.HttpClients;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.boot.context.embedded.Ssl;
import org.springframework.boot.web.client.RestTemplateCustomizer;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.ResourceUtils;
import org.springframework.web.client.RestTemplate;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;

/**
 * Customizes Spring {@link RestTemplate} instances by using our configured SSL certificate to present a client certificate whenever asked by remote services.
 */
@Component
public class ClientCertRestTemplateCustomizer implements RestTemplateCustomizer {
    private final Ssl ssl;
    
    @Autowired
    public ClientCertRestTemplateCustomizer(ServerProperties serverProperties) {
        this.ssl = serverProperties.getSsl();
    }
    
    @Override
    public void customize(RestTemplate restTemplate) {
        try {
            restTemplate.setRequestFactory(clientHttpRequestFactory());
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            throw new IllegalArgumentException("Unable to configure client HTTP request factory: " + e.getMessage(), e);
        }
    }
    
    private ClientHttpRequestFactory clientHttpRequestFactory() throws NoSuchAlgorithmException, KeyManagementException {
        SSLContext sslContext = SSLContext.getInstance(ssl.getProtocol());
        sslContext.init(getKeyManagers(), getTrustManagers(), null);
        // TODO: We're allowing all hosts, since the cert presented by the service we're calling likely won't match its hostname (e.g., a docker host name)
        // Instead, we could list the expected cert as a property (or use our server cert), and verify that the presented name matches.
        HttpClient httpClient = HttpClients.custom().setSSLContext(sslContext).setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE).build();
        return new HttpComponentsClientHttpRequestFactory(httpClient);
    }
    
    private KeyManager[] getKeyManagers() {
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
    
    private TrustManager[] getTrustManagers() {
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
