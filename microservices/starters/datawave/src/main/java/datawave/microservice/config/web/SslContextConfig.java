package datawave.microservice.config.web;

import java.io.IOException;
import java.net.Socket;
import java.net.URL;
import java.security.InvalidAlgorithmParameterException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Enumeration;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.KeyManagerFactorySpi;
import javax.net.ssl.ManagerFactoryParameters;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.TrustManagerFactorySpi;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.ResourceUtils;

import com.google.common.base.Preconditions;

import datawave.microservice.config.web.DatawaveServerProperties.OutboundSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.util.concurrent.FastThreadLocal;

/**
 * Provides an {@link SSLContext} (JDK) or an {@link SslContext} (Netty) for use in the application.
 */
@Configuration
@ConditionalOnWebApplication
@ConditionalOnProperty(name = "server.outbound-ssl.enabled", matchIfMissing = true)
public class SslContextConfig {
    
    @Bean
    @Qualifier("outboundJDKSslContext")
    public SSLContext sslContext(DatawaveServerProperties serverProperties) throws NoSuchAlgorithmException, KeyManagementException {
        final OutboundSsl ssl = serverProperties.getOutboundSsl();
        SSLContext sslContext = SSLContext.getInstance(ssl.getProtocol());
        sslContext.init(getKeyManagers(ssl), getTrustManagers(ssl), null);
        return sslContext;
    }
    
    @Bean
    @Qualifier("outboundNettySslContext")
    public SslContext nettySslContext(DatawaveServerProperties serverProperties) throws SSLException {
        final OutboundSsl ssl = serverProperties.getOutboundSsl();
        // @formatter:off
        SslContextBuilder builder = SslContextBuilder.forClient()
            .sslProvider(SslProvider.OPENSSL)
            .trustManager(getTrustManagerFactory(ssl))
            .keyManager(getKeyManagerFactory(ssl));
        // @formatter:on
        if (ssl.getCiphers() != null)
            builder.ciphers(Arrays.asList(ssl.getCiphers()));
        if (ssl.getEnabledProtocols() != null)
            builder.protocols(ssl.getEnabledProtocols());
        return builder.build();
    }
    
    private KeyManager[] getKeyManagers(OutboundSsl ssl) {
        return getKeyManagerFactory(ssl).getKeyManagers();
    }
    
    private KeyManagerFactory getKeyManagerFactory(OutboundSsl ssl) {
        try {
            KeyStore keyStore = getKeyStore(ssl);
            
            // Get key manager to provide client credentials.
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            char[] keyPassword = ssl.getKeyPassword() != null ? ssl.getKeyPassword().toCharArray() : ssl.getKeyStorePassword().toCharArray();
            keyManagerFactory.init(keyStore, keyPassword);
            if (ssl.getKeyAlias() != null)
                return new KeyAliasKeyManagerFactory(keyManagerFactory, ssl.getKeyAlias());
            else
                return keyManagerFactory;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
    
    private KeyStore getKeyStore(OutboundSsl ssl) throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException {
        String keyStoreType = ssl.getKeyStoreType();
        if (keyStoreType == null) {
            keyStoreType = "JKS";
        }
        KeyStore keyStore = KeyStore.getInstance(keyStoreType);
        URL url = ResourceUtils.getURL(ssl.getKeyStore());
        keyStore.load(url.openStream(), ssl.getKeyStorePassword().toCharArray());
        return keyStore;
    }
    
    private TrustManager[] getTrustManagers(OutboundSsl ssl) {
        TrustManagerFactory tmf = getTrustManagerFactory(ssl);
        return tmf == null ? null : tmf.getTrustManagers();
    }
    
    private TrustManagerFactory getTrustManagerFactory(OutboundSsl ssl) {
        try {
            KeyStore trustStore = getTrustStore(ssl);
            KeyStore keyStore = getKeyStore(ssl);
            
            X509Certificate trustedCertificate = getTrustedCertificate(ssl.getKeyAlias(), keyStore);
            
            TrustManagerFactory trustManagerFactory = new DatawaveTrustMangerFactory(TrustManagerFactory.getDefaultAlgorithm(), trustedCertificate);
            trustManagerFactory.init(trustStore);
            return trustManagerFactory;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
    
    private KeyStore getTrustStore(OutboundSsl ssl) throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException {
        String trustStoreType = ssl.getTrustStoreType();
        if (trustStoreType == null) {
            trustStoreType = "JKS";
        }
        String trustStoreURL = ssl.getTrustStore();
        if (trustStoreURL == null) {
            return null;
        }
        KeyStore trustStore = KeyStore.getInstance(trustStoreType);
        URL url = ResourceUtils.getURL(trustStoreURL);
        trustStore.load(url.openStream(), ssl.getTrustStorePassword().toCharArray());
        return trustStore;
    }
    
    private X509Certificate getTrustedCertificate(String alias, KeyStore keyStore) throws KeyStoreException {
        Certificate trustCandidate = null;
        X509Certificate trustedCertificate = null;
        if (alias != null) {
            trustCandidate = keyStore.getCertificate(alias);
        } else {
            Enumeration<String> aliases = keyStore.aliases();
            if (aliases.hasMoreElements()) {
                trustCandidate = keyStore.getCertificate(aliases.nextElement());
            }
        }
        if (trustCandidate instanceof X509Certificate) {
            trustedCertificate = (X509Certificate) trustCandidate;
        }
        return trustedCertificate;
    }
    
    /**
     * A custom {@link TrustManagerFactory} that wraps any returned {@link TrustManager}s in order to apply custom behavior for trusting a remote server.
     */
    private static class DatawaveTrustMangerFactory extends TrustManagerFactory {
        private static final Provider PROVIDER = new Provider("", 0.0, "") {
            private static final long serialVersionUID = -2680540247105807895L;
        };
        private static final FastThreadLocal<DatawaveTrustManagerSpi> CURRENT_SPI = new FastThreadLocal<DatawaveTrustManagerSpi>() {
            @Override
            protected DatawaveTrustManagerSpi initialValue() {
                return new DatawaveTrustManagerSpi();
            }
        };
        
        public DatawaveTrustMangerFactory(String algorithm, X509Certificate trustedCertificate) throws NoSuchAlgorithmException {
            super(CURRENT_SPI.get(), PROVIDER, algorithm);
            CURRENT_SPI.get().init(TrustManagerFactory.getInstance(algorithm), trustedCertificate);
            CURRENT_SPI.remove();
            
            Preconditions.checkNotNull(algorithm);
        }
    }
    
    /**
     * The {@link TrustManagerFactorySpi} that is used by {@link DatawaveTrustMangerFactory} in order to wrap any returned {@link TrustManager}s with a
     * {@link DatawaveTrustManager}.
     */
    private static class DatawaveTrustManagerSpi extends TrustManagerFactorySpi {
        private TrustManagerFactory delegate;
        private X509Certificate trustedCertificate;
        private volatile TrustManager[] trustManagers;
        
        public void init(TrustManagerFactory delegate, X509Certificate trustedCertificate) {
            this.delegate = delegate;
            this.trustedCertificate = trustedCertificate;
        }
        
        @Override
        protected void engineInit(KeyStore keyStore) throws KeyStoreException {
            delegate.init(keyStore);
        }
        
        @Override
        protected void engineInit(ManagerFactoryParameters managerFactoryParameters) throws InvalidAlgorithmParameterException {
            delegate.init(managerFactoryParameters);
        }
        
        @Override
        protected TrustManager[] engineGetTrustManagers() {
            TrustManager[] trustManagers = this.trustManagers;
            if (trustManagers == null) {
                trustManagers = delegate.getTrustManagers();
                for (int i = 0; i < trustManagers.length; i++) {
                    final TrustManager tm = trustManagers[i];
                    if (tm instanceof X509TrustManager) {
                        trustManagers[i] = new DatawaveTrustManager((X509TrustManager) tm, trustedCertificate);
                    } else {
                        trustManagers[i] = tm;
                    }
                }
                this.trustManagers = trustManagers;
            }
            return trustManagers.clone();
        }
    }
    
    /**
     * An extension of {@link X509ExtendedTrustManager} that applies custom trust behavior for use with microservices. It is expected that these services will
     * run on a cluster where DNS might be used for service discovery. However, the server certificate used by the microservices will not have the DNS names as
     * Subject Alternative Name values, so normal trust for the certificate would fail. This trust manager trusts the remote server regardless of the host name
     * used to access it, so long as the presented certificate is the one we expect. Otherwise, it falls back to the normal trust behavior.
     */
    private static class DatawaveTrustManager extends X509ExtendedTrustManager {
        private X509Certificate trustedCertificate;
        private X509TrustManager delegate;
        private X509ExtendedTrustManager extendedDelegate;
        
        public DatawaveTrustManager(X509TrustManager delegate, X509Certificate trustedCertificate) {
            this.trustedCertificate = trustedCertificate;
            this.delegate = delegate;
            if (delegate instanceof X509ExtendedTrustManager) {
                extendedDelegate = (X509ExtendedTrustManager) delegate;
            }
        }
        
        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return delegate.getAcceptedIssuers();
        }
        
        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
            delegate.checkClientTrusted(x509Certificates, s);
        }
        
        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
            delegate.checkServerTrusted(x509Certificates, s);
        }
        
        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws CertificateException {
            Preconditions.checkNotNull(extendedDelegate, "Not wrapping an X509ExtendedTrustManager");
            boolean certChainTrusted = certChainTrusted(x509Certificates);
            if ((socket instanceof SSLSocket) && socket.isConnected() && certChainTrusted) {
                SSLSocket sslSocket = (SSLSocket) socket;
                SSLParameters origParameters = sslSocket.getSSLParameters();
                String curIdentityAlgorithm = origParameters.getEndpointIdentificationAlgorithm();
                try {
                    // Setting the identification algorithm to empty will prevent the hostname verification.
                    SSLParameters overrideParameters = sslSocket.getSSLParameters();
                    overrideParameters.setEndpointIdentificationAlgorithm("");
                    sslSocket.setSSLParameters(overrideParameters);
                    
                    extendedDelegate.checkClientTrusted(x509Certificates, s, socket);
                } finally {
                    origParameters.setEndpointIdentificationAlgorithm(curIdentityAlgorithm);
                    sslSocket.setSSLParameters(origParameters);
                }
            } else {
                extendedDelegate.checkClientTrusted(x509Certificates, s, socket);
            }
        }
        
        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws CertificateException {
            Preconditions.checkNotNull(extendedDelegate, "Not wrapping an X509ExtendedTrustManager");
            boolean certChainTrusted = certChainTrusted(x509Certificates);
            if ((socket instanceof SSLSocket) && socket.isConnected() && certChainTrusted) {
                SSLSocket sslSocket = (SSLSocket) socket;
                SSLParameters origParameters = sslSocket.getSSLParameters();
                String curIdentityAlgorithm = origParameters.getEndpointIdentificationAlgorithm();
                try {
                    // Setting the identification algorithm to empty will prevent the hostname verification.
                    SSLParameters overrideParameters = sslSocket.getSSLParameters();
                    overrideParameters.setEndpointIdentificationAlgorithm("");
                    sslSocket.setSSLParameters(overrideParameters);
                    extendedDelegate.checkServerTrusted(x509Certificates, s, socket);
                } finally {
                    origParameters.setEndpointIdentificationAlgorithm(curIdentityAlgorithm);
                    sslSocket.setSSLParameters(origParameters);
                }
            } else {
                extendedDelegate.checkServerTrusted(x509Certificates, s, socket);
            }
        }
        
        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws CertificateException {
            Preconditions.checkNotNull(extendedDelegate, "Not wrapping an X509ExtendedTrustManager");
            boolean certChainTrusted = certChainTrusted(x509Certificates);
            SSLParameters origParameters = sslEngine.getSSLParameters();
            String curIdentityAlgorithm = origParameters.getEndpointIdentificationAlgorithm();
            try {
                // Setting the identification algorithm to empty will prevent the hostname verification.
                if (certChainTrusted) {
                    SSLParameters overrideParameters = sslEngine.getSSLParameters();
                    overrideParameters.setEndpointIdentificationAlgorithm("");
                    sslEngine.setSSLParameters(overrideParameters);
                }
                
                extendedDelegate.checkClientTrusted(x509Certificates, s, sslEngine);
            } finally {
                origParameters.setEndpointIdentificationAlgorithm(curIdentityAlgorithm);
                sslEngine.setSSLParameters(origParameters);
            }
        }
        
        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws CertificateException {
            Preconditions.checkNotNull(extendedDelegate, "Not wrapping an X509ExtendedTrustManager");
            boolean certChainTrusted = certChainTrusted(x509Certificates);
            SSLParameters origParameters = sslEngine.getSSLParameters();
            String curIdentityAlgorithm = origParameters.getEndpointIdentificationAlgorithm();
            try {
                // Setting the identification algorithm to empty will prevent the hostname verification.
                if (certChainTrusted) {
                    SSLParameters overrideParameters = sslEngine.getSSLParameters();
                    overrideParameters.setEndpointIdentificationAlgorithm("");
                    sslEngine.setSSLParameters(overrideParameters);
                }
                extendedDelegate.checkServerTrusted(x509Certificates, s, sslEngine);
            } finally {
                origParameters.setEndpointIdentificationAlgorithm(curIdentityAlgorithm);
                sslEngine.setSSLParameters(origParameters);
            }
        }
        
        private boolean certChainTrusted(X509Certificate[] x509Certificates) {
            // If there was no peer certificate available or none matched the trusted certificate, then return
            // immediately so that we use the default trust verification instead of our custom verification.
            boolean peerCertIsTrusted = false;
            if (x509Certificates != null && trustedCertificate != null) {
                for (X509Certificate cert : x509Certificates) {
                    if (cert.getSubjectDN().equals(trustedCertificate.getSubjectDN()) && cert.getIssuerDN().equals(trustedCertificate.getIssuerDN())) {
                        peerCertIsTrusted = true;
                        break;
                    }
                }
            }
            return peerCertIsTrusted;
        }
    }
    
    private static class KeyAliasKeyManagerFactory extends KeyManagerFactory {
        private static final Provider PROVIDER = new Provider("", 0.0, "") {
            private static final long serialVersionUID = -2680540247105807895L;
        };
        private static final FastThreadLocal<KeyAliasKeyManagerSpi> CURRENT_SPI = new FastThreadLocal<KeyAliasKeyManagerSpi>() {
            @Override
            protected KeyAliasKeyManagerSpi initialValue() {
                return new KeyAliasKeyManagerSpi();
            }
        };
        
        public KeyAliasKeyManagerFactory(KeyManagerFactory delegate, String alias) {
            
            super(CURRENT_SPI.get(), PROVIDER, delegate.getAlgorithm());
            CURRENT_SPI.get().init(delegate, alias);
            CURRENT_SPI.remove();
        }
    }
    
    private static class KeyAliasKeyManagerSpi extends KeyManagerFactorySpi {
        private KeyManagerFactory delegate;
        private String alias;
        private volatile KeyManager[] keyManagers;
        
        public void init(KeyManagerFactory delegate, String alias) {
            this.delegate = delegate;
            this.alias = alias;
        }
        
        @Override
        protected void engineInit(KeyStore keyStore, char[] chars) throws KeyStoreException, NoSuchAlgorithmException, UnrecoverableKeyException {
            delegate.init(keyStore, chars);
        }
        
        @Override
        protected void engineInit(ManagerFactoryParameters managerFactoryParameters) throws InvalidAlgorithmParameterException {
            delegate.init(managerFactoryParameters);
        }
        
        @Override
        protected KeyManager[] engineGetKeyManagers() {
            KeyManager[] keyManagers = this.keyManagers;
            if (keyManagers == null) {
                keyManagers = delegate.getKeyManagers();
                for (int i = 0; i < keyManagers.length; i++) {
                    final KeyManager km = keyManagers[i];
                    if (km instanceof X509ExtendedKeyManager) {
                        keyManagers[i] = new KeyAliasKeyManager((X509ExtendedKeyManager) km, alias);
                    } else {
                        keyManagers[i] = km;
                    }
                }
                this.keyManagers = keyManagers;
            }
            return keyManagers.clone();
        }
    }
    
    private static class KeyAliasKeyManager extends X509ExtendedKeyManager {
        private final X509ExtendedKeyManager delegate;
        private final String alias;
        
        public KeyAliasKeyManager(X509ExtendedKeyManager delegate, String alias) {
            this.delegate = delegate;
            this.alias = alias;
        }
        
        @Override
        public String chooseEngineClientAlias(String[] strings, Principal[] principals, SSLEngine sslEngine) {
            return delegate.chooseEngineClientAlias(strings, principals, sslEngine);
        }
        
        @Override
        public String chooseEngineServerAlias(String s, Principal[] principals, SSLEngine sslEngine) {
            return (alias != null) ? alias : delegate.chooseEngineServerAlias(s, principals, sslEngine);
        }
        
        @Override
        public String[] getClientAliases(String s, Principal[] principals) {
            return delegate.getClientAliases(s, principals);
        }
        
        @Override
        public String chooseClientAlias(String[] strings, Principal[] principals, Socket socket) {
            return delegate.chooseClientAlias(strings, principals, socket);
        }
        
        @Override
        public String[] getServerAliases(String s, Principal[] principals) {
            return delegate.getServerAliases(s, principals);
        }
        
        @Override
        public String chooseServerAlias(String s, Principal[] principals, Socket socket) {
            return delegate.chooseServerAlias(s, principals, socket);
        }
        
        @Override
        public X509Certificate[] getCertificateChain(String s) {
            return delegate.getCertificateChain(s);
        }
        
        @Override
        public PrivateKey getPrivateKey(String s) {
            return delegate.getPrivateKey(s);
        }
    }
}
