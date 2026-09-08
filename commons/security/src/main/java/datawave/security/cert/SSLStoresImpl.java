package datawave.security.cert;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import com.google.common.base.Preconditions;

/**
 * Represents a key store/trust store pair.
 */
public class SSLStoresImpl implements SSLStores {

    private final KeyStore keyStore;
    private final KeyManager[] keyManagers;

    private final KeyStore trustStore;
    private final TrustManager[] trustManagers;

    /**
     * Return a new builder.
     *
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    public SSLStoresImpl(KeyStore keyStore, KeyManager[] keyManagers, KeyStore trustStore, TrustManager[] trustManagers) {
        this.keyStore = keyStore;
        this.keyManagers = keyManagers;
        this.trustStore = trustStore;
        this.trustManagers = trustManagers;
    }

    @Override
    public KeyStore getKeyStore() {
        return this.keyStore;
    }

    @Override
    public KeyManager[] getKeyManagers() {
        return this.keyManagers;
    }

    @Override
    public KeyStore getTrustStore() {
        return this.trustStore;
    }

    @Override
    public TrustManager[] getTrustManagers() {
        return this.trustManagers;
    }

    public static class Builder {
        private String keyStoreUrl;
        private String keyStoreType;
        private String keyStorePassword;
        private String trustStoreUrl;
        private String trustStoreType;
        private String trustStorePassword;

        /**
         * Set the information required to load the keystore.
         *
         * @param url
         *            the keystore URL, this may be a URL, file path, or classloader resource
         * @param password
         *            the keystore password
         * @param type
         *            the keystore type
         * @return this builder
         */
        public Builder withKeystore(String url, String password, String type) {
            Preconditions.checkNotNull(url, "keystore URL cannot be null");
            Preconditions.checkNotNull(password, "keystore password cannot be null");
            Preconditions.checkNotNull(type, "keystore type cannot be null");
            this.keyStoreUrl = url;
            this.keyStoreType = type;
            this.keyStorePassword = password;
            return this;
        }

        /**
         * Set the information required to load the trust store.
         *
         * @param url
         *            the trust store URL, this may be a URL, file path, or classpath resource
         * @param password
         *            the trust store password
         * @param type
         *            the trust store type
         * @return this builder
         */
        public Builder withTruststore(String url, String password, String type) {
            Preconditions.checkNotNull(url, "truststore URL cannot be null");
            Preconditions.checkNotNull(password, "truststore password cannot be null");
            Preconditions.checkNotNull(type, "truststore type cannot be null");
            this.trustStoreUrl = url;
            this.trustStoreType = type;
            this.trustStorePassword = password;
            return this;
        }

        /**
         * Build and return an {@link SSLStoresImpl} instance.
         *
         * @return the new instance
         * @throws Exception
         *             if an error occurs while loading the keystore and truststore
         */
        public SSLStoresImpl build() throws Exception {
            // Load the keystore.
            char[] keyStorePassword = this.keyStorePassword.toCharArray();
            KeyStore keyStore = getKeyStore(keyStoreUrl, keyStorePassword, keyStoreType);
            KeyManager[] keyManagers = getKeyManagers(keyStore, keyStorePassword);

            KeyStore trustStore;
            // If no trust store URL was set, use the keystore as the trust store.
            if (trustStoreUrl != null) {
                trustStore = getKeyStore(trustStoreUrl, trustStorePassword.toCharArray(), trustStoreType);
            } else {
                trustStore = keyStore;
            }
            TrustManager[] trustManagers = getTrustManagers(trustStore);

            return new SSLStoresImpl(keyStore, keyManagers, trustStore, trustManagers);
        }

        private static final String PKCS11 = "PKCS11";
        private static final String PKCS11IMPLKS = "PKCS11IMPLKS";

        /**
         * Load and return a {@link KeyStore}.
         *
         * @param url
         *            the keystore URL
         * @param password
         *            the keystore password
         * @param type
         *            the keystore type
         * @return the keystore
         */
        private KeyStore getKeyStore(final String url, final char[] password, String type)
                        throws IOException, KeyStoreException, CertificateException, NoSuchAlgorithmException {
            // If no type was specified, use JKS by default.
            KeyStore keyStore = KeyStore.getInstance(type);
            // If the type is not PKCS11, load the keystore from the URL stream.
            if (!PKCS11.equalsIgnoreCase(type) && !PKCS11IMPLKS.equalsIgnoreCase(type)) {
                URL keyStoreUrl = validateKeystoreUrl(url);
                try (InputStream is = keyStoreUrl.openStream()) {
                    keyStore.load(is, password);
                }
            }

            return keyStore;
        }

        /**
         * Return the key managers from the given keystore.
         *
         * @param keyStore
         *            the keystore
         * @param password
         *            the keystore password
         * @return the key managers
         */
        private KeyManager[] getKeyManagers(KeyStore keyStore, char[] password) throws NoSuchAlgorithmException, UnrecoverableKeyException, KeyStoreException {
            String algorithm = KeyManagerFactory.getDefaultAlgorithm();
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(algorithm);
            keyManagerFactory.init(keyStore, password);
            return keyManagerFactory.getKeyManagers();
        }

        /**
         * Return the trust managers from the given truststore.
         *
         * @param trustStore
         *            the truststore
         * @return the trust managers
         */
        private TrustManager[] getTrustManagers(KeyStore trustStore) throws NoSuchAlgorithmException, KeyStoreException {
            String algorithm = TrustManagerFactory.getDefaultAlgorithm();
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(algorithm);
            trustManagerFactory.init(trustStore);
            return trustManagerFactory.getTrustManagers();
        }

        /**
         * Validate the given URL string and verify that it is a valid URL, file path, or classpath resource.
         *
         * @param urlStr
         *            the URL string
         * @return the URL
         */
        private URL validateKeystoreUrl(String urlStr) throws MalformedURLException {
            // First, try to parse it as a URL.
            try {
                return new URL(urlStr);
            } catch (MalformedURLException e) {
                // Either not a URL or protocol without a handler.
            }

            // Next, try to locate this as a file path.
            File file = new File(urlStr);
            if (file.exists()) {
                return file.toURI().toURL();
            }

            // Next, try to locate this as a classpath resource.
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            if (classLoader != null) {
                URL url = classLoader.getResource(urlStr);
                if (url != null) {
                    return url;
                }
            }

            throw new MalformedURLException("Failed to validate " + urlStr + " as a URL, file, or classpath resource.");
        }
    }
}
