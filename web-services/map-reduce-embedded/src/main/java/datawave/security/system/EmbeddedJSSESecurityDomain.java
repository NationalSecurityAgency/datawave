package datawave.security.system;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.Key;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Properties;

import javax.annotation.PostConstruct;
import javax.annotation.Priority;
import javax.enterprise.inject.Alternative;
import javax.inject.Inject;
import javax.interceptor.Interceptor;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.deltaspike.core.api.config.ConfigProperty;
import org.jboss.security.JSSESecurityDomain;
import org.jboss.security.PicketBoxMessages;

@Alternative
@Priority(Interceptor.Priority.APPLICATION)
public class EmbeddedJSSESecurityDomain implements JSSESecurityDomain {

    @Inject
    @ConfigProperty(name = "dw.mapreduce.securitydomain.keyStoreURL")
    private String keyStoreURLString;

    @Inject
    @ConfigProperty(name = "dw.mapreduce.securitydomain.keyStoreType")
    private String keyStoreType;

    @Inject
    @ConfigProperty(name = "dw.mapreduce.securitydomain.keyStorePassword")
    private String keyStorePassword;

    @Inject
    @ConfigProperty(name = "dw.mapreduce.securitydomain.trustStoreURL")
    private String trustStoreURLString;

    @Inject
    @ConfigProperty(name = "dw.mapreduce.securitydomain.trustStoreType")
    private String trustStoreType;

    @Inject
    @ConfigProperty(name = "dw.mapreduce.securitydomain.trustStorePassword")
    private String trustStorePassword;

    @Inject
    @ConfigProperty(name = "dw.mapreduce.securitydomain.clientAlias")
    private String clientAlias;

    @Inject
    @ConfigProperty(name = "dw.mapreduce.securitydomain.serverAlias")
    private String serverAlias;

    @Inject
    @ConfigProperty(name = "dw.mapreduce.securitydomain.cipherSuites")
    private String cipherSuitesString;

    @Inject
    @ConfigProperty(name = "dw.mapreduce.securitydomain.protocols")
    private String protocolsString;

    @Inject
    @ConfigProperty(name = "dw.mapreduce.securitydomain.clientAuth")
    private String clientAuthString;

    private KeyStore keyStore;
    private URL keyStoreURL;
    private KeyManagerFactory keyManagerFactory;
    private KeyManager[] keyManagers;
    private KeyStore trustStore;
    private URL trustStoreURL;
    private TrustManagerFactory trustManagerFactory;
    private TrustManager[] trustManagers;
    private boolean clientAuth;
    private String[] cipherSuites;
    private String[] protocols;
    private String name = this.getClass().getName();

    public EmbeddedJSSESecurityDomain() {

    }

    @PostConstruct
    public void initialize() throws Exception {

        if (this.cipherSuitesString != null) {
            this.cipherSuites = StringUtils.split(this.cipherSuitesString);
        }
        if (this.protocolsString != null) {
            this.protocols = StringUtils.split(this.protocolsString);
        }
        this.clientAuth = Boolean.parseBoolean(this.clientAuthString);

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream is = null;
        if (keyStorePassword != null) {
            this.keyStore = KeyStore.getInstance(this.keyStoreType);
            is = null;
            try {
                try {
                    this.keyStoreURL = new URL(this.keyStoreURLString);
                } catch (MalformedURLException e) {
                    this.keyStoreURL = loader.getResource(this.keyStoreURLString);
                }

                if (!"PKCS11".equalsIgnoreCase(this.keyStoreType) && !"PKCS11IMPLKS".equalsIgnoreCase(this.keyStoreType)) {
                    if (this.keyStoreURL == null) {
                        throw PicketBoxMessages.MESSAGES.invalidNullKeyStoreURL(this.keyStoreType);
                    }
                    is = this.keyStoreURL.openStream();
                }
                this.keyStore.load(is, this.keyStorePassword.toCharArray());
            } finally {
                safeClose(is);
            }

            this.keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            this.keyManagerFactory.init(this.keyStore, this.keyStorePassword.toCharArray());
            this.keyManagers = this.keyManagerFactory.getKeyManagers();
        }

        if (trustStorePassword != null) {

            this.trustStore = KeyStore.getInstance(this.trustStoreType);
            is = null;
            try {
                try {
                    this.trustStoreURL = new URL(this.trustStoreURLString);
                } catch (MalformedURLException e) {
                    this.trustStoreURL = loader.getResource(this.trustStoreURLString);
                }

                if (!"PKCS11".equalsIgnoreCase(this.trustStoreType) && !"PKCS11IMPLKS".equalsIgnoreCase(this.trustStoreType)) {
                    if (this.trustStoreURL == null) {
                        throw PicketBoxMessages.MESSAGES.invalidNullKeyStoreURL(this.trustStoreType);
                    }
                    is = this.trustStoreURL.openStream();
                }
                this.trustStore.load(is, this.trustStorePassword.toCharArray());
            } finally {
                safeClose(is);
            }

            this.trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            this.trustManagerFactory.init(this.trustStore);
            this.trustManagers = this.trustManagerFactory.getTrustManagers();
        } else if (this.keyStore != null) {
            this.trustStore = this.keyStore;
            this.trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            this.trustManagerFactory.init(this.trustStore);
            this.trustManagers = this.trustManagerFactory.getTrustManagers();
        }
    }

    private void safeClose(InputStream fis) {
        try {
            if (fis != null) {
                fis.close();
            }
        } catch (Exception e) {
            ;
        }
    }

    @Override
    public KeyStore getKeyStore() throws SecurityException {
        return this.keyStore;
    }

    @Override
    public KeyManager[] getKeyManagers() throws SecurityException {
        return this.keyManagers;
    }

    @Override
    public KeyStore getTrustStore() throws SecurityException {
        return this.trustStore;
    }

    @Override
    public TrustManager[] getTrustManagers() throws SecurityException {
        return this.trustManagers;
    }

    @Override
    public void reloadKeyAndTrustStore() throws Exception {
        initialize();
    }

    @Override
    public String getServerAlias() {
        return this.serverAlias;
    }

    @Override
    public String getClientAlias() {
        return this.clientAlias;
    }

    @Override
    public boolean isClientAuth() {
        return this.clientAuth;
    }

    @Override
    public Key getKey(String alias, String serviceAuthToken) throws Exception {
        return this.keyStore.getKey(alias, this.keyStorePassword.toCharArray());
    }

    @Override
    public Certificate getCertificate(String alias) throws Exception {
        return this.trustStore.getCertificate(alias);
    }

    @Override
    public String[] getCipherSuites() {
        return this.cipherSuites;
    }

    @Override
    public String[] getProtocols() {
        return this.protocols;
    }

    @Override
    public Properties getAdditionalProperties() {
        return null;
    }

    @Override
    public String getSecurityDomain() {
        return this.name;
    }
}
