package datawave.security.system;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Default;
import javax.enterprise.inject.Produces;

import datawave.security.cert.SSLStores;
import datawave.security.cert.SSLStoresImpl;

/**
 * A producer class for producing server-security related artifacts.
 */
@ApplicationScoped
public class SSLStoresProducer {

    /**
     * Allow injection of an {@link SSLStores} instance that is instantiated via system properties set via wildfly. This is intended to be the default way to
     * configure the instance of {@link SSLStores} used throughout the application.
     *
     * @return the new {@link SSLStores} instance
     * @throws Exception
     *             if an error occurs while creating the {@link SSLStores} instance
     */
    @Produces
    @Default
    public SSLStores produceSSlStores() throws Exception {
        String keyStoreUrl = System.getProperty("dw.ssl.context.info.keyStoreURL");
        String keyStorePassword = System.getProperty("dw.ssl.context.info.keyStorePassword");
        String keyStoreType = System.getProperty("dw.ssl.context.info.keyStoreType");
        String trustStoreUrl = System.getProperty("dw.ssl.context.info.trustStoreURL");
        String trustStorePassword = System.getProperty("dw.ssl.context.info.trustStorePassword");
        String trustStoreType = System.getProperty("dw.ssl.context.info.trustStoreType");

        // @formatter:off
        return SSLStoresImpl.builder()
                        .withKeystore(keyStoreUrl, keyStorePassword, keyStoreType)
                        .withTruststore(trustStoreUrl, trustStorePassword, trustStoreType)
                        .build();
        // @formatter:on
    }

}
