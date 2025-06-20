package datawave.security.system;

import javax.validation.constraints.NotBlank;

import org.springframework.validation.annotation.Validated;

import datawave.security.cert.SSLStores;
import datawave.security.cert.SSLStoresImpl;

/**
 * A class for producing server-security related artifacts.
 */
@Validated
public class SSLStoresProducer {

    @NotBlank
    String keyStoreType;
    @NotBlank
    String keyStoreUrl;
    @NotBlank
    String keyStorePassword;
    @NotBlank
    String trustStoreType;
    @NotBlank
    String trustStoreUrl;
    @NotBlank
    String trustStorePassword;

    public void setKeyStoreType(String keyStoreType) {
        this.keyStoreType = keyStoreType;
    }

    public void setKeyStoreUrl(String keyStoreUrl) {
        this.keyStoreUrl = keyStoreUrl;
    }

    public void setKeyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
    }

    public void setTrustStoreType(String trustStoreType) {
        this.trustStoreType = trustStoreType;
    }

    public void setTrustStoreUrl(String trustStoreUrl) {
        this.trustStoreUrl = trustStoreUrl;
    }

    public void setTrustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
    }

    /**
     * Allow injection of an {@link SSLStores} instance that is instantiated via properties via Spring. This is intended to be the default way to configure the
     * instance of {@link SSLStores} used throughout the application.
     *
     * @return the new {@link SSLStores} instance
     * @throws Exception
     *             if an error occurs while creating the {@link SSLStores} instance
     */
    public SSLStores produceSSlStores() throws Exception {
        // @formatter:off
        return SSLStoresImpl.builder()
                        .withKeystore(keyStoreUrl, keyStorePassword, keyStoreType)
                        .withTruststore(trustStoreUrl, trustStorePassword, trustStoreType)
                        .build();
        // @formatter:on
    }
}
