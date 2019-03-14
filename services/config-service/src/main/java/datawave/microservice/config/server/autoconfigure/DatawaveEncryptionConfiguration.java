package datawave.microservice.config.server.autoconfigure;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.bootstrap.encrypt.KeyProperties;
import org.springframework.cloud.bootstrap.encrypt.RsaProperties;
import org.springframework.cloud.config.server.encryption.KeyStoreTextEncryptorLocator;
import org.springframework.cloud.config.server.encryption.TextEncryptorLocator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.rsa.crypto.RsaAlgorithm;
import org.springframework.security.rsa.crypto.RsaSecretEncryptor;

/**
 * Overrides {@link org.springframework.cloud.config.server.config.EncryptionAutoConfiguration} to provide a {@link TextEncryptorLocator} that uses a
 * {@link DatawaveKeyStoreKeyFactory} instead of a {@link org.springframework.security.rsa.crypto.KeyStoreKeyFactory}.
 */
@Configuration
@ConditionalOnClass(RsaSecretEncryptor.class)
@ConditionalOnProperty(prefix = "encrypt.key-store", value = "location", matchIfMissing = false)
public class DatawaveEncryptionConfiguration {
    private final KeyProperties key;
    private final RsaProperties rsaProperties;
    
    @Autowired
    public DatawaveEncryptionConfiguration(KeyProperties key, RsaProperties rsaProperties) {
        this.key = key;
        this.rsaProperties = rsaProperties;
    }
    
    @Bean
    @ConditionalOnMissingBean
    public TextEncryptorLocator textEncryptorLocator() {
        KeyProperties.KeyStore keyStore = this.key.getKeyStore();
        KeyStoreTextEncryptorLocator locator = new KeyStoreTextEncryptorLocator(
                        new DatawaveKeyStoreKeyFactory(keyStore.getLocation(), keyStore.getPassword().toCharArray()), keyStore.getSecret(),
                        keyStore.getAlias());
        RsaAlgorithm algorithm = this.rsaProperties.getAlgorithm();
        locator.setRsaAlgorithm(algorithm);
        locator.setSalt(this.rsaProperties.getSalt());
        locator.setStrong(this.rsaProperties.isStrong());
        return locator;
    }
}
