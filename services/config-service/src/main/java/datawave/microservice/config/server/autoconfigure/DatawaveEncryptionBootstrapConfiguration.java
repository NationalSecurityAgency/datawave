package datawave.microservice.config.server.autoconfigure;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.bootstrap.encrypt.EncryptionBootstrapConfiguration;
import org.springframework.cloud.bootstrap.encrypt.KeyProperties;
import org.springframework.cloud.bootstrap.encrypt.RsaProperties;
import org.springframework.cloud.context.encrypt.EncryptorFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.crypto.encrypt.TextEncryptor;
import org.springframework.security.rsa.crypto.RsaSecretEncryptor;

/**
 * Overrides {@link EncryptionBootstrapConfiguration} in order to provide a {@link TextEncryptor} that uses a {@link DatawaveKeyStoreKeyFactory} instead of a
 * {@link org.springframework.security.rsa.crypto.KeyStoreKeyFactory}.
 */
@Configuration
@Conditional(EncryptionBootstrapConfiguration.KeyCondition.class)
public class DatawaveEncryptionBootstrapConfiguration {
    private final KeyProperties keyProperties;
    private final RsaProperties rsaProperties;
    
    @Autowired
    public DatawaveEncryptionBootstrapConfiguration(KeyProperties keyProperties, RsaProperties rsaProperties) {
        this.keyProperties = keyProperties;
        this.rsaProperties = rsaProperties;
    }
    
    @Bean
    public TextEncryptor textEncryptor() {
        KeyProperties.KeyStore keyStore = this.keyProperties.getKeyStore();
        if (keyStore.getLocation() != null) {
            if (keyStore.getLocation().exists()) {
                return new RsaSecretEncryptor(
                                new DatawaveKeyStoreKeyFactory(keyStore.getLocation(), keyStore.getPassword().toCharArray()).getKeyPair(keyStore.getAlias(),
                                                keyStore.getSecret().toCharArray()),
                                this.rsaProperties.getAlgorithm(), this.rsaProperties.getSalt(), this.rsaProperties.isStrong());
            }
            
            throw new IllegalStateException("Invalid keystore location");
        }
        
        return new EncryptorFactory(this.keyProperties.getSalt()).create(this.keyProperties.getKey());
    }
}
