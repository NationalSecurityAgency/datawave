package datawave.microservice.authorization.jwt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import datawave.microservice.authorization.config.DatawaveSecurityProperties;
import datawave.webservice.security.JWTTokenHandler;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.boot.context.embedded.Ssl;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.ResourceUtils;

import java.security.Key;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.concurrent.TimeUnit;

/**
 * Provides configuration for working with JWTs: Provides a {@link ObjectMapper} that includes the {@link GuavaModule} to handle JSON-encoded Guava types.
 */
@Configuration
public class JWTConfiguration {
    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new GuavaModule());
        return objectMapper;
    }
    
    @Bean
    @RefreshScope
    public JWTTokenHandler jwtTokenHandler(ServerProperties serverProperties, DatawaveSecurityProperties securityProperties, ObjectMapper objectMapper) {
        try {
            Ssl ssl = serverProperties.getSsl();
            String keyStoreType = ssl.getKeyStoreType();
            KeyStore keyStore = KeyStore.getInstance(keyStoreType == null ? "JKS" : keyStoreType);
            char[] keyPassword = ssl.getKeyPassword() != null ? ssl.getKeyPassword().toCharArray() : ssl.getKeyStorePassword().toCharArray();
            keyStore.load(ResourceUtils.getURL(ssl.getKeyStore()).openStream(), keyPassword);
            String alias = keyStore.aliases().nextElement();
            Key signingKey = keyStore.getKey(alias, ssl.getKeyStorePassword().toCharArray());
            Certificate cert = keyStore.getCertificate(alias);
            return new JWTTokenHandler(cert, signingKey, securityProperties.getJwt().getTtl(TimeUnit.SECONDS), TimeUnit.SECONDS, objectMapper);
        } catch (Exception e) {
            throw new IllegalStateException("Invalid SSL configuration.", e);
        }
    }
}
