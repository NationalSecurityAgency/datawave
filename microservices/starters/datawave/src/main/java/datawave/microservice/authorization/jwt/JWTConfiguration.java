package datawave.microservice.authorization.jwt;

import java.security.Key;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.concurrent.TimeUnit;

import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.boot.web.server.Ssl;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.ResourceUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;

import datawave.microservice.authorization.config.DatawaveSecurityProperties;
import datawave.security.authorization.JWTTokenHandler;

/**
 * Provides configuration for working with JWTs: Provides a {@link GuavaModule} bean which will be picked up automatically by Spring when it creates any
 * {@link ObjectMapper}, thus allowing serialization to handle JSON-encoded Guava types.
 */
@Configuration
@ConditionalOnWebApplication
public class JWTConfiguration {
    
    @Bean
    public GuavaModule guavaModule() {
        return new GuavaModule();
    }
    
    @Bean
    @RefreshScope
    public JWTTokenHandler jwtTokenHandler(ServerProperties serverProperties, DatawaveSecurityProperties securityProperties, ObjectMapper objectMapper) {
        try {
            Ssl ssl = serverProperties.getSsl();
            String keyStoreType = ssl.getKeyStoreType();
            KeyStore keyStore = KeyStore.getInstance(keyStoreType == null ? "JKS" : keyStoreType);
            char[] keyPassword = ssl.getKeyPassword() != null ? ssl.getKeyPassword().toCharArray() : ssl.getKeyStorePassword().toCharArray();
            keyStore.load(ResourceUtils.getURL(ssl.getKeyStore()).openStream(), ssl.getKeyStorePassword().toCharArray());
            String alias = keyStore.aliases().nextElement();
            Key signingKey = keyStore.getKey(alias, keyPassword);
            Certificate cert = keyStore.getCertificate(alias);
            return new JWTTokenHandler(cert, signingKey, securityProperties.getJwt().getTtl(TimeUnit.SECONDS), TimeUnit.SECONDS, objectMapper);
        } catch (Exception e) {
            throw new IllegalStateException("Invalid SSL configuration.", e);
        }
    }
}
