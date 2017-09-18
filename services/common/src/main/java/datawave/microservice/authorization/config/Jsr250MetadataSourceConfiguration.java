package datawave.microservice.authorization.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.access.annotation.Jsr250MethodSecurityMetadataSource;

/**
 * Create a source for {@link Jsr250MethodSecurityMetadataSource} beans, to ensure that the created {@link Jsr250MethodSecurityMetadataSource} has its
 * {@link Jsr250MethodSecurityMetadataSource#defaultRolePrefix} value set to the empty string. We don't want "ROLE_" prefixed on every role we test in the
 * application.
 */
@Configuration
public class Jsr250MetadataSourceConfiguration {
    
    @Bean
    public Jsr250MethodSecurityMetadataSource jsr250MethodSecurityMetadataSource() {
        Jsr250MethodSecurityMetadataSource source = new Jsr250MethodSecurityMetadataSource();
        source.setDefaultRolePrefix("");
        return source;
    }
}
