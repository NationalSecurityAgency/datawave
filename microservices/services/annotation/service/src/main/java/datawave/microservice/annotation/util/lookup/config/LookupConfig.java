package datawave.microservice.annotation.util.lookup.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import datawave.microservice.annotation.config.AnnotationRole;
import datawave.microservice.annotation.config.ConditionalOnAnnotationRole;

@Configuration
@ConditionalOnAnnotationRole({AnnotationRole.API, AnnotationRole.COMBINED})
public class LookupConfig {
    @Bean
    @ConditionalOnMissingBean(LookupProperties.class)
    @ConfigurationProperties("annotation.lookup")
    public LookupProperties lookupProperties() {
        return new LookupProperties();
    }
}
