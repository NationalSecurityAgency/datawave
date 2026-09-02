package datawave.microservice.security.util;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import datawave.security.util.DnProperties;

@Configuration
@ConditionalOnProperty(name = {"datawave.security.util.subjectDnPattern", "datawave.security.util.npeOuList"})
@EnableConfigurationProperties(DnPropertiesConfig.class)
public class DnPropertiesProvider {

    @Bean
    public DnProperties dnProperties(DnPropertiesConfig config) {
        return new DnProperties(config.getCompiledSubjectDnPattern(), config.getNpeOuList());
    }
}
