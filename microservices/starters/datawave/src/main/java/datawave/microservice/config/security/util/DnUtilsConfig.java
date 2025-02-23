package datawave.microservice.config.security.util;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import datawave.microservice.security.util.DnUtils;

@Configuration
@ConditionalOnProperty(name = {"datawave.security.util.subjectDnPattern", "datawave.security.util.npeOuList"})
@EnableConfigurationProperties(DnUtilsProperties.class)
public class DnUtilsConfig {
    @Bean
    public DnUtils dnUtils(DnUtilsProperties dnUtilsProperties) {
        return new DnUtils(dnUtilsProperties.getCompiledSubjectDnPattern(), dnUtilsProperties.getNpeOuList());
    }
}
