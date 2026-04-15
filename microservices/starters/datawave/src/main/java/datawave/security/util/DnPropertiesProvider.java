package datawave.security.util;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = {"datawave.security.util.subjectDnPattern", "datawave.security.util.npeOuList"})
@EnableConfigurationProperties(DnPropertiesConfig.class)
public class DnPropertiesProvider {

    @Bean
    public DnProperties dnProperties(DnPropertiesConfig config) {
        return new DnProperties(config.getCompiledSubjectDnPattern(), config.getNpeOuList());
    }
}
