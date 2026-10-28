package datawave.webservice.config;

import java.util.List;
import java.util.Map;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "datawave.modification.enabled", havingValue = "true", matchIfMissing = true)
public class ModificationHandlerConfiguration {

    @Bean
    public ModificationHandlerProperties modificationHandlerProperties() {
        return new ModificationHandlerProperties();
    }

    @Bean
    public List<String> authorizedRoles(ModificationHandlerProperties modificationHandlerProperties) {
        return modificationHandlerProperties.getAuthorizedRoles();
    }

    @Bean
    public List<String> securityMarkingExemptFields(ModificationHandlerProperties modificationHandlerProperties) {
        return modificationHandlerProperties.getSecurityMarkingExemptFields();
    }

    @Bean
    public Map<String,String> indexOnlyMap(ModificationHandlerProperties modificationHandlerProperties) {
        return modificationHandlerProperties.getIndexOnlyMap();
    }

    @Bean
    public List<String> indexOnlySuffixes(ModificationHandlerProperties modificationHandlerProperties) {
        return modificationHandlerProperties.getIndexOnlySuffixes();
    }

    @Bean
    public List<String> contentFields(ModificationHandlerProperties modificationHandlerProperties) {
        return modificationHandlerProperties.getContentFields();
    }
}
