package datawave.microservice.modification.config;

import java.util.List;
import java.util.Map;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({ModificationQueryProperties.class, ModificationDataProperties.class, ModificationHandlerProperties.class})
public class ModificationHandlerConfiguration {
    
    @Bean
    public List<String> authorizedRoles(ModificationHandlerProperties props) {
        return props.getAuthorizedRoles();
    }
    
    @Bean
    public List<String> securityMarkingExemptFields(ModificationHandlerProperties props) {
        return props.getSecurityMarkingExemptFields();
    }
    
    @Bean
    public Map<String,String> indexOnlyMap(ModificationHandlerProperties props) {
        return props.getIndexOnlyMap();
    }
    
    @Bean
    public List<String> indexOnlySuffixes(ModificationHandlerProperties props) {
        return props.getIndexOnlySuffixes();
    }
    
    @Bean
    public List<String> contentFields(ModificationHandlerProperties props) {
        return props.getContentFields();
    }
}
