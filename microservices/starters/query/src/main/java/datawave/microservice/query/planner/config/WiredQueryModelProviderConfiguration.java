package datawave.microservice.query.planner.config;

import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WiredQueryModelProviderConfiguration {
    
    @Bean
    @ConfigurationProperties("datawave.query.planner.wired-query-model-provider")
    public WiredQueryModelProviderProperties wiredQueryModelProviderProperties() {
        return new WiredQueryModelProviderProperties();
    }
    
    @Bean
    public Map<String,String> wiredQueryModelProviderQueryModel() {
        return wiredQueryModelProviderProperties().getWiredQueryModelProviderQueryModel();
    }
}
