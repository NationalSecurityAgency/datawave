package datawave.microservice.querymetric.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({CorrelatorProperties.class})
public class CorrelatorConfiguration {
    
}
