package datawave.microservice.query.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.validation.Valid;

@Configuration
public class QueryServiceConfig {
    
    @Bean
    @ConfigurationProperties("query")
    public QueryProperties queryProperties() {
        return new QueryProperties();
    }
}
