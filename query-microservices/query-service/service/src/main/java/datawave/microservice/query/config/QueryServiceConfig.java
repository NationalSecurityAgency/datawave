package datawave.microservice.query.config;

import datawave.microservice.query.DefaultQueryParameters;
import datawave.microservice.query.QueryParameters;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.context.annotation.RequestScope;

@Configuration
public class QueryServiceConfig {

    @Bean
    @ConditionalOnMissingBean
    @RequestScope
    public QueryParameters queryParameters() {
        return new DefaultQueryParameters();
    }
    
    @Bean
    @ConfigurationProperties("datawave.query")
    public QueryProperties queryProperties() {
        return new QueryProperties();
    }
}
