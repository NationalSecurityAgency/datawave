package datawave.microservice.query.config;

import datawave.microservice.query.QueryParameters;
import datawave.microservice.query.QueryParametersImpl;
import datawave.microservice.query.result.event.DefaultResponseObjectFactory;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.web.context.annotation.RequestScope;

@Configuration
public class QueryServiceConfig {
    
    @Bean
    @Order(Ordered.LOWEST_PRECEDENCE)
    @RequestScope
    public QueryParameters queryParameters() {
        return new QueryParametersImpl();
    }
    
    @Bean
    @Order(Ordered.LOWEST_PRECEDENCE)
    public ResponseObjectFactory responseObjectFactory() {
        return new DefaultResponseObjectFactory();
    }
    
    @Bean
    @ConfigurationProperties("query")
    public QueryProperties queryProperties() {
        return new QueryProperties();
    }
}
