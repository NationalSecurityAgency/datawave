package datawave.microservice.query;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ImportResource;
import org.springframework.web.context.annotation.RequestScope;

/**
 * Launcher for the query service
 */
@EnableDiscoveryClient
@SpringBootApplication(scanBasePackages = "datawave.microservice", exclude = {ErrorMvcAutoConfiguration.class})
@ImportResource("${queryLogicFactoryLocation:classpath:QueryLogicFactory.xml}")
public class QueryService {
    
    @Bean
    @RequestScope
    public QueryParameters queryParameters() {
        return new QueryParametersImpl();
    }
    
    public static void main(String[] args) {
        SpringApplication.run(QueryService.class, args);
    }
}
