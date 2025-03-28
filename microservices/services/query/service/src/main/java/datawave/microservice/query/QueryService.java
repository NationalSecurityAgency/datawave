package datawave.microservice.query;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

/**
 * Launcher for the query service
 */
@EnableDiscoveryClient
@SpringBootApplication(scanBasePackages = "datawave.microservice", exclude = {ErrorMvcAutoConfiguration.class})
public class QueryService {
    public static void main(String[] args) {
        SpringApplication.run(QueryService.class, args);
    }
}
