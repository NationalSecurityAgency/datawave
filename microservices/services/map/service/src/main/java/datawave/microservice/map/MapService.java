package datawave.microservice.map;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

/**
 * Launcher for the map service
 */
@EnableDiscoveryClient
@SpringBootApplication(scanBasePackages = "datawave.microservice", exclude = {ErrorMvcAutoConfiguration.class})
public class MapService {
    public static void main(String[] args) {
        SpringApplication.run(MapService.class, args);
    }
}
