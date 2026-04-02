package datawave.microservice.fileProvider;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

/**
 * Launcher for the file provider service
 */
@EnableDiscoveryClient
@SpringBootApplication(scanBasePackages = "datawave.microservice", exclude = {ErrorMvcAutoConfiguration.class})
public class FileProviderService {
    public static void main(String[] args) {
        SpringApplication.run(FileProviderService.class, args);
    }
}
