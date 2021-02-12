package datawave.microservice.common.storage;

import datawave.microservice.common.storage.config.QueryStorageConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.stream.annotation.EnableBinding;

/**
 * Launcher for the query storage service
 */
@EnableDiscoveryClient
@SpringBootApplication(scanBasePackages = "datawave.microservice", exclude = {ErrorMvcAutoConfiguration.class})
public class QueryStorageLauncher {
    public static void main(String[] args) {
        SpringApplication.run(QueryStorageLauncher.class, args);
    }
}
