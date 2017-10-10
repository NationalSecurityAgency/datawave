package datawave.microservice.audit;

import datawave.microservice.audit.config.AuditServiceConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.ErrorMvcAutoConfiguration;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.stream.annotation.EnableBinding;

@EnableDiscoveryClient
@SpringBootApplication(scanBasePackages = "datawave.microservice")
@EnableAutoConfiguration(exclude = {ErrorMvcAutoConfiguration.class})
@EnableBinding(AuditServiceConfig.AuditSourceBinding.class)
public class AuditService {
    public static void main(String[] args) {
        SpringApplication.run(AuditService.class, args);
    }
}
