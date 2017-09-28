package datawave.microservice.audit;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@EnableDiscoveryClient
@SpringBootApplication
public class AuditService {
    public static void main(String[] args) {
        SpringApplication.run(AuditService.class, args);
    }
}
