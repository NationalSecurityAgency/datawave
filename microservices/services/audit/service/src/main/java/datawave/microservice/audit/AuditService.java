package datawave.microservice.audit;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.web.context.annotation.RequestScope;

import datawave.webservice.common.audit.AuditParameters;

/**
 * Launcher for the audit service
 */
@EnableDiscoveryClient
@SpringBootApplication(scanBasePackages = "datawave.microservice", exclude = {ErrorMvcAutoConfiguration.class})
public class AuditService {
    
    @Bean("restAuditParams")
    @RequestScope
    public AuditParameters restAuditParams() {
        return new AuditParameters();
    }
    
    @Bean("msgHandlerAuditParams")
    public AuditParameters msgHandlerAuditParams() {
        return new AuditParameters();
    }
    
    public static void main(String[] args) {
        SpringApplication.run(AuditService.class, args);
    }
}
