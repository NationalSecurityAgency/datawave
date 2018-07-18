package datawave.microservice.audit;

import datawave.microservice.audit.config.AuditServiceConfig;
import datawave.webservice.common.audit.AuditParameters;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.ErrorMvcAutoConfiguration;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.context.annotation.Bean;
import org.springframework.web.context.annotation.RequestScope;

@EnableDiscoveryClient
@SpringBootApplication(scanBasePackages = "datawave.microservice")
@EnableAutoConfiguration(exclude = {ErrorMvcAutoConfiguration.class})
@EnableBinding(AuditServiceConfig.AuditSourceBinding.class)
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
