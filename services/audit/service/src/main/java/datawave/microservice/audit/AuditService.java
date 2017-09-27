package datawave.microservice.audit;

import datawave.microservice.audit.config.AuditProperties;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class AuditService {
    public static void main(String[] args) {
        SpringApplication.run(AuditService.class, args);
    }
}
