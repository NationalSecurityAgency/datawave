package datawave.microservice.audit.config;

import org.springframework.amqp.core.FanoutExchange;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(AuditProperties.class)
public class AuditServiceConfig {
    @Autowired
    private AuditProperties auditProperties;
    
    @Bean
    public FanoutExchange auditExchange() {
        return new FanoutExchange(auditProperties.getExchangeName());
    }
}
