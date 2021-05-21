package datawave.microservice.common.storage.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import static datawave.microservice.common.storage.queue.KafkaQueryQueueManager.KAFKA;

@Configuration
@ConditionalOnProperty(name = "query.storage.backend", havingValue = KAFKA)
public class KafkaQueryQueueManagerConfig {
    
    @Bean
    public AdminClient kafkaAdminClient(KafkaAdmin admin) {
        return AdminClient.create(admin.getConfigurationProperties());
    }
    
}
