package datawave.microservice.query.messaging.kafka.config;

import static datawave.microservice.query.messaging.kafka.KafkaQueryResultsManager.KAFKA;
import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.ProducerFactory;

import datawave.microservice.query.messaging.QueryResultsManager;
import datawave.microservice.query.messaging.config.MessagingProperties;
import datawave.microservice.query.messaging.kafka.KafkaQueryResultsManager;

@Configuration
@ConditionalOnProperty(name = "datawave.query.messaging.backend", havingValue = KAFKA)
public class KafkaMessagingConfiguration {
    
    @Bean
    public QueryResultsManager kafkaQueryResultsManager(MessagingProperties messagingProperties, @Autowired(required = false) KafkaAdmin kafkaAdmin,
                    @Autowired(required = false) ProducerFactory<String,String> kafkaProducerFactory,
                    @Autowired(required = false) ConsumerFactory<String,String> kafkaConsumerFactory) {
        Map<String,Object> kafkaConfigProps = createKafkaConfigProps(messagingProperties);
        // @formatter:off
        return new KafkaQueryResultsManager(
                messagingProperties,
                createAdminClient(messagingProperties, kafkaConfigProps, kafkaAdmin),
                createProducerFactory(messagingProperties, kafkaConfigProps, kafkaProducerFactory),
                createConsumerFactory(messagingProperties, kafkaConfigProps, kafkaConsumerFactory));
        // @formatter:on
    }
    
    public Map<String,Object> createKafkaConfigProps(MessagingProperties messagingProperties) {
        MessagingProperties.KafkaInstanceSettings instanceSettings = messagingProperties.getKafka().getInstanceSettings();
        
        Map<String,Object> configProps = new HashMap<>();
        if (instanceSettings.getBootstrapServers() != null) {
            configProps.put(BOOTSTRAP_SERVERS_CONFIG, instanceSettings.getBootstrapServers());
        }
        
        if (instanceSettings.getAutoOffsetReset() != null) {
            configProps.put(AUTO_OFFSET_RESET_CONFIG, instanceSettings.getAutoOffsetReset().name().toLowerCase());
        }
        
        if (instanceSettings.isEnableAutoCommit() != null) {
            configProps.put(ENABLE_AUTO_COMMIT_CONFIG, instanceSettings.isEnableAutoCommit());
        }
        
        if (instanceSettings.isAllowAutoCreateTopics() != null) {
            configProps.put(ALLOW_AUTO_CREATE_TOPICS_CONFIG, instanceSettings.isAllowAutoCreateTopics());
        }
        
        return configProps;
    }
    
    public AdminClient createAdminClient(MessagingProperties messagingProperties, Map<String,Object> queryKafkaConfigProps, KafkaAdmin kafkaAdmin) {
        AdminClient finalClient = null;
        
        if (messagingProperties.getKafka().isUseDedicatedInstance()) {
            finalClient = AdminClient.create(queryKafkaConfigProps);
        } else {
            finalClient = AdminClient.create(kafkaAdmin.getConfigurationProperties());
        }
        
        return finalClient;
    }
    
    public ProducerFactory<String,String> createProducerFactory(MessagingProperties messagingProperties, Map<String,Object> queryKafkaConfigProps,
                    ProducerFactory<String,String> kafkaProducerFactory) {
        ProducerFactory<String,String> finalKafkaProducerFactory = null;
        
        if (messagingProperties.getKafka().isUseDedicatedInstance()) {
            // @formatter:off
            finalKafkaProducerFactory = new DefaultKafkaProducerFactory<>(
                    queryKafkaConfigProps,
                    new StringSerializer(),
                    new StringSerializer());
            // @formatter:on
        } else {
            finalKafkaProducerFactory = kafkaProducerFactory;
        }
        
        return finalKafkaProducerFactory;
    }
    
    public ConsumerFactory<String,String> createConsumerFactory(MessagingProperties messagingProperties, Map<String,Object> queryKafkaConfigProps,
                    ConsumerFactory<String,String> kafkaConsumerFactory) {
        ConsumerFactory<String,String> finalKafkaConsumerFactory = null;
        
        if (messagingProperties.getKafka().isUseDedicatedInstance()) {
            // @formatter:off
            finalKafkaConsumerFactory = new DefaultKafkaConsumerFactory<>(
                    queryKafkaConfigProps,
                    new StringDeserializer(),
                    new StringDeserializer());
            // @formatter:on
        } else {
            finalKafkaConsumerFactory = kafkaConsumerFactory;
        }
        
        return finalKafkaConsumerFactory;
    }
}
