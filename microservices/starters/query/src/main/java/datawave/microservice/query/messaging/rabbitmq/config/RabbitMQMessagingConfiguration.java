package datawave.microservice.query.messaging.rabbitmq.config;

import static datawave.microservice.query.messaging.rabbitmq.RabbitMQQueryResultsManager.RABBITMQ;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.rabbitmq.client.ConnectionFactory;

import datawave.microservice.query.messaging.ClaimCheck;
import datawave.microservice.query.messaging.QueryResultsManager;
import datawave.microservice.query.messaging.config.MessagingProperties;
import datawave.microservice.query.messaging.rabbitmq.RabbitMQQueryResultsManager;

@Configuration
@ConditionalOnProperty(name = "datawave.query.messaging.backend", havingValue = RABBITMQ)
public class RabbitMQMessagingConfiguration {
    
    @Bean
    public QueryResultsManager rabbitMQQueryResultsManager(MessagingProperties messagingProperties,
                    RabbitListenerEndpointRegistry rabbitListenerEndpointRegistry, @Autowired(required = false) CachingConnectionFactory connectionFactory,
                    @Autowired(required = false) ClaimCheck claimCheck) {
        // @formatter:off
        return new RabbitMQQueryResultsManager(
                messagingProperties,
                rabbitListenerEndpointRegistry,
                createCachingConnectionFactory(messagingProperties, connectionFactory),
                claimCheck);
        // @formatter:on
    }
    
    public CachingConnectionFactory createCachingConnectionFactory(MessagingProperties messagingProperties, CachingConnectionFactory connectionFactory) {
        CachingConnectionFactory finalConnectionFactory = connectionFactory;
        
        if (messagingProperties.getRabbitmq().isUseDedicatedInstance()) {
            MessagingProperties.RabbitMQInstanceSettings rabbitMqProperties = messagingProperties.getRabbitmq().getInstanceSettings();
            
            ConnectionFactory dedicatedConnectionFactory = new ConnectionFactory();
            
            if (rabbitMqProperties.getHost() != null) {
                dedicatedConnectionFactory.setHost(rabbitMqProperties.getHost());
            }
            
            if (rabbitMqProperties.getPassword() != null) {
                dedicatedConnectionFactory.setPort(rabbitMqProperties.getPort());
            }
            
            if (rabbitMqProperties.getUsername() != null) {
                dedicatedConnectionFactory.setUsername(rabbitMqProperties.getUsername());
            }
            
            if (rabbitMqProperties.getPassword() != null) {
                dedicatedConnectionFactory.setPassword(rabbitMqProperties.getPassword());
            }
            
            if (rabbitMqProperties.getVirtualHost() != null) {
                dedicatedConnectionFactory.setVirtualHost(rabbitMqProperties.getVirtualHost());
            }
            
            finalConnectionFactory = new CachingConnectionFactory(dedicatedConnectionFactory);
            if (rabbitMqProperties.getPublisherConfirmType() != null) {
                finalConnectionFactory.setPublisherConfirmType(rabbitMqProperties.getPublisherConfirmType());
            }
            
            finalConnectionFactory.setPublisherConfirms(CachingConnectionFactory.ConfirmType.SIMPLE != rabbitMqProperties.getPublisherConfirmType()
                            && rabbitMqProperties.isPublisherConfirms());
            finalConnectionFactory.setPublisherReturns(rabbitMqProperties.isPublisherReturns());
        }
        return finalConnectionFactory;
    }
}
