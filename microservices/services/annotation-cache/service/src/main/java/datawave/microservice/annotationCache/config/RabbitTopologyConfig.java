package datawave.microservice.annotationCache.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Setup rabbit topics and queues at startup time for minimal operation
 */
@Configuration
public class RabbitTopologyConfig {

    // Explicitly define the durable exchange
    @Bean
    public TopicExchange annotationExchange() {
        return ExchangeBuilder.topicExchange("annotation").durable(true).build();
    }

    // Explicitly define the durable
    @Bean
    public Queue cacheQueue() {
        return QueueBuilder.durable("annotation.cache").build();
    }

    // Bind them together with a wildcard routing key (#)
    @Bean
    public Binding cacheBinding(TopicExchange annotationExchange, Queue cacheQueue) {
        // Receives everything published to this exchange
        return BindingBuilder.bind(cacheQueue).to(annotationExchange).with("#");
    }
}
