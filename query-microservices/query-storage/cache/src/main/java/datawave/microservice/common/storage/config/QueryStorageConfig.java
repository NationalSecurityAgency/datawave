package datawave.microservice.common.storage.config;

import com.hazelcast.spring.cache.HazelcastCacheManager;
import datawave.microservice.cached.CacheInspector;
import datawave.microservice.cached.LockableCacheInspector;
import datawave.microservice.cached.LockableHazelcastCacheInspector;
import datawave.microservice.cached.UniversalLockableCacheInspector;
import datawave.microservice.common.storage.QueryCache;
import datawave.microservice.common.storage.QueryQueueManager;
import datawave.microservice.common.storage.queue.KafkaQueryQueueManager;
import datawave.microservice.common.storage.queue.LocalQueryQueueManager;
import datawave.microservice.common.storage.queue.RabbitQueryQueueManager;
import org.apache.log4j.Logger;
import org.springframework.amqp.rabbit.annotation.RabbitListenerConfigurer;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistrar;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;

@Configuration
@EnableCaching
@EnableConfigurationProperties(QueryStorageProperties.class)
public class QueryStorageConfig implements RabbitListenerConfigurer {
    private static final Logger log = Logger.getLogger(QueryStorageConfig.class);
    
    @Autowired
    private ConnectionFactory connectionFactory;
    
    @Autowired
    private QueryStorageProperties properties;
    
    @Bean
    public Jackson2JsonMessageConverter producerJackson2MessageConverter() {
        return new Jackson2JsonMessageConverter();
    }
    
    @Bean
    public MappingJackson2MessageConverter consumerJackson2MessageConverter() {
        return new MappingJackson2MessageConverter();
    }
    
    @Bean
    public RabbitTemplate rabbitTemplate() {
        final RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setMessageConverter(producerJackson2MessageConverter());
        return rabbitTemplate;
    }
    
    @Bean
    public RabbitAdmin rabbitAdmin() {
        return new RabbitAdmin(connectionFactory);
    }
    
    @Bean
    public RabbitListenerEndpointRegistry rabbitListenerEndpointRegistry() {
        return new RabbitListenerEndpointRegistry();
    }
    
    public DefaultMessageHandlerMethodFactory messageHandlerMethodFactory() {
        DefaultMessageHandlerMethodFactory factory = new DefaultMessageHandlerMethodFactory();
        factory.setMessageConverter(consumerJackson2MessageConverter());
        return factory;
    }
    
    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }
    
    @Override
    public void configureRabbitListeners(final RabbitListenerEndpointRegistrar registrar) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setPrefetchCount(1);
        factory.setConsecutiveActiveTrigger(1);
        factory.setConsecutiveIdleTrigger(1);
        factory.setConnectionFactory(connectionFactory);
        registrar.setContainerFactory(factory);
        registrar.setEndpointRegistry(rabbitListenerEndpointRegistry());
        registrar.setMessageHandlerMethodFactory(messageHandlerMethodFactory());
    }
    
    @Bean
    public QueryCache queryStorageCache(CacheInspector cacheInspector, CacheManager cacheManager) {
        log.debug("Using " + cacheManager.getClass() + " for caching");
        LockableCacheInspector lockableCacheInspector = null;
        if (cacheManager instanceof HazelcastCacheManager)
            lockableCacheInspector = new LockableHazelcastCacheInspector(cacheManager);
        else
            lockableCacheInspector = new UniversalLockableCacheInspector(cacheInspector);
        return new QueryCache(lockableCacheInspector);
    }
    
    @Bean
    public QueryQueueManager queueManager() {
        switch (properties.getBackend()) {
            case KAFKA:
                return new KafkaQueryQueueManager();
            case RABBITMQ:
                return new RabbitQueryQueueManager();
            case LOCAL:
                return new LocalQueryQueueManager();
            default:
                throw new IllegalArgumentException("Unknown queue backend " + properties.getBackend());
        }
    }
}
