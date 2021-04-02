package datawave.microservice.common.storage.config;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.spring.cache.HazelcastCacheManager;
import datawave.microservice.cached.CacheInspector;
import datawave.microservice.cached.LockableCacheInspector;
import datawave.microservice.cached.LockableHazelcastCacheInspector;
import datawave.microservice.cached.UniversalLockableCacheInspector;
import datawave.microservice.common.storage.QueryCache;
import datawave.microservice.common.storage.QueryLockManager;
import datawave.microservice.common.storage.QueryQueueManager;
import datawave.microservice.common.storage.lock.LocalQueryLockManager;
import datawave.microservice.common.storage.lock.ZooQueryLockManager;
import datawave.microservice.common.storage.queue.KafkaQueryQueueManager;
import datawave.microservice.common.storage.queue.LocalQueryQueueManager;
import datawave.microservice.common.storage.queue.RabbitQueryQueueManager;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import org.springframework.amqp.rabbit.annotation.RabbitListenerConfigurer;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
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
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableCaching

@EnableConfigurationProperties(QueryStorageProperties.class)
public class QueryStorageConfig implements RabbitListenerConfigurer {
    private static final Logger log = Logger.getLogger(QueryStorageConfig.class);
    
    @Autowired
    private QueryStorageProperties properties;
    
    @Bean
    @Primary
    public CacheManager cacheManager() {
        return new HazelcastCacheManager(Hazelcast.newHazelcastInstance());
    }
    
    @Bean
    public Jackson2JsonMessageConverter producerJackson2MessageConverter() {
        return new Jackson2JsonMessageConverter();
    }
    
    @Bean
    public MappingJackson2MessageConverter consumerJackson2MessageConverter() {
        return new MappingJackson2MessageConverter();
    }
    
    private CachingConnectionFactory connectionFactory;
    
    @Bean
    public ConnectionFactory getConnectionFactory() {
        if (connectionFactory == null) {
            synchronized (this) {
                try {
                    if (properties.getRabbitConnectionString() != null) {
                        connectionFactory = new CachingConnectionFactory(new URI(properties.getRabbitConnectionString()));
                    } else {
                        connectionFactory = new CachingConnectionFactory();
                    }
                } catch (URISyntaxException e) {
                    throw new RuntimeException("Unable to create rabbitMQ connection factory for " + properties.getRabbitConnectionString(), e);
                }
            }
        }
        return connectionFactory;
    }
    
    @Bean
    public RabbitTemplate rabbitTemplate() {
        final RabbitTemplate rabbitTemplate = new RabbitTemplate(getConnectionFactory());
        rabbitTemplate.setMessageConverter(producerJackson2MessageConverter());
        return rabbitTemplate;
    }
    
    @Bean
    public RabbitAdmin rabbitAdmin() {
        return new RabbitAdmin(getConnectionFactory());
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
        factory.setConnectionFactory(getConnectionFactory());
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
    
    private Map<String,Object> kafkaProducerConfig() {
        Map<String,Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getKafkaConnectionString());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        
        return config;
    }
    
    @Bean
    public KafkaTemplate kafkaTemplate() {
        return new KafkaTemplate(new DefaultKafkaProducerFactory<>(kafkaProducerConfig()));
    }
    
    private Map<String,Object> kafkaConsumerConfig() {
        Map<String,Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getKafkaConnectionString());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaQueryQueueManager");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return config;
    }
    
    @Bean
    public DefaultKafkaConsumerFactory kafkaConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(kafkaConsumerConfig(), new StringDeserializer(), new ByteArrayDeserializer());
    }
    
    private Map<String,Object> kafkaAdminClientConfig() {
        Map<String,Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getKafkaConnectionString());
        return config;
    }
    
    @Bean
    public AdminClient kafkaAdminClient() {
        return KafkaAdminClient.create(kafkaAdminClientConfig());
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
    
    @Bean
    public QueryLockManager lockManager() {
        switch (properties.getLockManager()) {
            case ZOO:
                return new ZooQueryLockManager(properties.getZookeeperConnectionString());
            case LOCAL:
                return new LocalQueryLockManager();
            default:
                throw new IllegalArgumentException("Unknown lock manager " + properties.getLockManager());
        }
    }
}
