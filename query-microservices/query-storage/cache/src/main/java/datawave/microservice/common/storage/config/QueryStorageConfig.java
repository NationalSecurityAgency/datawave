package datawave.microservice.common.storage.config;

import com.hazelcast.spring.cache.HazelcastCacheManager;
import datawave.microservice.cached.CacheInspector;
import datawave.microservice.cached.LockableCacheInspector;
import datawave.microservice.cached.LockableHazelcastCacheInspector;
import datawave.microservice.cached.UniversalLockableCacheInspector;
import datawave.microservice.common.storage.QueryCache;
import org.apache.log4j.Logger;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@EnableCaching
@Profile("QueryStorageConfig")
@EnableConfigurationProperties(QueryStorageProperties.class)
public class QueryStorageConfig {
    private static final Logger log = Logger.getLogger(QueryStorageConfig.class);
    
    @Bean
    public AmqpAdmin amqpAdmin(ConnectionFactory connectionFactory) {
        return new RabbitAdmin(connectionFactory);
    }
    
    @Bean
    public AmqpTemplate amqpTemplate(ConnectionFactory connectionFactory) {
        return new RabbitTemplate(connectionFactory);
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
}
