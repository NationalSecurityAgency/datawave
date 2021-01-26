package datawave.webservice.common.storage.config;

import com.hazelcast.spring.cache.HazelcastCacheManager;
import datawave.microservice.cached.LockableCacheInspector;
import datawave.microservice.cached.UniversalLockableCacheInspector;
import datawave.microservice.cached.LockableHazelcastCacheInspector;
import datawave.microservice.cached.CacheInspector;
import datawave.webservice.common.storage.QueryStorageCache;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.MessageChannel;

@Configuration
@EnableCaching
@EnableConfigurationProperties(QueryStorageProperties.class)
public class QueryStorageConfig {
    
    public interface TaskNotificationSourceBinding {
        String NAME = "queryTaskSource";
        
        @Output(NAME)
        MessageChannel queryTaskSource();
    }
    
    @Bean
    public QueryStorageCache queryStorageCache(CacheInspector cacheInspector, CacheManager cacheManager) {
        LockableCacheInspector lockableCacheInspector = null;
        if (cacheManager instanceof HazelcastCacheManager)
            lockableCacheInspector = new LockableHazelcastCacheInspector(cacheManager);
        else
            lockableCacheInspector = new UniversalLockableCacheInspector(cacheInspector);
        return new QueryStorageCache(lockableCacheInspector);
    }
}
