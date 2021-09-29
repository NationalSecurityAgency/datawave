package datawave.microservice.query.storage.config;

import com.hazelcast.spring.cache.HazelcastCacheManager;
import datawave.microservice.cached.CacheInspector;
import datawave.microservice.cached.LockableCacheInspector;
import datawave.microservice.cached.LockableHazelcastCacheInspector;
import datawave.microservice.cached.UniversalLockableCacheInspector;
import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.storage.QueryStatusCache;
import datawave.microservice.query.storage.TaskCache;
import datawave.microservice.query.storage.TaskStatesCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableCaching
@EnableConfigurationProperties(QueryStorageProperties.class)
public class QueryStorageConfig {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    @Bean
    @ConditionalOnMissingBean(QueryProperties.class)
    @ConfigurationProperties("datawave.query")
    public QueryProperties queryProperties() {
        return new QueryProperties();
    }
    
    @Bean
    public QueryStatusCache queryStatusCache(CacheInspector cacheInspector, CacheManager cacheManager) {
        log.debug("Using " + cacheManager.getClass() + " for caching");
        LockableCacheInspector lockableCacheInspector = null;
        if (cacheManager instanceof HazelcastCacheManager)
            lockableCacheInspector = new LockableHazelcastCacheInspector(cacheManager);
        else
            lockableCacheInspector = new UniversalLockableCacheInspector(cacheInspector);
        return new QueryStatusCache(lockableCacheInspector);
    }
    
    @Bean
    public TaskStatesCache taskStatesCache(CacheInspector cacheInspector, CacheManager cacheManager) {
        log.debug("Using " + cacheManager.getClass() + " for caching");
        LockableCacheInspector lockableCacheInspector = null;
        if (cacheManager instanceof HazelcastCacheManager)
            lockableCacheInspector = new LockableHazelcastCacheInspector(cacheManager);
        else
            lockableCacheInspector = new UniversalLockableCacheInspector(cacheInspector);
        return new TaskStatesCache(lockableCacheInspector);
    }
    
    @Bean
    public TaskCache taskCache(CacheInspector cacheInspector, CacheManager cacheManager) {
        log.debug("Using " + cacheManager.getClass() + " for caching");
        LockableCacheInspector lockableCacheInspector = null;
        if (cacheManager instanceof HazelcastCacheManager)
            lockableCacheInspector = new LockableHazelcastCacheInspector(cacheManager);
        else
            lockableCacheInspector = new UniversalLockableCacheInspector(cacheInspector);
        return new TaskCache(lockableCacheInspector);
    }
}
