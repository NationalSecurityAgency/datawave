package datawave.microservice.query.storage.config;

import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.hazelcast.spring.cache.HazelcastCacheManager;

import datawave.microservice.cached.CacheInspector;
import datawave.microservice.cached.LockableCacheInspector;
import datawave.microservice.cached.LockableHazelcastCacheInspector;
import datawave.microservice.cached.UniversalLockableCacheInspector;
import datawave.microservice.query.storage.QueryStatusCache;
import datawave.microservice.query.storage.TaskCache;
import datawave.microservice.query.storage.TaskStatesCache;

@Configuration
@EnableCaching
@ConditionalOnProperty(name = "datawave.query.storage.cache.enabled", havingValue = "true", matchIfMissing = true)
public class QueryStorageConfiguration {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    @Bean
    public QueryStatusCache queryStatusCache(@Qualifier("cacheInspectorFactory") Function<CacheManager,CacheInspector> cacheInspectorFactory,
                    CacheManager cacheManager) {
        log.debug("Using " + cacheManager.getClass() + " for caching");
        LockableCacheInspector lockableCacheInspector;
        if (cacheManager instanceof HazelcastCacheManager) {
            lockableCacheInspector = new LockableHazelcastCacheInspector(cacheManager);
        } else {
            lockableCacheInspector = new UniversalLockableCacheInspector(cacheInspectorFactory.apply(cacheManager));
        }
        return new QueryStatusCache(lockableCacheInspector);
    }
    
    @Bean
    public TaskStatesCache taskStatesCache(@Qualifier("cacheInspectorFactory") Function<CacheManager,CacheInspector> cacheInspectorFactory,
                    CacheManager cacheManager) {
        log.debug("Using " + cacheManager.getClass() + " for caching");
        LockableCacheInspector lockableCacheInspector;
        if (cacheManager instanceof HazelcastCacheManager) {
            lockableCacheInspector = new LockableHazelcastCacheInspector(cacheManager);
        } else {
            lockableCacheInspector = new UniversalLockableCacheInspector(cacheInspectorFactory.apply(cacheManager));
        }
        return new TaskStatesCache(lockableCacheInspector);
    }
    
    @Bean
    public TaskCache taskCache(@Qualifier("cacheInspectorFactory") Function<CacheManager,CacheInspector> cacheInspectorFactory, CacheManager cacheManager) {
        log.debug("Using " + cacheManager.getClass() + " for caching");
        LockableCacheInspector lockableCacheInspector;
        if (cacheManager instanceof HazelcastCacheManager) {
            lockableCacheInspector = new LockableHazelcastCacheInspector(cacheManager);
        } else {
            lockableCacheInspector = new UniversalLockableCacheInspector(cacheInspectorFactory.apply(cacheManager));
        }
        return new TaskCache(lockableCacheInspector);
    }
}
