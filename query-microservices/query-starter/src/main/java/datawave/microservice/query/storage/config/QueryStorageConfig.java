package datawave.microservice.query.storage.config;

import com.hazelcast.spring.cache.HazelcastCacheManager;
import datawave.microservice.cached.CacheInspector;
import datawave.microservice.cached.LockableCacheInspector;
import datawave.microservice.cached.LockableHazelcastCacheInspector;
import datawave.microservice.cached.UniversalLockableCacheInspector;
import datawave.microservice.query.storage.QueryStatusCache;
import datawave.microservice.query.storage.TaskCache;
import datawave.microservice.query.storage.TaskStatesCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.interceptor.CacheErrorHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
@EnableCaching
public class QueryStorageConfig {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    @Bean
    public QueryStatusCache queryStatusCache(CacheInspector cacheInspector, CacheManager cacheManager) {
        log.debug("Using " + cacheManager.getClass() + " for caching");
        LockableCacheInspector lockableCacheInspector;
        if (cacheManager instanceof HazelcastCacheManager)
            lockableCacheInspector = new LockableHazelcastCacheInspector(cacheManager);
        else
            lockableCacheInspector = new UniversalLockableCacheInspector(cacheInspector);
        return new QueryStatusCache(lockableCacheInspector);
    }
    
    @Bean
    public TaskStatesCache taskStatesCache(CacheInspector cacheInspector, CacheManager cacheManager) {
        log.debug("Using " + cacheManager.getClass() + " for caching");
        LockableCacheInspector lockableCacheInspector;
        if (cacheManager instanceof HazelcastCacheManager)
            lockableCacheInspector = new LockableHazelcastCacheInspector(cacheManager);
        else
            lockableCacheInspector = new UniversalLockableCacheInspector(cacheInspector);
        return new TaskStatesCache(lockableCacheInspector);
    }
    
    @Bean
    public TaskCache taskCache(CacheInspector cacheInspector, CacheManager cacheManager) {
        log.debug("Using " + cacheManager.getClass() + " for caching");
        LockableCacheInspector lockableCacheInspector;
        if (cacheManager instanceof HazelcastCacheManager)
            lockableCacheInspector = new LockableHazelcastCacheInspector(cacheManager);
        else
            lockableCacheInspector = new UniversalLockableCacheInspector(cacheInspector);
        return new TaskCache(lockableCacheInspector);
    }
    
    @Bean
    @Primary
    public CacheErrorHandler errorHandler() {
        return new QueryCacheErrorHandler();
    }
    
    private static class QueryCacheErrorHandler implements CacheErrorHandler {
        private final Logger logger = LoggerFactory.getLogger(this.getClass());
        
        @Override
        public void handleCacheGetError(RuntimeException exception, Cache cache, Object key) {
            logger.error("Exception retrieving value for " + key + " from cache " + cache.getName(), exception);
            // preserve the underlying stack trace by wrapping the exception instead of rethrowing it.
            throw new RuntimeException(exception);
        }
        
        @Override
        public void handleCachePutError(RuntimeException exception, Cache cache, Object key, Object value) {
            logger.error("Exception putting value " + key + " => " + value + " in cache " + cache.getName(), exception);
            // preserve the underlying stack trace by wrapping the exception instead of rethrowing it.
            throw new RuntimeException(exception);
        }
        
        @Override
        public void handleCacheEvictError(RuntimeException exception, Cache cache, Object key) {
            logger.error("Exception evicting " + key + " from cache " + cache.getName(), exception);
            // preserve the underlying stack trace by wrapping the exception instead of rethrowing it.
            throw new RuntimeException(exception);
        }
        
        @Override
        public void handleCacheClearError(RuntimeException exception, Cache cache) {
            logger.error("Exception clearing cache " + cache.getName(), exception);
            // preserve the underlying stack trace by wrapping the exception instead of rethrowing it.
            throw new RuntimeException(exception);
        }
    }
    
}
