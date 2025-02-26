package datawave.microservice.query.storage.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.Cache;
import org.springframework.cache.annotation.CachingConfigurerSupport;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.interceptor.CacheErrorHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configures the Spring caching support to supply a special error handler. The error handler clears the entire cache if a get request fails. This is done since
 * the main reason a get request would fail is if the cache used a backing store and the entries in the backing store can no longer be loaded.
 */
@Configuration
@EnableCaching
@ConditionalOnProperty(name = "datawave.query.storage.cache.enabled", havingValue = "true", matchIfMissing = true)
public class CachingConfigurer extends CachingConfigurerSupport {
    
    @Bean
    @Override
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
