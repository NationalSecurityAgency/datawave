package datawave.microservice.cached;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Provides an instance of a {@link CacheInspector}.
 */
@Configuration
@ConditionalOnClass(CacheManager.class)
@ConditionalOnMissingBean(CacheInspector.class)
public class CacheInspectorConfiguration {
    
    @Bean
    public CacheInspector cacheInspector(CacheManager cacheManager) {
        return new HazelcastCacheInspector(cacheManager);
    }
}
