package datawave.autoconfigure;

import java.util.Collection;

import org.springframework.boot.autoconfigure.cache.CacheManagerCustomizers;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
@ConditionalOnBean(Cache.class)
@ConditionalOnMissingBean(name = "cacheManager")
@Conditional(CacheCondition.class)
public class GenericCacheConfiguration {
    private final CacheManagerCustomizers customizers;
    
    GenericCacheConfiguration(CacheManagerCustomizers customizers) {
        this.customizers = customizers;
    }
    
    @Bean
    @Primary
    public SimpleCacheManager cacheManager(Collection<Cache> caches) {
        SimpleCacheManager cacheManager = new SimpleCacheManager();
        cacheManager.setCaches(caches);
        return this.customizers.customize(cacheManager);
    }
}
