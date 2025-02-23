package datawave.autoconfigure;

import java.util.List;

import org.springframework.boot.autoconfigure.cache.CacheManagerCustomizers;
import org.springframework.boot.autoconfigure.cache.CacheProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
@ConditionalOnMissingBean(name = "cacheManager")
@Conditional(CacheCondition.class)
public class SimpleCacheConfiguration {
    private final CacheProperties cacheProperties;
    
    private final CacheManagerCustomizers customizerInvoker;
    
    SimpleCacheConfiguration(CacheProperties cacheProperties, CacheManagerCustomizers customizerInvoker) {
        this.cacheProperties = cacheProperties;
        this.customizerInvoker = customizerInvoker;
    }
    
    @Bean
    @Primary
    public ConcurrentMapCacheManager cacheManager() {
        ConcurrentMapCacheManager cacheManager = new ConcurrentMapCacheManager();
        List<String> cacheNames = this.cacheProperties.getCacheNames();
        if (!cacheNames.isEmpty()) {
            cacheManager.setCacheNames(cacheNames);
        }
        return this.customizerInvoker.customize(cacheManager);
    }
}
