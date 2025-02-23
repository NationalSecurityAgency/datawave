package datawave.autoconfigure;

import java.util.List;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.cache.CacheManagerCustomizers;
import org.springframework.boot.autoconfigure.cache.CacheProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.CaffeineSpec;

@Configuration
@ConditionalOnClass({Caffeine.class, CaffeineCacheManager.class})
@ConditionalOnMissingBean(name = "cacheManager")
@Conditional(CacheCondition.class)
public class CaffeineCacheConfiguration {
    private final CacheProperties cacheProperties;
    
    private final CacheManagerCustomizers customizers;
    
    private final Caffeine<Object,Object> caffeine;
    
    private final CaffeineSpec caffeineSpec;
    
    private final CacheLoader<Object,Object> cacheLoader;
    
    CaffeineCacheConfiguration(CacheProperties cacheProperties, CacheManagerCustomizers customizers, ObjectProvider<Caffeine<Object,Object>> caffeine,
                    ObjectProvider<CaffeineSpec> caffeineSpec, ObjectProvider<CacheLoader<Object,Object>> cacheLoader) {
        this.cacheProperties = cacheProperties;
        this.customizers = customizers;
        this.caffeine = caffeine.getIfAvailable();
        this.caffeineSpec = caffeineSpec.getIfAvailable();
        this.cacheLoader = cacheLoader.getIfAvailable();
    }
    
    @Bean
    @Primary
    public CaffeineCacheManager cacheManager() {
        CaffeineCacheManager cacheManager = createCacheManager();
        List<String> cacheNames = this.cacheProperties.getCacheNames();
        if (!CollectionUtils.isEmpty(cacheNames)) {
            cacheManager.setCacheNames(cacheNames);
        }
        return this.customizers.customize(cacheManager);
    }
    
    private CaffeineCacheManager createCacheManager() {
        CaffeineCacheManager cacheManager = new CaffeineCacheManager();
        setCacheBuilder(cacheManager);
        if (this.cacheLoader != null) {
            cacheManager.setCacheLoader(this.cacheLoader);
        }
        return cacheManager;
    }
    
    private void setCacheBuilder(CaffeineCacheManager cacheManager) {
        String specification = this.cacheProperties.getCaffeine().getSpec();
        if (StringUtils.hasText(specification)) {
            cacheManager.setCacheSpecification(specification);
        } else if (this.caffeineSpec != null) {
            cacheManager.setCaffeineSpec(this.caffeineSpec);
        } else if (this.caffeine != null) {
            cacheManager.setCaffeine(this.caffeine);
        }
    }
}
