package datawave.autoconfigure;

import org.springframework.boot.autoconfigure.cache.CacheManagerCustomizers;
import org.springframework.boot.autoconfigure.cache.CacheProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ResourceCondition;
import org.springframework.cache.ehcache.EhCacheCacheManager;
import org.springframework.cache.ehcache.EhCacheManagerUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;

@Configuration
@ConditionalOnClass({Cache.class, EhCacheCacheManager.class})
@ConditionalOnMissingBean(name = "cacheManager")
@Conditional({CacheCondition.class, EhCacheCacheConfiguration.ConfigAvailableCondition.class})
class EhCacheCacheConfiguration {
    
    private final CacheProperties cacheProperties;
    
    private final CacheManagerCustomizers customizers;
    
    EhCacheCacheConfiguration(CacheProperties cacheProperties, CacheManagerCustomizers customizers) {
        this.cacheProperties = cacheProperties;
        this.customizers = customizers;
    }
    
    @Bean
    public EhCacheCacheManager cacheManager(CacheManager ehCacheCacheManager) {
        return this.customizers.customize(new EhCacheCacheManager(ehCacheCacheManager));
    }
    
    @Bean
    @ConditionalOnMissingBean
    public CacheManager ehCacheCacheManager() {
        Resource location = this.cacheProperties.resolveConfigLocation(this.cacheProperties.getEhcache().getConfig());
        if (location != null) {
            return EhCacheManagerUtils.buildCacheManager(location);
        }
        return EhCacheManagerUtils.buildCacheManager();
    }
    
    /**
     * Determine if the EhCache configuration is available. This either kick in if a default configuration has been found or if property referring to the file
     * to use has been set.
     */
    static class ConfigAvailableCondition extends ResourceCondition {
        
        ConfigAvailableCondition() {
            super("EhCache", "spring.cache.ehcache.config", "classpath:/ehcache.xml");
        }
        
    }
    
}
