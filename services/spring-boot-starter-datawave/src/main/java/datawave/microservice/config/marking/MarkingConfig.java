package datawave.microservice.config.marking;

import com.github.benmanes.caffeine.cache.CaffeineSpec;
import datawave.cache.CollectionSafeKeyGenerator;
import datawave.marking.ColumnVisibilitySecurityMarking;
import datawave.marking.MarkingFunctions;
import datawave.marking.SecurityMarking;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Provides default configuration for DATAWAVE {@link MarkingFunctions} and relates objects to be injected from the Spring Boot
 * {@link org.springframework.context.ApplicationContext}.
 */
@Configuration
public class MarkingConfig {
    
    @Bean
    @ConditionalOnMissingBean
    public MarkingFunctions markingFunctions() {
        return new MarkingFunctions.NoOp();
    }
    
    @Bean
    @ConditionalOnMissingBean
    public SecurityMarking securityMarking() {
        return new ColumnVisibilitySecurityMarking();
    }
    
    @Bean(name = "markings-cache-manager")
    @ConditionalOnMissingBean
    public CacheManager markingsCacheManager(@Autowired @Qualifier("markings-cache-manager-specification") CaffeineSpec caffeineSpec) {
        CaffeineCacheManager caffeineCacheManager = new CaffeineCacheManager();
        caffeineCacheManager.setCaffeineSpec(caffeineSpec);
        return caffeineCacheManager;
    }
    
    @Bean(name = "markings-cache-manager-specification")
    @ConditionalOnMissingBean
    public CaffeineSpec cacheManagerSpec() {
        return CaffeineSpec.parse("maximumSize=1000, expireAfterAccess=24h, expireAfterWrite=24h");
    }
    
    @Bean
    @ConditionalOnMissingBean
    public CollectionSafeKeyGenerator collectionSafeKeyGenerator() {
        return new CollectionSafeKeyGenerator();
    }
}
