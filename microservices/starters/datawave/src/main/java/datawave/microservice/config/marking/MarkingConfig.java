package datawave.microservice.config.marking;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.github.benmanes.caffeine.cache.CaffeineSpec;

import datawave.cache.CollectionSafeKeyGenerator;
import datawave.marking.ColumnVisibilitySecurityMarking;
import datawave.marking.MarkingFunctions;
import datawave.marking.SecurityMarking;

/**
 * Provides default configuration for DATAWAVE {@link MarkingFunctions} and relates objects to be injected from the Spring Boot
 * {@link org.springframework.context.ApplicationContext}.
 */
@Configuration
public class MarkingConfig {
    
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(name = "datawave.defaults.MarkingFunctions.enabled", havingValue = "true", matchIfMissing = true)
    public MarkingFunctions markingFunctions() {
        return new MarkingFunctions.Default();
    }
    
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(name = "datawave.defaults.SecurityMarking.enabled", havingValue = "true", matchIfMissing = true)
    public SecurityMarking securityMarking() {
        return new ColumnVisibilitySecurityMarking();
    }
    
    @Bean(name = "markings-cache-manager")
    @ConditionalOnMissingBean(name = "markings-cache-manager")
    public CacheManager markingsCacheManager() {
        CaffeineCacheManager caffeineCacheManager = new CaffeineCacheManager();
        caffeineCacheManager.setCaffeineSpec(CaffeineSpec.parse("maximumSize=1000, expireAfterAccess=24h, expireAfterWrite=24h"));
        return caffeineCacheManager;
    }
    
    @Bean
    @ConditionalOnMissingBean
    public CollectionSafeKeyGenerator collectionSafeKeyGenerator() {
        return new CollectionSafeKeyGenerator();
    }
}
