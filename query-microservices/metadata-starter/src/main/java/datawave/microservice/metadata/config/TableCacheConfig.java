package datawave.microservice.metadata.config;

import datawave.services.common.cache.AccumuloTableCache;
import datawave.services.common.cache.AccumuloTableCacheImpl;
import datawave.services.common.cache.AccumuloTableCacheProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
public class TableCacheConfig {
    
    @Bean
    @ConditionalOnProperty(name = "datawave.table.cache.enabled", havingValue = "true", matchIfMissing = true)
    @ConfigurationProperties("datawave.table.cache")
    public AccumuloTableCacheProperties tableCacheConfiguration() {
        return new AccumuloTableCacheProperties();
    }
    
    @Bean
    @ConditionalOnProperty(name = "datawave.table.cache.enabled", havingValue = "true", matchIfMissing = true)
    public AccumuloTableCache tableCache(AccumuloTableCacheProperties accumuloTableCacheProperties) {
        return new AccumuloTableCacheImpl(accumuloTableCacheProperties);
    }
}
