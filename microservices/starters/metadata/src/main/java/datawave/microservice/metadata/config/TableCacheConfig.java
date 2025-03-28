package datawave.microservice.metadata.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import datawave.core.common.cache.AccumuloTableCache;
import datawave.core.common.cache.AccumuloTableCacheImpl;
import datawave.core.common.cache.AccumuloTableCacheProperties;
import datawave.microservice.metadata.tablecache.task.TableCacheReloadMonitor;

@Configuration
@EnableScheduling
@ConditionalOnProperty(name = "datawave.table.cache.enabled", havingValue = "true", matchIfMissing = true)
public class TableCacheConfig {
    
    @Bean
    @ConfigurationProperties("datawave.table.cache")
    @ConditionalOnMissingBean(AccumuloTableCacheProperties.class)
    public AccumuloTableCacheProperties tableCacheConfiguration() {
        return new AccumuloTableCacheProperties();
    }
    
    @Bean
    @ConditionalOnMissingBean(AccumuloTableCache.class)
    public AccumuloTableCache tableCache(AccumuloTableCacheProperties accumuloTableCacheProperties) {
        return new AccumuloTableCacheImpl(accumuloTableCacheProperties);
    }
    
    @Bean
    @ConditionalOnMissingBean(TableCacheReloadMonitor.class)
    public TableCacheReloadMonitor tableCacheMonitor(AccumuloTableCache cache, AccumuloTableCacheProperties properties) {
        return new TableCacheReloadMonitor(cache, properties);
    }
}
