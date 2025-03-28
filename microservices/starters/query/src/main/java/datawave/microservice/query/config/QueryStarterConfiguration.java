package datawave.microservice.query.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.github.benmanes.caffeine.cache.CaffeineSpec;

import datawave.microservice.query.edge.config.EdgeDictionaryProviderProperties;
import datawave.microservice.query.mapreduce.config.MapReduceQueryProperties;
import datawave.microservice.query.stream.StreamingProperties;

@Configuration
@EnableConfigurationProperties({QueryProperties.class, MapReduceQueryProperties.class, StreamingProperties.class, EdgeDictionaryProviderProperties.class})
public class QueryStarterConfiguration {
    
    @Bean
    public CaffeineCacheManager dateIndexHelperCacheManager() {
        CaffeineCacheManager caffeineCacheManager = new CaffeineCacheManager();
        caffeineCacheManager.setCaffeineSpec(CaffeineSpec.parse("maximumSize=1000, expireAfterAccess=24h, expireAfterWrite=24h"));
        return caffeineCacheManager;
    }
}
