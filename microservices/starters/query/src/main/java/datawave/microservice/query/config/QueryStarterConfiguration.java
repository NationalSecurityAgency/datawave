package datawave.microservice.query.config;

import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.github.benmanes.caffeine.cache.CaffeineSpec;

import datawave.microservice.query.mapreduce.config.MapReduceQueryProperties;
import datawave.microservice.query.stream.StreamingProperties;

@Configuration
public class QueryStarterConfiguration {

    @Bean
    public QueryProperties queryProperties() {
        return new QueryProperties();
    }

    @Bean
    public MapReduceQueryProperties mapReduceQueryProperties() {
        return new MapReduceQueryProperties();
    }

    @Bean
    public StreamingProperties streamingProperties() {
        return new StreamingProperties();
    }

    @Bean
    public CaffeineCacheManager dateIndexHelperCacheManager() {
        CaffeineCacheManager caffeineCacheManager = new CaffeineCacheManager();
        caffeineCacheManager.setCaffeineSpec(CaffeineSpec.parse("maximumSize=1000, expireAfterAccess=24h, expireAfterWrite=24h"));
        return caffeineCacheManager;
    }
}
