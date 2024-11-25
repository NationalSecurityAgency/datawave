package datawave.microservice.querymetric.config;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.Cache;
import org.springframework.cache.caffeine.CaffeineCache;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.github.benmanes.caffeine.cache.Caffeine;

import datawave.microservice.querymetric.persistence.MetricCacheListener;

@Configuration
@EnableConfigurationProperties({QueryMetricCacheProperties.class})
public class QueryMetricCacheConfiguration {
    
    private Logger log = LoggerFactory.getLogger(QueryMetricCacheConfiguration.class);
    public static final String LAST_WRITTEN_METRICS = "lastWrittenQueryMetrics";
    
    @Bean
    @Qualifier("lastWrittenQueryMetrics")
    public Cache lastWrittenQueryMetrics(QueryMetricCacheProperties cacheProperties) {
        // @formatter:off
        QueryMetricCacheProperties.Cache lastWrittenQueryMetrics = cacheProperties.getLastWrittenQueryMetrics();
        return new CaffeineCache("lastWrittenQueryMetrics",
            Caffeine.newBuilder()
                    .initialCapacity(lastWrittenQueryMetrics.getMaximumSize())
                    .maximumSize(lastWrittenQueryMetrics.getMaximumSize())
                    .expireAfterWrite(lastWrittenQueryMetrics.getTtlSeconds(), TimeUnit.SECONDS)
                    .removalListener(new MetricCacheListener(LAST_WRITTEN_METRICS))
                    .build(), false);
        // @formatter:on
    }
}
