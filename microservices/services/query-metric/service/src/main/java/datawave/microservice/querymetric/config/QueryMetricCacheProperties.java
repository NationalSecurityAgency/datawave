package datawave.microservice.querymetric.config;

import javax.validation.constraints.PositiveOrZero;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "datawave.query.metric.cache")
public class QueryMetricCacheProperties {
    
    private Cache lastWrittenQueryMetrics = new Cache();
    
    public void setLastWrittenQueryMetrics(Cache lastWrittenQueryMetrics) {
        this.lastWrittenQueryMetrics = lastWrittenQueryMetrics;
    }
    
    public Cache getLastWrittenQueryMetrics() {
        return lastWrittenQueryMetrics;
    }
    
    public class Cache {
        @PositiveOrZero
        private int maximumSize = 5000;
        @PositiveOrZero
        private long ttlSeconds = 600;
        
        public void setMaximumSize(int maximumSize) {
            this.maximumSize = maximumSize;
        }
        
        public int getMaximumSize() {
            return maximumSize;
        }
        
        public void setTtlSeconds(long ttlSeconds) {
            this.ttlSeconds = ttlSeconds;
        }
        
        public long getTtlSeconds() {
            return ttlSeconds;
        }
    }
}
