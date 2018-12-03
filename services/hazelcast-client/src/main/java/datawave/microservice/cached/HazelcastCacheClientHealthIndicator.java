package datawave.microservice.cached;

import com.hazelcast.core.IMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class HazelcastCacheClientHealthIndicator extends AbstractHealthIndicator {
    private static Logger log = LoggerFactory.getLogger(HazelcastCacheClientHealthIndicator.class);
    private final Random r = new Random(System.currentTimeMillis());
    
    @Autowired
    private CacheManager cacheManager;
    
    protected HazelcastCacheClientHealthIndicator() {
        super();
    }
    
    protected HazelcastCacheClientHealthIndicator(String healthCheckFailedMessage) {
        super(healthCheckFailedMessage);
    }
    
    protected HazelcastCacheClientHealthIndicator(Function<Exception,String> healthCheckFailedMessage) {
        super(healthCheckFailedMessage);
    }
    
    @Override
    protected void doHealthCheck(Health.Builder builder) {
        try {
            Collection<String> cacheNames = cacheManager.getCacheNames();
            cacheNames.forEach(this::checkCache);
            
            builder.up().withDetail("cacheNames", cacheNames);
            // @formatter:off
            Map<String,Integer> sizes = cacheNames.stream()
                    .map(cacheManager::getCache)
                    .filter(Objects::nonNull)
                    .map(Cache::getNativeCache)
                    .map(IMap.class::cast)
                    .collect(Collectors.toMap(IMap::getName, IMap::size));
            builder.withDetail("cacheEntryCounts", sizes);
            // @formatter:on
        } catch (Exception e) {
            log.warn("Unable to retrieve hazelcast cache health", e);
            builder.down().withException(e);
        }
    }
    
    /**
     * Checks the cache by attempting to add, retrieve, then evict a value from the cache.
     * 
     * @param cacheName
     *            the name of the cache to check
     */
    private void checkCache(String cacheName) {
        String key = "__healthCheckKey" + r.nextInt();
        String value = "__healthCheckValue";
        Cache cache = cacheManager.getCache(cacheName);
        if (cache != null) {
            cache.put(key, value);
            String actualValue = cache.get(key, String.class);
            if (!value.equals(actualValue)) {
                throw new RuntimeException(
                                "Retrieved incorrect value from " + cacheName + " for key=" + key + ". Expected " + value + " but got " + actualValue);
            }
            cache.evict(key);
        }
    }
}
