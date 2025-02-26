package datawave.microservice.query.monitor.config;

import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.hazelcast.spring.cache.HazelcastCacheManager;

import datawave.microservice.cached.CacheInspector;
import datawave.microservice.cached.LockableCacheInspector;
import datawave.microservice.cached.LockableHazelcastCacheInspector;
import datawave.microservice.cached.UniversalLockableCacheInspector;
import datawave.microservice.query.monitor.cache.MonitorStatusCache;

@EnableCaching
@EnableScheduling
@Configuration("QueryMonitorConfig")
@ConditionalOnProperty(name = "datawave.query.monitor.enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(MonitorProperties.class)
public class MonitorConfig {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    @Bean
    public MonitorStatusCache monitorStatusCache(@Qualifier("cacheInspectorFactory") Function<CacheManager,CacheInspector> cacheInspectorFactory,
                    CacheManager cacheManager) {
        log.debug("Using " + cacheManager.getClass() + " for caching");
        LockableCacheInspector lockableCacheInspector;
        if (cacheManager instanceof HazelcastCacheManager)
            lockableCacheInspector = new LockableHazelcastCacheInspector(cacheManager);
        else
            lockableCacheInspector = new UniversalLockableCacheInspector(cacheInspectorFactory.apply(cacheManager));
        return new MonitorStatusCache(lockableCacheInspector);
    }
}
