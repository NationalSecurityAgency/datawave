package datawave.microservice.modification.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import datawave.modification.cache.ModificationCache;

@Component
@ConditionalOnProperty(name = "datawave.modification.cache.monitor.enabled", havingValue = "true", matchIfMissing = true)
public class CacheReloadMonitor {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final ModificationCache modificationCache;
    
    public CacheReloadMonitor(ModificationCache modificationCache) {
        this.modificationCache = modificationCache;
    }
    
    // this runs in a separate thread every 30 seconds (by default)
    @Scheduled(cron = "${datawave.modification.cache.monitor.scheduler-crontab:*/30 * * * * ?}")
    public void monitorTaskScheduler() {
        modificationCache.reloadMutableFieldCache();
    }
}
