package datawave.microservice.query.monitor;

import datawave.microservice.query.monitor.cache.MonitorStatusCache;
import datawave.microservice.query.monitor.config.MonitorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Component
@ConditionalOnProperty(name = "datawave.query.monitor.enabled", havingValue = "true", matchIfMissing = true)
public class QueryMonitor {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final MonitorProperties monitorProperties;
    private final MonitorStatusCache monitorStatusCache;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    
    private long taskStartTime;
    private Future<Void> taskFuture;
    
    public QueryMonitor(MonitorProperties monitorProperties, MonitorStatusCache monitorStatusCache) {
        this.monitorProperties = monitorProperties;
        this.monitorStatusCache = monitorStatusCache;
    }
    
    // this runs in a separate thread every 30 seconds (by default)
    @Scheduled(cron = "${datawave.query.monitor.scheduler-crontab:*/30 * * * * ?}")
    public void damageReport() {
        // perform some upkeep
        if (taskFuture != null) {
            if (taskFuture.isDone()) {
                try {
                    taskFuture.get();
                } catch (InterruptedException e) {
                    log.warn("Query Monitor task was interrupted");
                } catch (ExecutionException e) {
                    log.error("Query Monitor task failed", e.getCause());
                }
                taskFuture = null;
            } else if (isTaskLeaseExpired()) {
                // if the lease has expired for the future, cancel it and wait for next scheduled task
                taskFuture.cancel(true);
            }
        }
        
        // schedule a new monitor task if the previous one has finished/expired
        if (taskFuture != null && isMonitorIntervalExpired()) {
            taskStartTime = System.currentTimeMillis();
            taskFuture = executor.submit(new MonitorTask(monitorProperties, monitorStatusCache));
        }
    }
    
    private boolean isTaskLeaseExpired() {
        return (System.currentTimeMillis() - taskStartTime) > monitorProperties.getMonitorIntervalMillis();
    }
    
    private boolean isMonitorIntervalExpired() {
        return (System.currentTimeMillis() - monitorStatusCache.getStatus().getLastCheckedMillis()) > monitorProperties.getMonitorIntervalMillis();
    }
}
