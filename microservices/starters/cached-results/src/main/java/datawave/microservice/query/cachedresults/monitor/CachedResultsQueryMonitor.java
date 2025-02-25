package datawave.microservice.query.cachedresults.monitor;

import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import datawave.microservice.query.cachedresults.CachedResultsQueryService;
import datawave.microservice.query.cachedresults.config.CachedResultsQueryProperties;
import datawave.microservice.query.cachedresults.monitor.cache.MonitorStatusCache;
import datawave.microservice.query.cachedresults.monitor.config.MonitorProperties;
import datawave.microservice.query.cachedresults.status.cache.CachedResultsQueryCache;

@Component
@ConditionalOnProperty(name = {"datawave.query.cached-results.enabled", "datawave.query.cached-results.monitor.enabled"}, havingValue = "true",
                matchIfMissing = true)
public class CachedResultsQueryMonitor {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final MonitorProperties monitorProperties;
    private final CachedResultsQueryProperties cachedResultsQueryProperties;
    private final MonitorStatusCache cachedResultsMonitorStatusCache;
    private final CachedResultsQueryService cachedResultsQueryService;
    private final JdbcTemplate cachedResultsJdbcTemplate;
    private final CachedResultsQueryCache cachedResultsQueryCache;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    
    private long taskStartTime;
    private Future<Void> taskFuture;
    
    public CachedResultsQueryMonitor(MonitorProperties monitorProperties, CachedResultsQueryProperties cachedResultsQueryProperties,
                    MonitorStatusCache cachedResultsMonitorStatusCache, CachedResultsQueryService cachedResultsQueryService,
                    JdbcTemplate cachedResultsJdbcTemplate, CachedResultsQueryCache cachedResultsQueryCache) {
        this.monitorProperties = monitorProperties;
        this.cachedResultsQueryProperties = cachedResultsQueryProperties;
        this.cachedResultsMonitorStatusCache = cachedResultsMonitorStatusCache;
        this.cachedResultsQueryService = cachedResultsQueryService;
        this.cachedResultsJdbcTemplate = cachedResultsJdbcTemplate;
        this.cachedResultsQueryCache = cachedResultsQueryCache;
    }
    
    // this runs in a separate thread every 30 minutes (by default)
    @Scheduled(cron = "${datawave.query.cached-results.monitor.scheduler-crontab:0 0/30 * * * ?}")
    public void monitorTaskScheduler() {
        // perform some upkeep
        if (taskFuture != null) {
            if (taskFuture.isDone()) {
                try {
                    taskFuture.get();
                } catch (InterruptedException e) {
                    log.warn("Cached Results Monitor task was interrupted");
                } catch (ExecutionException e) {
                    log.error("Cached Results Monitor task failed", e.getCause());
                }
                taskFuture = null;
            } else if (isTaskLeaseExpired()) {
                // if the lease has expired for the future, cancel it and wait for next scheduled task
                taskFuture.cancel(true);
            }
        }
        
        // schedule a new monitor task if the previous one has finished/expired
        if (taskFuture == null && isMonitorIntervalExpired()) {
            taskStartTime = System.currentTimeMillis();
            try {
                // @formatter:off
                taskFuture = executor.submit(
                        new MonitorTask(
                                monitorProperties,
                                cachedResultsQueryProperties,
                                cachedResultsMonitorStatusCache,
                                cachedResultsQueryService,
                                cachedResultsQueryCache,
                                cachedResultsJdbcTemplate));
                // @formatter:on
            } catch (SQLException e) {
                log.error("Unable to start cached results monitor task", e);
            }
        }
    }
    
    private boolean isTaskLeaseExpired() {
        return (System.currentTimeMillis() - taskStartTime) > monitorProperties.getMonitorIntervalMillis();
    }
    
    private boolean isMonitorIntervalExpired() {
        return (System.currentTimeMillis() - cachedResultsMonitorStatusCache.getStatus().getLastCheckedMillis()) > monitorProperties.getMonitorIntervalMillis();
    }
}
