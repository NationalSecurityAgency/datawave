package datawave.microservice.query.executor.task;

import datawave.microservice.query.executor.QueryExecutor;
import datawave.microservice.query.executor.config.ExecutorProperties;
import datawave.microservice.query.storage.QueryStorageCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Component
@ConditionalOnProperty(name = "datawave.query.executor.monitor.enabled", havingValue = "true", matchIfMissing = true)
public class FindWorkMonitor {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final ExecutorProperties executorProperties;
    private final QueryStorageCache queryStorageCache;
    private final QueryExecutor queryExecutor;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final FindWorkTask.CloseCancelCache closeCancelCache;
    
    private long taskStartTime;
    private Future<Void> taskFuture;
    
    public FindWorkMonitor(ExecutorProperties executorProperties, QueryStorageCache cache, QueryExecutor executor,
                    FindWorkTask.CloseCancelCache closeCancelCache) {
        this.executorProperties = executorProperties;
        this.queryStorageCache = cache;
        this.queryExecutor = executor;
        this.closeCancelCache = closeCancelCache;
    }
    
    // this runs in a separate thread every 30 seconds (by default)
    @Scheduled(cron = "${datawave.query.executor.monitor.scheduler-crontab:*/30 * * * * ?}")
    public void monitorTaskScheduler() {
        // perform some upkeep
        if (taskFuture != null) {
            if (taskFuture.isDone()) {
                try {
                    taskFuture.get();
                } catch (InterruptedException e) {
                    log.warn("Query Monitor task was interrupted");
                } catch (Exception e) {
                    log.error("Query Monitor task failed", e.getCause());
                }
                taskFuture = null;
            } else if (isTaskLeaseExpired()) {
                // if the lease has expired for the future, cancel it and wait for next scheduled task
                taskFuture.cancel(true);
            }
        }
        
        // schedule a new monitor task if the previous one has finished
        if (taskFuture == null) {
            taskStartTime = System.currentTimeMillis();
            taskFuture = executor.submit(new FindWorkTask(queryStorageCache, queryExecutor, closeCancelCache));
        }
    }
    
    private boolean isTaskLeaseExpired() {
        return (System.currentTimeMillis() - taskStartTime) > executorProperties.getMonitorTaskLeaseMillis();
    }
}
