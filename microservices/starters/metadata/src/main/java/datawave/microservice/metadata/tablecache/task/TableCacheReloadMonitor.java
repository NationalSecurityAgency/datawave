package datawave.microservice.metadata.tablecache.task;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import datawave.core.common.cache.AccumuloTableCache;
import datawave.core.common.cache.AccumuloTableCacheProperties;

@Component
@ConditionalOnProperty(name = "datawave.table.cache.enabled", havingValue = "true", matchIfMissing = true)
public class TableCacheReloadMonitor {
    private final Logger log = Logger.getLogger(TableCacheReloadMonitor.class);
    private final AccumuloTableCache cache;
    private final AccumuloTableCacheProperties properties;
    
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private long taskStartTime;
    private Future<Void> taskFuture;
    
    public TableCacheReloadMonitor(AccumuloTableCache cache, AccumuloTableCacheProperties properties) {
        this.cache = cache;
        this.properties = properties;
    }
    
    @Scheduled(cron = "${datawave.table.cache.reload-crontab:* * * * * ?}")
    public void monitorReloadTasks() {
        // perform some upkeep
        if (taskFuture != null) {
            if (taskFuture.isDone()) {
                try {
                    taskFuture.get();
                } catch (InterruptedException e) {
                    log.warn("Table cache reload task was interrupted");
                } catch (CancellationException e) {
                    log.warn("Table cache reload task was cancelled");
                } catch (Exception e) {
                    log.error("Table cache reload task failed", e);
                }
                taskFuture = null;
            } else if (isTaskLeaseExpired()) {
                // if the lease has expired for the future, cancel it and wait for next scheduled task
                log.warn("Table cache reload task being cancelled");
                taskFuture.cancel(true);
            }
        }
        
        // schedule a new monitor task if the previous one has finished
        if (taskFuture == null) {
            taskStartTime = System.currentTimeMillis();
            taskFuture = executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    cache.submitReloadTasks();
                    return null;
                }
            });
        }
    }
    
    private boolean isTaskLeaseExpired() {
        return (System.currentTimeMillis() - taskStartTime) > properties.getTableCacheReloadTaskLeaseMillis();
    }
    
}
