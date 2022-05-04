package datawave.microservice.query.executor.task;

import datawave.microservice.query.executor.config.ExecutorProperties;
import datawave.services.common.cache.AccumuloTableCache;
import org.apache.log4j.Logger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Component
@ConditionalOnProperty(name = "datawave.query.executor.table.cache.reload.enabled", havingValue = "true", matchIfMissing = true)
public class TableCacheReloadMonitor {
    private final Logger log = Logger.getLogger(TableCacheReloadMonitor.class);
    private final AccumuloTableCache cache;
    private final ExecutorProperties executorProperties;
    
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private long taskStartTime;
    private Future<Void> taskFuture;
    
    public TableCacheReloadMonitor(AccumuloTableCache cache, ExecutorProperties executorProperties) {
        this.cache = cache;
        this.executorProperties = executorProperties;
    }
    
    @Scheduled(cron = "${datawave.query.executor.tablecache.reload-crontab:* * */5 * * ?}")
    public void submitReloadTasks() {
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
        return (System.currentTimeMillis() - taskStartTime) > executorProperties.getTableCacheReloadTaskLeaseMillis();
    }
    
}
