package datawave.microservice.query.executor.task;

import java.util.Arrays;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.cloud.bus.BusProperties;
import org.springframework.cloud.bus.event.PathDestinationFactory;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.executor.QueryExecutor;
import datawave.microservice.query.executor.config.ExecutorProperties;
import datawave.microservice.query.executor.status.cache.ExecutorPoolStatus;
import datawave.microservice.query.executor.status.cache.ExecutorStatusCache;
import datawave.microservice.query.executor.status.cache.util.LockedCacheUpdateUtil;
import datawave.microservice.query.storage.QueryStorageCache;

@Component
@ConditionalOnProperty(name = "datawave.query.executor.monitor.enabled", havingValue = "true", matchIfMissing = true)
public class FindWorkMonitor {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final BusProperties busProperties;
    private final ExecutorProperties executorProperties;
    private final ExecutorStatusCache executorStatusCache;
    private final LockedCacheUpdateUtil<ExecutorPoolStatus> executorStatusUpdateUtil;
    
    private final QueryStorageCache queryStorageCache;
    private final QueryExecutor queryExecutor;
    private final ExecutorStatusLogger executorStatusLogger = new ExecutorStatusLogger();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    
    private final String originService;
    private final String destinationService;
    
    private long taskStartTime;
    private Future<Void> taskFuture;
    
    public FindWorkMonitor(BusProperties busProperties, ExecutorProperties executorProperties, QueryProperties queryProperties,
                    ExecutorStatusCache executorStatusCache, QueryStorageCache cache, QueryExecutor executor) {
        this.busProperties = busProperties;
        this.executorProperties = executorProperties;
        this.executorStatusCache = executorStatusCache;
        this.executorStatusUpdateUtil = new LockedCacheUpdateUtil<>(executorStatusCache);
        this.queryStorageCache = cache;
        this.queryExecutor = executor;
        
        PathDestinationFactory pathDestinationFactory = new PathDestinationFactory();
        this.originService = pathDestinationFactory.getDestination(queryProperties.getQueryServiceName()).getDestinationAsString();
        this.destinationService = pathDestinationFactory
                        .getDestination(String.join("-", Arrays.asList(queryProperties.getExecutorServiceName(), executorProperties.getPool())))
                        .getDestinationAsString();
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
                } catch (CancellationException e) {
                    log.warn("Query Monitor task was cancelled");
                } catch (Exception e) {
                    log.error("Query Monitor task failed", e);
                }
                taskFuture = null;
            } else if (isTaskLeaseExpired()) {
                // if the lease has expired for the future, cancel it and wait for next scheduled task
                log.warn("Query Monitor task being cancelled");
                taskFuture.cancel(true);
            }
        }
        
        // schedule a new monitor task if the previous one has finished
        if (taskFuture == null) {
            taskStartTime = System.currentTimeMillis();
            taskFuture = executor.submit(new FindWorkTask(queryStorageCache, queryExecutor, originService, destinationService, executorStatusLogger));
        }
        
        // update the executor heartbeat on every iteration
        updateExecutorHeartbeat();
    }
    
    private boolean isTaskLeaseExpired() {
        return (System.currentTimeMillis() - taskStartTime) > executorProperties.getMonitorTaskLeaseMillis();
    }
    
    @EventListener
    private void initializeHeartbeatPool(ApplicationReadyEvent event) {
        executorStatusCache.lock(executorProperties.getPool());
        try {
            ExecutorPoolStatus poolStatus = executorStatusCache.get(executorProperties.getPool());
            if (poolStatus == null) {
                executorStatusCache.update(executorProperties.getPool(), new ExecutorPoolStatus(executorProperties.getPool()));
                log.info("Executor heartbeat pool initialized ({})", executorProperties.getPool());
            }
        } finally {
            executorStatusCache.unlock(executorProperties.getPool());
        }
    }
    
    private void updateExecutorHeartbeat() {
        try {
            executorStatusUpdateUtil.lockedUpdate(executorProperties.getPool(), (executorStatus) -> {
                executorStatus.getExecutorHeartbeat().put(busProperties.getId(), System.currentTimeMillis());
            }, executorProperties.getHealthCheckWaitTimeMillis(), executorProperties.getHealthCheckLeaseTimeMillis());
        } catch (Exception e) {
            log.error("Failed to update executor heartbeat for pool {}", executorProperties.getPool());
        }
    }
}
