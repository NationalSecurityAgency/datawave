package datawave.microservice.query.monitor;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import datawave.microservice.query.QueryManagementService;
import datawave.microservice.query.config.QueryExpirationProperties;
import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.executor.status.cache.ExecutorPoolStatus;
import datawave.microservice.query.executor.status.cache.ExecutorStatusCache;
import datawave.microservice.query.executor.status.cache.util.LockedCacheUpdateUtil;
import datawave.microservice.query.messaging.QueryResultsManager;
import datawave.microservice.query.monitor.cache.MonitorStatusCache;
import datawave.microservice.query.monitor.config.MonitorProperties;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.microservice.querymetric.QueryMetricFactory;

@Component
@ConditionalOnProperty(name = "datawave.query.monitor.enabled", havingValue = "true", matchIfMissing = true)
public class QueryMonitor {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final MonitorProperties monitorProperties;
    private final QueryExpirationProperties expirationProperties;
    private final MonitorStatusCache monitorStatusCache;
    private final QueryStorageCache queryStorageCache;
    private final ExecutorStatusCache executorStatusCache;
    private final LockedCacheUpdateUtil<ExecutorPoolStatus> executorStatusUpdateUtil;
    private final QueryResultsManager queryResultsManager;
    private final QueryManagementService queryManagementService;
    private final QueryMetricFactory queryMetricFactory;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    
    private long taskStartTime;
    private Future<Void> taskFuture;
    
    @Value("${datawave.connection.factory.default-pool:default}")
    private String defaultConnectionPool;
    
    public QueryMonitor(MonitorProperties monitorProperties, QueryProperties queryProperties, MonitorStatusCache monitorStatusCache,
                    QueryStorageCache queryStorageCache, ExecutorStatusCache executorStatusCache, QueryResultsManager queryResultsManager,
                    QueryManagementService queryManagementService, QueryMetricFactory queryMetricFactory) {
        this.monitorProperties = monitorProperties;
        this.expirationProperties = queryProperties.getExpiration();
        this.monitorStatusCache = monitorStatusCache;
        this.queryStorageCache = queryStorageCache;
        this.executorStatusCache = executorStatusCache;
        this.executorStatusUpdateUtil = new LockedCacheUpdateUtil<>(this.executorStatusCache);
        this.queryResultsManager = queryResultsManager;
        this.queryManagementService = queryManagementService;
        this.queryMetricFactory = queryMetricFactory;
    }
    
    // this runs in a separate thread every 30 seconds (by default)
    @Scheduled(cron = "${datawave.query.monitor.scheduler-crontab:*/30 * * * * ?}")
    public void monitorTaskScheduler() {
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
        
        // pull the list of query status ONCE and share it between the monitor task and the query count method
        List<QueryStatus> queryStatusList = queryStorageCache.getQueryStatus();
        
        // schedule a new monitor task if the previous one has finished/expired
        if (taskFuture == null && isMonitorIntervalExpired()) {
            taskStartTime = System.currentTimeMillis();
            // @formatter:off
            taskFuture = executor.submit(
                    new MonitorTask(
                            queryStatusList,
                            monitorProperties,
                            expirationProperties,
                            monitorStatusCache,
                            queryStorageCache,
                            queryResultsManager,
                            queryManagementService,
                            queryMetricFactory));
            // @formatter:on
        }
        
        // independent of the monitor task, count the number of running queries per pool, per connectionPool
        storeQueryCountsInExecutorStatusCache(queryStatusList);
    }
    
    private boolean isTaskLeaseExpired() {
        return (System.currentTimeMillis() - taskStartTime) > monitorProperties.getMonitorIntervalMillis();
    }
    
    private boolean isMonitorIntervalExpired() {
        return (System.currentTimeMillis() - monitorStatusCache.getStatus().getLastCheckedMillis()) > monitorProperties.getMonitorIntervalMillis();
    }
    
    private void storeQueryCountsInExecutorStatusCache(List<QueryStatus> queryStatusList) {
        Map<String,Map<String,Integer>> queryCountByPool = new LinkedHashMap<>();
        for (QueryStatus queryStatus : queryStatusList) {
            if (queryStatus.isRunning()) {
                // @formatter:off
                Map<String, Integer> countsByConnectionPool = queryCountByPool.computeIfAbsent(
                        queryStatus.getQueryKey().getQueryPool(),
                        k -> new LinkedHashMap<>());
                // @formatter:on
                
                String connectionPoolName = queryStatus.getConfig().getConnPoolName();
                if (connectionPoolName == null) {
                    connectionPoolName = defaultConnectionPool;
                }
                
                countsByConnectionPool.merge(connectionPoolName, 1, Integer::sum);
            }
        }
        
        // write this to the query pool status cache
        for (String queryPool : queryCountByPool.keySet()) {
            try {
                executorStatusUpdateUtil.lockedUpdate(queryPool, (executorStatus) -> {
                    executorStatus.setQueryCountByConnectionPool(queryCountByPool.get(queryPool));
                }, monitorProperties.getExecutorStatusLockWaitTimeMillis(), monitorProperties.getExecutorStatusLockLeaseTimeMillis());
            } catch (Exception e) {
                log.error("Failed to update executor status with query counts for query pool: {}", queryPool);
            }
        }
    }
}
