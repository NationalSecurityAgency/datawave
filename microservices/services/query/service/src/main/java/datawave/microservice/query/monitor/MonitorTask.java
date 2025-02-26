package datawave.microservice.query.monitor;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.microservice.query.QueryManagementService;
import datawave.microservice.query.config.QueryExpirationProperties;
import datawave.microservice.query.messaging.QueryResultsManager;
import datawave.microservice.query.monitor.cache.MonitorStatus;
import datawave.microservice.query.monitor.cache.MonitorStatusCache;
import datawave.microservice.query.monitor.config.MonitorProperties;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.webservice.query.exception.QueryException;

public class MonitorTask implements Callable<Void> {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final List<QueryStatus> queryStatusList;
    private final MonitorProperties monitorProperties;
    private final QueryExpirationProperties expirationProperties;
    private final MonitorStatusCache monitorStatusCache;
    private final QueryStorageCache queryStorageCache;
    private final QueryResultsManager queryQueueManager;
    private final QueryManagementService queryManagementService;
    private final QueryMetricFactory queryMetricFactory;
    
    public MonitorTask(List<QueryStatus> queryStatusList, MonitorProperties monitorProperties, QueryExpirationProperties expirationProperties,
                    MonitorStatusCache monitorStatusCache, QueryStorageCache queryStorageCache, QueryResultsManager queryQueueManager,
                    QueryManagementService queryManagementService, QueryMetricFactory queryMetricFactory) {
        this.queryStatusList = queryStatusList;
        this.monitorProperties = monitorProperties;
        this.expirationProperties = expirationProperties;
        this.monitorStatusCache = monitorStatusCache;
        this.queryStorageCache = queryStorageCache;
        this.queryQueueManager = queryQueueManager;
        this.queryManagementService = queryManagementService;
        this.queryMetricFactory = queryMetricFactory;
    }
    
    @Override
    public Void call() throws Exception {
        if (tryLock()) {
            boolean success = false;
            MonitorStatus monitorStatus = null;
            try {
                long currentTimeMillis = System.currentTimeMillis();
                monitorStatus = monitorStatusCache.getStatus();
                if (monitorStatus.isExpired(currentTimeMillis, monitorProperties.getMonitorIntervalMillis())) {
                    monitor(currentTimeMillis);
                    success = true;
                }
            } finally {
                if (success) {
                    monitorStatus.setLastChecked(System.currentTimeMillis());
                    monitorStatusCache.setStatus(monitorStatus);
                }
                unlock();
            }
        }
        return null;
    }
    
    // Check for the following conditions
    // 1) Is query progress idle? If so, poke the query
    // 2) Is the user idle? If so, close the query
    // 3) Are there any other conditions that we should check for?
    private void monitor(long currentTimeMillis) {
        for (QueryStatus status : queryStatusList) {
            String queryId = status.getQueryKey().getQueryId();
            
            // if the query is not running
            if (!status.isRunning()) {
                
                // if the query has been inactive too long (i.e. no interaction from the user or software)
                if (status.isInactive(currentTimeMillis, monitorProperties.getInactiveQueryTimeToLiveMillis())) {
                    deleteQuery(queryId);
                }
                // delete the results queue if it exists
                else {
                    queryQueueManager.deleteQuery(queryId);
                }
            }
            // if the query is running
            else {
                // if the query isn't making progress
                if (status.isProgressIdle(currentTimeMillis, expirationProperties.getProgressTimeoutMillis())) {
                    defibrillateQuery(queryId, status.getQueryKey().getQueryPool());
                }
                // if the user hasn't interacted with the query
                else if (status.isUserIdle(currentTimeMillis, expirationProperties.getIdleTimeoutMillis())) {
                    cancelQuery(queryId);
                }
            }
        }
    }
    
    private void cancelQuery(String queryId) {
        // since this is running in a separate thread, we need to set and use the thread-local baseQueryMetric
        ThreadLocal<BaseQueryMetric> baseQueryMetricOverride = queryManagementService.getBaseQueryMetricOverride();
        baseQueryMetricOverride.set(queryMetricFactory.createMetric());
        
        try {
            queryManagementService.cancel(queryId, true);
        } catch (InterruptedException e) {
            log.error("Interrupted while trying to cancel idle query: " + queryId, e);
        } catch (QueryException e) {
            log.error("Encountered error while trying to cancel idle query: " + queryId, e);
        } finally {
            baseQueryMetricOverride.remove();
        }
    }
    
    private void defibrillateQuery(String queryId, String queryPool) {
        // publish a next event to the executor pool
        queryManagementService.publishNextEvent(queryId, queryPool);
    }
    
    private void deleteQuery(String queryId) {
        try {
            // deletes everything for a query
            // the result queue, the query status, the tasks, the task states
            queryStorageCache.deleteQuery(queryId);
        } catch (IOException e) {
            log.error("Encountered error while trying to evict inactive query: " + queryId, e);
        }
    }
    
    private boolean tryLock() throws InterruptedException {
        return monitorStatusCache.tryLock(monitorProperties.getLockWaitTime(), monitorProperties.getLockWaitTimeUnit(), monitorProperties.getLockLeaseTime(),
                        monitorProperties.getLockLeaseTimeUnit());
    }
    
    private void unlock() {
        monitorStatusCache.unlock();
    }
}
