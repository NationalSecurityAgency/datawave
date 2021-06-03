package datawave.microservice.query.monitor;

import datawave.microservice.common.storage.QueryStatus;
import datawave.microservice.common.storage.QueryStorageCache;
import datawave.microservice.query.QueryManagementService;
import datawave.microservice.query.config.QueryExpirationProperties;
import datawave.microservice.query.monitor.cache.MonitorStatus;
import datawave.microservice.query.monitor.cache.MonitorStatusCache;
import datawave.microservice.query.monitor.config.MonitorProperties;
import datawave.webservice.query.exception.QueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Callable;

public class MonitorTask implements Callable<Void> {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final MonitorProperties monitorProperties;
    private final QueryExpirationProperties expirationProperties;
    private final MonitorStatusCache monitorStatusCache;
    private final QueryStorageCache queryStorageCache;
    private final QueryManagementService queryManagementService;
    
    public MonitorTask(MonitorProperties monitorProperties, QueryExpirationProperties expirationProperties, MonitorStatusCache monitorStatusCache,
                    QueryStorageCache queryStorageCache, QueryManagementService queryManagementService) {
        this.monitorProperties = monitorProperties;
        this.expirationProperties = expirationProperties;
        this.monitorStatusCache = monitorStatusCache;
        this.queryStorageCache = queryStorageCache;
        this.queryManagementService = queryManagementService;
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
    
    // TODO: Check for the following conditions
    // 1) Is query progress idle? If so, poke the query
    // 2) Is the user idle? If so, close the query
    // 3) Are there any other conditions that we should check for?
    private void monitor(long currentTimeMillis) {
        for (QueryStatus status : queryStorageCache.getQueryStatus()) {
            String queryId = status.getQueryKey().getQueryId().toString();
            if (status.getQueryState() == QueryStatus.QUERY_STATE.DEFINED || status.getQueryState() == QueryStatus.QUERY_STATE.CREATED) {
                if (status.isUserIdle(currentTimeMillis, expirationProperties.getIdleTimeoutMillis())) {
                    // if the user hasn't interacted with the query in a while, cancel it
                    cancelQuery(queryId);
                } else if (status.isProgressIdle(currentTimeMillis, expirationProperties.getIdleTimeoutMillis())) {
                    // if progress hasn't been made for the query in a while, apply the shock paddles
                    defibrilateQuery(queryId, status.getQueryKey().getQueryPool().getName());
                }
            } else {
                if (status.isInactive(currentTimeMillis, monitorProperties.getInactiveQueryTimeToLiveMillis())) {
                    // if the query has been inactive for too long, evict it
                    deleteQuery(UUID.fromString(queryId));
                }
            }
        }
    }
    
    private void cancelQuery(String queryId) {
        try {
            queryManagementService.cancel(queryId, true);
        } catch (InterruptedException e) {
            log.error("Interrupted while trying to cancel idle query: " + queryId, e);
        } catch (QueryException e) {
            log.error("Encountered error while trying to cancel idle query: " + queryId, e);
        }
    }
    
    private void defibrilateQuery(String queryId, String queryPool) {
        // publish a next event to the executor pool
        queryManagementService.publishNextEvent(queryId, queryPool);
    }
    
    private void deleteQuery(UUID queryUUID) {
        try {
            queryStorageCache.deleteQuery(queryUUID);
        } catch (IOException e) {
            log.error("Encountered error while trying to evict inactive query: " + queryUUID.toString(), e);
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
