package datawave.microservice.query.monitor;

import datawave.microservice.query.monitor.cache.MonitorStatus;
import datawave.microservice.query.monitor.cache.MonitorStatusCache;
import datawave.microservice.query.monitor.config.MonitorProperties;

import java.util.concurrent.Callable;

public class MonitorTask implements Callable<Void> {
    private final MonitorProperties monitorProperties;
    private final MonitorStatusCache monitorStatusCache;
    
    public MonitorTask(MonitorProperties monitorProperties, MonitorStatusCache monitorStatusCache) {
        this.monitorProperties = monitorProperties;
        this.monitorStatusCache = monitorStatusCache;
    }
    
    @Override
    public Void call() throws Exception {
        if (tryLock()) {
            boolean success = false;
            MonitorStatus monitorStatus = null;
            try {
                monitorStatus = monitorStatusCache.getStatus();
                if (isMonitorIntervalExpired()) {
                    // TODO: perform idle checks, etc.
                    
                    success = true;
                }
            } finally {
                if (success && monitorStatus != null) {
                    monitorStatus.setLastChecked(System.currentTimeMillis());
                }
                unlock();
            }
        }
        return null;
    }
    
    private boolean tryLock() throws InterruptedException {
        return monitorStatusCache.tryLock(monitorProperties.getLockWaitTime(), monitorProperties.getLockWaitTimeUnit(), monitorProperties.getLockLeaseTime(),
                        monitorProperties.getLockLeaseTimeUnit());
    }
    
    private void unlock() {
        monitorStatusCache.unlock();
    }
    
    private boolean isMonitorIntervalExpired() {
        return (System.currentTimeMillis() - monitorStatusCache.getStatus().getLastCheckedMillis()) > monitorProperties.getMonitorIntervalMillis();
    }
}
