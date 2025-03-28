package datawave.microservice.querymetric;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import datawave.microservice.querymetric.config.CorrelatorProperties;

@Component
public class Correlator {
    
    private CorrelatorProperties correlatorProperties;
    private boolean isShuttingDown = false;
    private Map<String,List<QueryMetricUpdate>> updates = new HashMap<>();
    private LinkedHashMap<String,Long> created = new LinkedHashMap<>();
    
    @Autowired
    public Correlator(CorrelatorProperties correlatorProperties) {
        this.correlatorProperties = correlatorProperties;
    }
    
    public void addMetricUpdate(QueryMetricUpdate update) {
        String queryId = update.getMetric().getQueryId();
        synchronized (this.updates) {
            List<QueryMetricUpdate> updatesForQuery = this.updates.get(queryId);
            if (updatesForQuery == null) {
                updatesForQuery = new ArrayList<>();
                this.created.put(queryId, System.currentTimeMillis());
                this.updates.put(queryId, updatesForQuery);
            }
            updatesForQuery.add(update);
        }
    }
    
    public List<QueryMetricUpdate> getMetricUpdates(Set<String> inProcess) {
        long now = System.currentTimeMillis();
        long maxQueueSize = this.correlatorProperties.getMaxCorrelationQueueSize();
        long maxCorrelationTimeMs = this.correlatorProperties.getMaxCorrelationTimeMs();
        String oldestQueryId;
        List<QueryMetricUpdate> returnedUpdates = null;
        synchronized (this.updates) {
            long numEntries = this.created.size();
            // Find the oldest entry that is not inProcessing on this instance
            // If shuttingDown, then just return the oldest entry
            Map.Entry<String,Long> oldestAvailableEntry = this.created.entrySet().stream().filter(e -> this.isShuttingDown || !inProcess.contains(e.getKey()))
                            .findFirst().orElse(null);
            
            // If we have reached the max queue size, then don't filter by !inProcess
            if (oldestAvailableEntry == null && numEntries >= maxQueueSize) {
                oldestAvailableEntry = this.created.entrySet().stream().findFirst().orElse(null);
            }
            
            if (oldestAvailableEntry != null) {
                long maxAge = now - oldestAvailableEntry.getValue();
                oldestQueryId = oldestAvailableEntry.getKey();
                if (numEntries >= maxQueueSize || maxAge > maxCorrelationTimeMs || this.isShuttingDown) {
                    returnedUpdates = this.updates.remove(oldestQueryId);
                    this.created.remove(oldestQueryId);
                }
            }
        }
        return returnedUpdates;
    }
    
    public boolean isEnabled() {
        return this.correlatorProperties.isEnabled();
    }
    
    public void shutdown(boolean isShuttingDown) {
        this.isShuttingDown = isShuttingDown;
    }
    
    public boolean isShuttingDown() {
        return isShuttingDown;
    }
}
