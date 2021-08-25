package datawave.webservice.common.cache;

import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.shared.SharedCountListener;
import org.apache.curator.framework.recipes.shared.SharedCountReader;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Object that caches data from Accumulo tables.
 */
public class AccumuloTableCacheImpl implements AccumuloTableCache {
    
    private final Logger log = Logger.getLogger(this.getClass());
    
    private final ExecutorService executorService;
    private final AccumuloTableCacheConfiguration accumuloTableCacheConfiguration;
    
    private InMemoryInstance instance;
    private Map<String,TableCache> details;
    private List<SharedCacheCoordinator> cacheCoordinators;
    private boolean connectionFactoryProvided = false;
    
    public AccumuloTableCacheImpl(ExecutorService executorService, AccumuloTableCacheConfiguration accumuloTableCacheConfiguration) {
        log.debug("Called AccumuloTableCacheImpl with accumuloTableCacheConfiguration = " + accumuloTableCacheConfiguration);
        this.executorService = executorService;
        this.accumuloTableCacheConfiguration = accumuloTableCacheConfiguration;
        setup();
    }
    
    private void setup() {
        log.debug("accumuloTableCacheConfiguration was setup as: " + accumuloTableCacheConfiguration);
        
        instance = new InMemoryInstance();
        details = new HashMap<>();
        cacheCoordinators = new ArrayList<>();
        
        String zookeepers = accumuloTableCacheConfiguration.getZookeepers();
        
        for (Entry<String,TableCache> entry : accumuloTableCacheConfiguration.getCaches().entrySet()) {
            final String tableName = entry.getKey();
            TableCache detail = entry.getValue();
            detail.setInstance(instance);
            
            final SharedCacheCoordinator cacheCoordinator = new SharedCacheCoordinator(tableName, zookeepers,
                            accumuloTableCacheConfiguration.getEvictionReaperIntervalInSeconds(), accumuloTableCacheConfiguration.getNumLocks(),
                            accumuloTableCacheConfiguration.getMaxRetries());
            cacheCoordinators.add(cacheCoordinator);
            try {
                cacheCoordinator.start();
            } catch (Exception e) {
                throw new RuntimeException("Error starting AccumuloTableCache", e);
            }
            
            try {
                cacheCoordinator.registerTriState(tableName + ":needsUpdate", new SharedTriStateListener() {
                    @Override
                    public void stateHasChanged(SharedTriStateReader reader, SharedTriState.STATE value) throws Exception {
                        if (log.isTraceEnabled()) {
                            log.trace("table:" + tableName + " stateHasChanged(" + reader + ", " + value + "). This listener does nothing");
                        }
                    }
                    
                    @Override
                    public void stateChanged(CuratorFramework client, ConnectionState newState) {
                        if (log.isTraceEnabled()) {
                            log.trace("table:" + tableName + " stateChanged(" + client + ", " + newState + "). This listener does nothing");
                        }
                    }
                });
            } catch (Exception e) {
                log.debug("Failure registering a triState for " + tableName, e);
            }
            
            try {
                cacheCoordinator.registerCounter(tableName, new SharedCountListener() {
                    @Override
                    public void stateChanged(CuratorFramework client, ConnectionState newState) {
                        // TODO Auto-generated method stub
                    }
                    
                    @Override
                    public void countHasChanged(SharedCountReader sharedCount, int newCount) throws Exception {
                        if (!cacheCoordinator.checkCounter(tableName, newCount)) {
                            handleReload(tableName);
                        }
                    }
                });
            } catch (Exception e) {
                throw new RuntimeException("table:" + tableName + " Unable to create shared counters: " + e.getMessage(), e);
            }
            detail.setWatcher(cacheCoordinator);
            details.put(entry.getKey(), entry.getValue());
            
        }
    }
    
    public void setConnectionFactory(AccumuloConnectionFactory connectionFactory) {
        for (Entry<String,TableCache> entry : accumuloTableCacheConfiguration.getCaches().entrySet()) {
            TableCache detail = entry.getValue();
            detail.setConnectionFactory(connectionFactory);
        }
        connectionFactoryProvided = true;
    }
    
    @Override
    public InMemoryInstance getInstance() {
        return this.instance;
    }
    
    @Override
    public void submitReloadTasks() {
        if (!connectionFactoryProvided) {
            log.trace("NOT submitting reload tasks since our connection factory hasn't been provided yet.");
            return;
        }
        
        // Check results of running tasks. If complete, set the references to null
        for (Entry<String,TableCache> entry : details.entrySet()) {
            Future<Boolean> ref = entry.getValue().getReference();
            if (null != ref && (ref.isCancelled() || ref.isDone())) {
                log.info("Reloading complete for table: " + entry.getKey());
                entry.getValue().setReference(null);
            }
            
        }
        // Start new tasks
        long now = System.currentTimeMillis();
        for (Entry<String,TableCache> entry : details.entrySet()) {
            if (null != entry.getValue().getReference()) {
                continue;
            }
            long last = entry.getValue().getLastRefresh().getTime();
            if ((now - last) > entry.getValue().getReloadInterval()) {
                log.info("Reloading " + entry.getKey());
                try {
                    Future<Boolean> result = executorService.submit(entry.getValue());
                    entry.getValue().setReference(result);
                } catch (Exception e) {
                    log.error("Error reloading table: " + entry.getKey(), e);
                }
            }
        }
    }
    
    @Override
    public void close() {
        for (Entry<String,TableCache> entry : details.entrySet()) {
            Future<Boolean> ref = entry.getValue().getReference();
            if (null != ref)
                ref.cancel(true);
        }
        for (SharedCacheCoordinator cacheCoordinator : cacheCoordinators)
            cacheCoordinator.stop();
        cacheCoordinators.clear();
    }
    
    /**
     * Reload a table cache
     *
     * @param tableName
     *            the name of the table for which the cached version is to be reloaded
     */
    @Override
    public void reloadTableCache(String tableName) {
        if (null == details.get(tableName)) {
            return;
        }
        // send an eviction notice to the cluster
        try {
            details.get(tableName).getWatcher().incrementCounter(tableName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        handleReload(tableName);
        handleReloadTypeMetadata(tableName);
    }
    
    private void handleReloadTypeMetadata(String tableName) {
        String triStateName = tableName + ":needsUpdate";
        try {
            log.debug(triStateName + " handleReloadTypeMetadata(" + tableName + ")");
            SharedCacheCoordinator watcher = details.get(tableName).getWatcher();
            if (!watcher.checkTriState(triStateName, SharedTriState.STATE.NEEDS_UPDATE)) {
                watcher.setTriState(triStateName, SharedTriState.STATE.NEEDS_UPDATE);
            }
        } catch (Throwable e) {
            log.debug("table:" + tableName + " could not update the triState '" + triStateName + " on watcher for table " + tableName, e);
        }
    }
    
    private void handleReload(String tableName) {
        details.get(tableName).setLastRefresh(new Date(0));
    }
    
    /**
     * Get the table caches
     */
    @Override
    public List<TableCacheDescription> getTableCaches() {
        List<TableCacheDescription> tableCaches = new ArrayList<>();
        for (Entry<String,TableCache> entry : details.entrySet()) {
            TableCacheDescription t = new TableCacheDescription();
            t.setTableName(entry.getValue().getTableName());
            t.setConnectionPoolName(entry.getValue().getConnectionPoolName());
            t.setAuthorizations(entry.getValue().getAuths());
            t.setReloadInterval(entry.getValue().getReloadInterval());
            t.setMaxRows(entry.getValue().getMaxRows());
            t.setLastRefresh(entry.getValue().getLastRefresh());
            t.setCurrentlyRefreshing((entry.getValue().getReference() != null));
            tableCaches.add(t);
        }
        return tableCaches;
    }
}
