package datawave.query.util;

import java.util.ArrayList;
import java.util.Collections;

import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.webservice.common.cache.SharedCacheCoordinator;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.shared.SharedCountListener;
import org.apache.curator.framework.recipes.shared.SharedCountReader;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.log4j.Logger;

/**
 * Uses the SharedCacheCoordinator to register listeners so that when an event is fired (for example, when a new model is loaded) the spring injected cache of
 * the MetadataHelpers will be evicted.
 *
 * Note that because the SharedCacheCoordinator uses zookeeper, this class will not work in cases where zookeeper is not running (like in unit tests). This
 * class is created by the MetadataHelperCacheListenerContext.xml which is not loaded in unit tests
 */
public class MetadataHelperCacheManagementListener {
    
    private static final Logger log = Logger.getLogger(MetadataHelperCacheManagementListener.class);
    
    private final String zookeepers;
    private final MetadataHelperFactory metadataHelperFactory;
    private final ArrayList<SharedCacheCoordinator> cacheCoordinators;
    private final Connector connector;
    
    public MetadataHelperCacheManagementListener(String zookeepers, MetadataHelperFactory metadataHelperFactory, String[] metadataTableNames) {
        Connector connector;
        this.zookeepers = zookeepers;
        this.metadataHelperFactory = metadataHelperFactory;
        
        try {
            connector = new InMemoryInstance().getConnector("root", new PasswordToken(""));
        } catch (AccumuloException | AccumuloSecurityException e) {
            connector = null;
            log.error("This should never happen -- can't initialize an in-memory connector.", e);
        }
        this.connector = connector;
        
        cacheCoordinators = new ArrayList<>(metadataTableNames.length);
        for (String metadataTableName : metadataTableNames) {
            SharedCacheCoordinator watcher = registerCacheListener(metadataTableName);
            cacheCoordinators.add(watcher);
        }
    }
    
    private SharedCacheCoordinator registerCacheListener(final String metadataTableName) {
        log.debug("created CacheManagement listener for table:" + metadataTableName);
        final SharedCacheCoordinator watcher = new SharedCacheCoordinator(metadataTableName, this.zookeepers, 30, 300, 10);
        try {
            watcher.start();
        } catch (Exception e) {
            throw new RuntimeException("Error starting Watcher for MetadataHelper", e);
        } catch (Error e) {
            throw new RuntimeException("Error starting Watcher for MetadataHelper", e);
        }
        try {
            watcher.registerCounter(metadataTableName, new SharedCountListener() {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState) {
                    if (log.isTraceEnabled()) {
                        log.trace("stateChanged(" + client + ", " + newState + ")");
                    }
                }
                
                @Override
                public void countHasChanged(SharedCountReader sharedCount, int newCount) throws Exception {
                    if (log.isDebugEnabled()) {
                        StringBuilder buf = new StringBuilder();
                        buf.append("countHasChanged from ");
                        try {
                            buf.append(sharedCount != null ? sharedCount.getCount() : null);
                        } catch (Throwable th) {
                            buf.append(th);
                        }
                        buf.append(" to " + newCount + " for " + metadataTableName);
                        log.debug(buf.toString());
                    }
                    if (!watcher.checkCounter(metadataTableName, newCount)) {
                        log.debug("will evictCaches for " + metadataTableName);
                        MetadataHelper metadataHelper = metadataHelperFactory.createMetadataHelper(connector, metadataTableName,
                                        Collections.singleton(new Authorizations()));
                        metadataHelper.evictCaches();
                    } else {
                        log.debug("did not evictCaches for " + metadataTableName);
                    }
                }
            });
        } catch (Exception e) {
            throw new RuntimeException("Unable to create shared counters: " + e.getMessage(), e);
        } catch (Error e) {
            throw new RuntimeException("Unable to create shared counters: " + e.getMessage(), e);
        }
        
        return watcher;
    }
    
    /**
     * Cleans up {@link SharedCacheCoordinator}s used by this class. This method should be named as the "destroy-method" when this class is declared as a Spring
     * bean.
     */
    @SuppressWarnings("unused")
    private void shutdown() {
        for (SharedCacheCoordinator watcher : cacheCoordinators) {
            try {
                watcher.stop();
            } catch (Exception e) {
                log.error("Unable to shutdown cache coordinator: " + e.getMessage(), e);
            }
        }
    }
}
