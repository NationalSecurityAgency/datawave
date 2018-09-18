package datawave.query.util;

import datawave.typemetadata.TypeMetadata;
import datawave.typemetadata.TypeMetadataWriter;
import datawave.webservice.common.cache.SharedCacheCoordinator;
import datawave.webservice.common.cache.SharedTriState;
import datawave.webservice.common.cache.SharedTriStateListener;
import datawave.webservice.common.cache.SharedTriStateReader;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Uses the SharedCacheCoordinator to register listeners so that when an event is fired (for example, when a new model is loaded) the TypeMetadata map will be
 * written to hdfs
 *
 * Note that because the SharedCacheCoordinator uses zookeeper, this class will not work in cases where zookeeper is not running (like in unit tests). This
 * class is created by the MetadataHelperCacheListenerContext.xml which is not loaded in unit tests
 */
public class MetadataHelperUpdateHdfsListener {
    
    private static final Logger log = Logger.getLogger(MetadataHelperUpdateHdfsListener.class);
    
    private String zookeepers;
    private MetadataHelper metadataHelper;
    private String[] metadataTableNames;
    private Set<Authorizations> allMetadataAuths;
    private TypeMetadataWriter typeMetadataWriter = TypeMetadataWriter.Factory.createTypeMetadataWriter();
    
    private String instance;
    private String username;
    private String password;
    private long lockWaitTime = 1000L; // configurable in MetadataHelperCacheListenerContext.xml
    
    public void setPassword(String password) {
        this.password = password;
    }
    
    public void setUsername(String username) {
        this.username = username;
    }
    
    public void setInstance(String instance) {
        this.instance = instance;
    }
    
    public void setZookeepers(String zookeepers) {
        this.zookeepers = zookeepers;
    }
    
    public void setMetadataHelper(MetadataHelper metadataHelper) {
        this.metadataHelper = metadataHelper;
    }
    
    public void setAllMetadataAuths(Set<Authorizations> allMetadataAuths) {
        this.allMetadataAuths = allMetadataAuths;
    }
    
    public void setMetadataTableNames(String[] metadataTableNames) {
        
        this.metadataTableNames = metadataTableNames;
        registerCacheListeners();
    }
    
    public long getLockWaitTime() {
        return lockWaitTime;
    }
    
    public void setLockWaitTime(long lockWaitTime) {
        this.lockWaitTime = lockWaitTime;
    }
    
    private void registerCacheListeners() {
        for (String metadataTableName : metadataTableNames) {
            registerCacheListener(metadataTableName);
        }
    }
    
    private void registerCacheListener(final String metadataTableName) {
        if (log.isDebugEnabled())
            log.debug("table:" + metadataTableName + " created UpdateHdfs listener for table:" + metadataTableName);
        final SharedCacheCoordinator watcher = new SharedCacheCoordinator(metadataTableName, this.zookeepers, 30, 300, 10);
        try {
            watcher.start();
        } catch (Exception e) {
            throw new RuntimeException("table:" + metadataTableName + " Error starting Watcher for MetadataHelper", e);
        } catch (Error e) {
            throw new RuntimeException("table:" + metadataTableName + " Error starting Watcher for MetadataHelper", e);
        }
        final String triStateName = metadataTableName + ":needsUpdate";
        try {
            watcher.registerTriState(triStateName, new SharedTriStateListener() {
                @Override
                public void stateHasChanged(SharedTriStateReader reader, SharedTriState.STATE value) throws Exception {
                    if (log.isTraceEnabled()) {
                        log.trace("table:" + metadataTableName + " stateHasChanged(" + reader + ", " + value + ") for " + triStateName);
                    }
                    if (value == SharedTriState.STATE.NEEDS_UPDATE) {
                        maybeUpdateTypeMetadataInHdfs(watcher, triStateName, metadataTableName);
                    }
                }
                
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState) {
                    if (log.isTraceEnabled())
                        log.trace("table:" + metadataTableName + " stateChanged(" + client + ", " + newState + ")");
                }
            });
            // The first time we register a cachelistener, tell it to update the type metadata. This happens when the webserver is (re)started
            if (!watcher.checkTriState(triStateName, SharedTriState.STATE.NEEDS_UPDATE)) {
                watcher.setTriState(triStateName, SharedTriState.STATE.NEEDS_UPDATE);
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private void maybeUpdateTypeMetadataInHdfs(final SharedCacheCoordinator watcher, String triStateName, String metadataTableName) throws Exception {
        
        boolean locked = false;
        InterProcessMutex lock = (InterProcessMutex) watcher.getMutex("lock");
        try {
            locked = lock.acquire(this.lockWaitTime, TimeUnit.MILLISECONDS);
            if (!locked)
                log.debug("table:" + metadataTableName + " Unable to acquire lock to update " + metadataTableName
                                + ". Another webserver is updating the typeMetadata.");
            else
                log.debug("table:" + metadataTableName + " Obtained lock on updateTypeMetadata for " + metadataTableName);
        } catch (Exception e) {
            log.warn("table:" + metadataTableName + " Got Exception trying to acquire lock to update " + metadataTableName + ".", e);
        }
        
        try {
            if (locked) {
                try {
                    log.debug("table:" + metadataTableName + " checkTriState(" + triStateName + ", " + SharedTriState.STATE.NEEDS_UPDATE);
                    if (watcher.checkTriState(triStateName, SharedTriState.STATE.NEEDS_UPDATE)) {
                        if (log.isDebugEnabled()) {
                            log.debug("table:" + metadataTableName + " " + this + " STATE is NEEDS_UPDATE. Will write the TypeMetadata map to hdfs");
                        }
                        watcher.setTriState(triStateName, SharedTriState.STATE.UPDATING);
                        if (log.isDebugEnabled()) {
                            log.debug("table:" + metadataTableName + " " + this + " setTriState to UPDATING");
                        }
                        // get a connection for my MetadataHelper, and get the TypeMetadata map
                        ZooKeeperInstance instance = new ZooKeeperInstance(ClientConfiguration.loadDefault().withInstance(this.instance)
                                        .withZkHosts(this.zookeepers));
                        Connector connector = instance.getConnector(this.username, new PasswordToken(this.password));
                        metadataHelper.initialize(connector, metadataTableName, allMetadataAuths);
                        this.typeMetadataWriter.writeTypeMetadataMap(this.metadataHelper.getTypeMetadataMap(), metadataTableName);
                        if (log.isDebugEnabled()) {
                            log.debug("table:" + metadataTableName + " " + this + " set the sharedTriState needsUpdate to UPDATED for " + metadataTableName);
                        }
                        watcher.setTriState(triStateName, SharedTriState.STATE.UPDATED);
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("table:"
                                            + metadataTableName
                                            + " "
                                            + this
                                            + "  STATE is not NEEDS_UPDATE! Someone else may be writing or has already written the TypeMetadata map, just release the lock");
                        }
                    }
                } catch (Exception ex) {
                    log.warn("table:" + metadataTableName + " Unable to write TypeMetadataMap for " + metadataTableName, ex);
                    watcher.setTriState(triStateName, SharedTriState.STATE.NEEDS_UPDATE);
                    if (log.isDebugEnabled()) {
                        log.debug("After exception, set the SharedTriState STATE to NEEDS_UPDATE");
                    }
                    
                }
            }
        } finally {
            if (locked) {
                lock.release();
                if (log.isTraceEnabled())
                    log.trace("table:" + metadataTableName + " " + this + " released the lock for " + metadataTableName);
                
            }
        }
    }
}
