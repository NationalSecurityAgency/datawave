package nsa.datawave.query.util;

import nsa.datawave.webservice.common.cache.SharedBooleanListener;
import nsa.datawave.webservice.common.cache.SharedBooleanReader;
import nsa.datawave.webservice.common.cache.SharedCacheCoordinator;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.log4j.Logger;

import java.util.Set;
import java.util.concurrent.TimeUnit;

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
    
    private void registerCacheListeners() {
        for (String metadataTableName : metadataTableNames) {
            registerCacheListener(metadataTableName);
        }
    }
    
    private void registerCacheListener(final String metadataTableName) {
        if (log.isDebugEnabled())
            log.debug("created listener for table:" + metadataTableName);
        final SharedCacheCoordinator watcher = new SharedCacheCoordinator(metadataTableName, this.zookeepers, 30, 300);
        try {
            watcher.start();
        } catch (Exception e) {
            throw new RuntimeException("Error starting Watcher for MetadataHelper", e);
        } catch (Error e) {
            throw new RuntimeException("Error starting Watcher for MetadataHelper", e);
        }
        final String booleanName = metadataTableName + ":needsUpdate";
        try {
            watcher.registerBoolean(booleanName, new SharedBooleanListener() {
                @Override
                public void booleanHasChanged(SharedBooleanReader reader, boolean value) throws Exception {
                    if (log.isDebugEnabled())
                        log.debug("booleanHasChanged(" + reader + ", " + value + ")");
                    
                    if (value) {
                        maybeUpdateTypeMetadataInHdfs(watcher, booleanName, metadataTableName);
                    }
                }
                
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState) {
                    if (log.isDebugEnabled())
                        log.debug("stateChanged(" + client + ", " + newState + ")");
                }
            });
            watcher.setBoolean(booleanName, true);
            this.maybeUpdateTypeMetadataInHdfs(watcher, booleanName, metadataTableName);
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private void maybeUpdateTypeMetadataInHdfs(final SharedCacheCoordinator watcher, String booleanName, String metadataTableName) throws Exception {
        
        boolean locked = false;
        InterProcessMutex lock = (InterProcessMutex) watcher.getMutex("lock");
        try {
            locked = lock.acquire(10000, TimeUnit.MILLISECONDS);
            if (!locked)
                log.warn("Unable to acquire lock. May be making duplicate updateTypeMetadata calls.");
            else
                log.trace("Obtained lock on updateTypeMetadata");
        } catch (Exception e) {
            log.warn("Unable to acquire lock. May be making duplicate updateTypeMetadata calls.");
        }
        
        try {
            if (locked) {
                log.debug("checkBoolean(" + booleanName + ", " + true);
                if (watcher.checkBoolean(booleanName, true)) {
                    if (log.isDebugEnabled())
                        log.debug(this + " Needs update is True! Will write the TypeMetadata map to hdfs");
                    // get a connection for my MetadataHelper, and get the TypeMetadata map
                    ZooKeeperInstance instance = new ZooKeeperInstance(ClientConfiguration.loadDefault().withInstance(this.instance)
                                    .withZkHosts(this.zookeepers));
                    Connector connector = instance.getConnector(this.username, new PasswordToken(this.password));
                    metadataHelper.initialize(connector, "DatawaveMetadata", allMetadataAuths);
                    this.typeMetadataWriter.writeTypeMetadataMap(this.metadataHelper.getTypeMetadataMap(), metadataTableName);
                    if (log.isDebugEnabled())
                        log.debug(this + " set the sharedBoolean needsUpdate to False");
                    watcher.setBoolean(booleanName, false);
                } else {
                    if (log.isDebugEnabled())
                        log.debug(this + "  Needs update is False! Someone else already wrote the TypeMetadata map, just release the lock");
                }
            }
            
        } finally {
            if (locked) {
                lock.release();
            }
            if (log.isDebugEnabled())
                log.debug(this + " released the lock");
        }
    }
}
