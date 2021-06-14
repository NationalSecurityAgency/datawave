package datawave.microservice.query.storage.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.io.Serializable;

@ConfigurationProperties(prefix = "query.storage")
public class QueryStorageProperties {
    
    public enum LOCKMGR implements Serializable {
        ZOO, LOCAL
    }
    
    // should storage be synched to disk on every call
    private boolean syncStorage = false;
    
    // the zookeeper connection string if needed
    private String zookeeperConnectionString = "localhost:2181";
    
    // which lock manager should be used
    private LOCKMGR lockManager = LOCKMGR.LOCAL;
    
    public boolean isSyncStorage() {
        return syncStorage;
    }
    
    public void setSyncStorage(boolean syncStorage) {
        this.syncStorage = syncStorage;
    }
    
    public LOCKMGR getLockManager() {
        return lockManager;
    }
    
    public void setLockManager(LOCKMGR lockmanager) {
        this.lockManager = lockmanager;
    }
    
    public String getZookeeperConnectionString() {
        return zookeeperConnectionString;
    }
    
    public void setZookeeperConnectionString(String zoo) {
        this.zookeeperConnectionString = zoo;
    }
}
