package datawave.microservice.common.storage.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.io.Serializable;

@ConfigurationProperties(prefix = "query.storage")
public class QueryStorageProperties {
    
    public enum LOCKMGR implements Serializable {
        ZOO, LOCAL
    }
    
    // should storage be synched to disk on every call
    private boolean synchStorage = false;
    
    // should task notifications be sent
    private boolean sendNotifications = false;
    
    // the zookeeper connection string if needed
    private String zookeeperConnectionString = "localhost:2181";
    
    // which lock manager should be used
    private LOCKMGR lockManager = LOCKMGR.LOCAL;
    
    public boolean isSynchStorage() {
        return synchStorage;
    }
    
    public void setSynchStorage(boolean synchStorage) {
        this.synchStorage = synchStorage;
    }
    
    public boolean isSendNotifications() {
        return sendNotifications;
    }
    
    public void setSendNotifications(boolean sendNotifications) {
        this.sendNotifications = sendNotifications;
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
