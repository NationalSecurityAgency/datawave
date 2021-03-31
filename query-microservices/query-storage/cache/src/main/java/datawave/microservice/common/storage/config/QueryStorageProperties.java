package datawave.microservice.common.storage.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import java.io.Serializable;

@EnableConfigurationProperties(QueryStorageProperties.class)
@ConfigurationProperties(prefix = "query.storage")
public class QueryStorageProperties {
    
    public enum BACKEND implements Serializable {
        KAFKA, RABBITMQ, LOCAL
    }
    
    public enum LOCKMGR implements Serializable {
        ZOO, LOCAL
    }
    
    // should storage be synched to disk on every call
    private boolean synchStorage;
    
    // should task notifications be sent
    private boolean sendNotifications;
    
    // the zookeeper connection string if needed
    private String zookeeperConnectionString;
    
    // which backend should be used
    private BACKEND backend;
    
    // which lock manager should be used
    private LOCKMGR lockManager;
    
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
    
    public BACKEND getBackend() {
        return backend;
    }
    
    public void setBackend(BACKEND backend) {
        this.backend = backend;
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
