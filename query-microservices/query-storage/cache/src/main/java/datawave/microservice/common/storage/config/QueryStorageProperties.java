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
    
    // should storage be synched to disk on every call
    private boolean synchStorage;
    
    // should task notifications be sent
    private boolean sendNotifications;
    
    // which backend should be used
    private BACKEND backend = BACKEND.LOCAL;
    
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
}
