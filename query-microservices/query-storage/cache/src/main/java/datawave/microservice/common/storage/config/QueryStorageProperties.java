package datawave.microservice.common.storage.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@EnableConfigurationProperties(QueryStorageProperties.class)
@ConfigurationProperties(prefix = "query.storage")
public class QueryStorageProperties {
    
    // should storage be synched to disk on every call
    private boolean synchStorage;

    // should task notifications be sent
    private boolean sendNotifications;
    
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

}
