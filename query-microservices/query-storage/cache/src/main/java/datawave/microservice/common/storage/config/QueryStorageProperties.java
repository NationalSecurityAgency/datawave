package datawave.microservice.common.storage.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@EnableConfigurationProperties(QueryStorageProperties.class)
@ConfigurationProperties(prefix = "query.storage")
public class QueryStorageProperties {
    
    // should storage be synched to disk on every call
    private boolean synchStorage;
    
    public boolean isSynchStorage() {
        return synchStorage;
    }
    
    public void setSynchStorage(boolean synchStorage) {
        this.synchStorage = synchStorage;
    }
    
}
