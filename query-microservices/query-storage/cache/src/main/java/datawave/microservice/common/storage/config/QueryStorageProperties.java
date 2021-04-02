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
    private boolean synchStorage = false;
    
    // should task notifications be sent
    private boolean sendNotifications = false;
    
    // the rabbitMQ connection string if needed
    private String rabbitConnectionString = "amqp://localhost:5672";
    
    // the kafka connection string if needed
    private String kafkaConnectionString = "localhost:9092";
    
    // the zookeeper connection string if needed
    private String zookeeperConnectionString = "localhost:2181";
    
    // which backend should be used
    private BACKEND backend = BACKEND.LOCAL;
    
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
    
    public String getRabbitConnectionString() {
        return rabbitConnectionString;
    }
    
    public void setRabbitConnectionString(String rabbitConnectionString) {
        this.rabbitConnectionString = rabbitConnectionString;
    }
    
    public String getKafkaConnectionString() {
        return kafkaConnectionString;
    }
    
    public void setKafkaConnectionString(String kafkaConnectionString) {
        this.kafkaConnectionString = kafkaConnectionString;
    }
    
}
