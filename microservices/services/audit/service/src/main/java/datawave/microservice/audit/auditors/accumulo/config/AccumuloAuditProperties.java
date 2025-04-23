package datawave.microservice.audit.auditors.accumulo.config;

import java.util.concurrent.TimeUnit;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@EnableConfigurationProperties(AccumuloAuditProperties.class)
@ConfigurationProperties(prefix = "audit.auditors.accumulo")
public class AccumuloAuditProperties {
    
    private String tableName = "QueryAuditTable";
    
    private Accumulo accumuloConfig = new Accumulo();
    
    private int concurrency = 1;
    
    private Health health = new Health();
    
    public String getTableName() {
        return tableName;
    }
    
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
    public Accumulo getAccumuloConfig() {
        return accumuloConfig;
    }
    
    public void setAccumuloConfig(Accumulo accumuloConfig) {
        this.accumuloConfig = accumuloConfig;
    }
    
    public int getConcurrency() {
        return concurrency;
    }
    
    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }
    
    public Health getHealth() {
        return health;
    }
    
    public void setHealth(Health health) {
        this.health = health;
    }
    
    public static class Accumulo {
        private String zookeepers;
        private String instanceName;
        private String username;
        private String password;
        
        public String getZookeepers() {
            return zookeepers;
        }
        
        public void setZookeepers(String zookeepers) {
            this.zookeepers = zookeepers;
        }
        
        public String getInstanceName() {
            return instanceName;
        }
        
        public void setInstanceName(String instanceName) {
            this.instanceName = instanceName;
        }
        
        public String getUsername() {
            return username;
        }
        
        public void setUsername(String username) {
            this.username = username;
        }
        
        public String getPassword() {
            return password;
        }
        
        public void setPassword(String password) {
            this.password = password;
        }
    }
    
    public static class Health {
        private Long hungTimeout = 5L;
        
        private TimeUnit hungTimeoutUnit = TimeUnit.MINUTES;
        
        // The minimum percentage of hung audit consumers (expressed as a number
        // between 0 and 1, inclusive) required to mark the service as down
        private double percentHungFailureThreshold = 0.5;
        
        public Long getHungAuditTimeoutMillis() {
            return hungTimeoutUnit.toMillis(hungTimeout);
        }
        
        public Long getHungTimeout() {
            return hungTimeout;
        }
        
        public void setHungTimeout(Long hungTimeout) {
            this.hungTimeout = hungTimeout;
        }
        
        public TimeUnit getHungTimeoutUnit() {
            return hungTimeoutUnit;
        }
        
        public void setHungTimeoutUnit(TimeUnit hungTimeoutUnit) {
            this.hungTimeoutUnit = hungTimeoutUnit;
        }
        
        public double getPercentHungFailureThreshold() {
            return percentHungFailureThreshold;
        }
        
        public void setPercentHungFailureThreshold(double percentHungFailureThreshold) {
            this.percentHungFailureThreshold = percentHungFailureThreshold;
        }
    }
}
