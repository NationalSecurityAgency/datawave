package datawave.microservice.audit.accumulo.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "dw.audit.accumulo")
public class AccumuloAuditProperties {
    
    private boolean enabled;
    private String queueName;
    private boolean durable;
    private String tableName;
    
    private Accumulo accumuloConfig = new Accumulo();
    
    public boolean isEnabled() {
        return enabled;
    }
    
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
    
    public String getQueueName() {
        return queueName;
    }
    
    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }
    
    public boolean isDurable() {
        return durable;
    }
    
    public void setDurable(boolean durable) {
        this.durable = durable;
    }
    
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
}
