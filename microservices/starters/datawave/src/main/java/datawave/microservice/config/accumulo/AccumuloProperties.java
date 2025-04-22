package datawave.microservice.config.accumulo;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import org.springframework.validation.annotation.Validated;

/**
 * Properties that define a connection to Accumulo. Typically, this properties object will be included as a nested property in a
 * {@link org.springframework.boot.context.properties.ConfigurationProperties} object, and then produced with a
 * {@link org.springframework.beans.factory.annotation.Qualifier} of either "warehouse" or "metrics"
 */
@Validated
public class AccumuloProperties {
    @NotEmpty
    private String zookeepers;
    @NotEmpty
    private String instanceName;
    @NotEmpty
    private String username;
    @NotNull
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
