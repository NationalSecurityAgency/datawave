package datawave.microservice.accumulo.lookup.config;

import javax.validation.constraints.Min;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "accumulo.lookup")
public class LookupProperties {
    
    private boolean enabled = true;
    
    @Min(1)
    private int numQueryThreads = 8;
    
    public int getNumQueryThreads() {
        return numQueryThreads;
    }
    
    public void setNumQueryThreads(int numQueryThreads) {
        this.numQueryThreads = numQueryThreads;
    }
    
    public boolean isEnabled() {
        return enabled;
    }
    
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
}
