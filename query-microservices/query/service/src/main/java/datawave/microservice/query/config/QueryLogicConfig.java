package datawave.microservice.query.config;

import org.springframework.validation.annotation.Validated;

// TODO: This will be a common property file used by multiple services, so we will eventually need to move it out
@Validated
public class QueryLogicConfig {
    
    private boolean metricsEnabled = false;
    
    public boolean isMetricsEnabled() {
        return metricsEnabled;
    }
    
    public void setMetricsEnabled(boolean metricsEnabled) {
        this.metricsEnabled = metricsEnabled;
    }
}
