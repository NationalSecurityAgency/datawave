package datawave.microservice.query.config;

import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;

@Validated
public class QueryLogicProperties {
    @Valid
    private boolean metricsEnabled = false;
    
    public boolean isMetricsEnabled() {
        return metricsEnabled;
    }
    
    public void setMetricsEnabled(boolean metricsEnabled) {
        this.metricsEnabled = metricsEnabled;
    }
}
