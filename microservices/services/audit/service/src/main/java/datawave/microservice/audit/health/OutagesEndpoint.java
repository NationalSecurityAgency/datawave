package datawave.microservice.audit.health;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.stereotype.Component;

/**
 * This adds an additional actuator endpoint, 'mgmt/outages', which provides information about infrastructure outages experienced by the Audit Service.
 */
@Component
@Endpoint(id = "outages")
public class OutagesEndpoint {
    
    @Autowired(required = false)
    private HealthChecker healthChecker;
    
    /**
     * In the event that a health checker is configured, this will return a list of outages experience by the audit service.
     * 
     * @return Outage statistics for the audit service infrastructure
     */
    @ReadOperation
    public Object outages() {
        if (healthChecker != null)
            return healthChecker.getOutageStats();
        return "To see outages, enable the health checker.";
    }
}
