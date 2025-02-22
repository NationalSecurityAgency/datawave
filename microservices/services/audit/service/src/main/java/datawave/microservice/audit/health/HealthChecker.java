package datawave.microservice.audit.health;

import java.util.List;
import java.util.Map;

/**
 * This is the base interface which defines the methods that need to be implemented by a given health checker.
 */
public interface HealthChecker {
    
    /**
     * Returns the poll interval which should be used for health checks.
     *
     * @return The health checker poll interval in millis.
     */
    long pollIntervalMillis();
    
    /**
     * Attempts to perform recovery actions to address issues with the messaging infrastructure.
     */
    void recover();
    
    /**
     * Checks the health of the messaging infrastructure.
     */
    void runHealthCheck();
    
    /**
     * Determines whether the messaging infrastructure is healthy.
     * 
     * @return a boolean indicating whether the messaging infrastructure is healthy
     */
    boolean isHealthy();
    
    /**
     * Returns information about messaging infrastructure outages, if any.
     * 
     * @return a list of messaging infrastructure outages experienced by the audit service
     */
    List<Map<String,Object>> getOutageStats();
}
