package datawave.microservice.audit.auditors.accumulo.health;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

import datawave.microservice.audit.auditors.accumulo.AccumuloAuditor;
import datawave.microservice.audit.auditors.accumulo.config.AccumuloAuditProperties;

public class AccumuloHealthChecker implements HealthIndicator {
    
    private AccumuloAuditProperties accumuloAuditProperties;
    private AccumuloAuditor accumuloAuditor;
    
    public AccumuloHealthChecker(AccumuloAuditProperties accumuloAuditProperties, AccumuloAuditor accumuloAuditor) {
        this.accumuloAuditProperties = accumuloAuditProperties;
        this.accumuloAuditor = accumuloAuditor;
    }
    
    @Override
    public Health health() {
        long currentTime = System.currentTimeMillis();
        Map<String,Long> auditTimers = new HashMap<>(accumuloAuditor.getAuditTimers());
        
        long hungTimeoutMillis = accumuloAuditProperties.getHealth().getHungAuditTimeoutMillis();
        
        int numHungConsumers = 0;
        for (Map.Entry<String,Long> auditTimer : auditTimers.entrySet()) {
            if ((currentTime - auditTimer.getValue()) > hungTimeoutMillis) {
                numHungConsumers++;
            }
        }
        
        double percentHung = (double) numHungConsumers / accumuloAuditProperties.getConcurrency();
        
        Health.Builder healthBuilder = new Health.Builder();
        healthBuilder.withDetail("percentHung", (percentHung * 100) + "%");
        healthBuilder.withDetail("numConsumers", accumuloAuditProperties.getConcurrency());
        healthBuilder.withDetail("numHungConsumers", numHungConsumers);
        healthBuilder.withDetail("hungTimeoutMillis", hungTimeoutMillis);
        healthBuilder.withDetail("percentHungFailureThreshold", (accumuloAuditProperties.getHealth().getPercentHungFailureThreshold() * 100) + "%");
        
        // if the hung threshold is greater than or equal to the failure threshold, mark the service as down
        if (percentHung >= accumuloAuditProperties.getHealth().getPercentHungFailureThreshold()) {
            healthBuilder.down();
        } else {
            healthBuilder.up();
        }
        
        return healthBuilder.build();
    }
}
