package datawave.microservice.annotation.writers.accumulo.health;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

import datawave.microservice.annotation.writers.accumulo.AccumuloAnnotationWriter;
import datawave.microservice.annotation.writers.accumulo.config.AccumuloAnnotationWriterProperties;

public class AccumuloHealthChecker implements HealthIndicator {
    private AccumuloAnnotationWriterProperties accumuloAnnotationWriterProperties;
    private AccumuloAnnotationWriter accumuloAnnotationWriter;

    public AccumuloHealthChecker(AccumuloAnnotationWriterProperties accumuloAnnotationWriterProperties, AccumuloAnnotationWriter accumuloAnnotationWriter) {
        this.accumuloAnnotationWriterProperties = accumuloAnnotationWriterProperties;
        this.accumuloAnnotationWriter = accumuloAnnotationWriter;
    }

    @Override
    public Health health() {
        long currentTime = System.currentTimeMillis();
        Map<String,Long> auditTimers = new HashMap<>(accumuloAnnotationWriter.getWriteTimers());

        long hungTimeoutMillis = accumuloAnnotationWriterProperties.getHealth().getHungAnnotationWriterTimeoutMillis();

        int numHungConsumers = 0;
        for (Map.Entry<String,Long> auditTimer : auditTimers.entrySet()) {
            if ((currentTime - auditTimer.getValue()) > hungTimeoutMillis) {
                numHungConsumers++;
            }
        }

        double percentHung = (double) numHungConsumers / accumuloAnnotationWriterProperties.getConcurrency();

        Health.Builder healthBuilder = new Health.Builder();
        healthBuilder.withDetail("percentHung", (percentHung * 100) + "%");
        healthBuilder.withDetail("numConsumers", accumuloAnnotationWriterProperties.getConcurrency());
        healthBuilder.withDetail("numHungConsumers", numHungConsumers);
        healthBuilder.withDetail("hungTimeoutMillis", hungTimeoutMillis);
        healthBuilder.withDetail("percentHungFailureThreshold", (accumuloAnnotationWriterProperties.getHealth().getPercentHungFailureThreshold() * 100) + "%");

        // if the hung threshold is greater than or equal to the failure threshold, mark the service as down
        if (percentHung >= accumuloAnnotationWriterProperties.getHealth().getPercentHungFailureThreshold()) {
            healthBuilder.down();
        } else {
            healthBuilder.up();
        }

        return healthBuilder.build();
    }
}
