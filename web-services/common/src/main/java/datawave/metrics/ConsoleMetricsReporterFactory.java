package datawave.metrics;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;

public class ConsoleMetricsReporterFactory implements MetricsReporterFactory {
    @Override
    public MetricsReporterBuilder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }
    
    public static class Builder extends MetricsReporterBuilder {
        protected Builder(MetricRegistry registry) {
            super(registry);
        }
        
        @Override
        public ScheduledReporter build(String host, int port) {
            return ConsoleReporter.forRegistry(registry).convertDurationsTo(durationUnit).convertRatesTo(rateUnit).filter(filter).build();
        }
    }
}
