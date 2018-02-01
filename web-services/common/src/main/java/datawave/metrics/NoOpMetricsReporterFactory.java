package datawave.metrics;

import com.codahale.metrics.MetricRegistry;

public class NoOpMetricsReporterFactory implements MetricsReporterFactory {
    @Override
    public MetricsReporterBuilder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }
    
    public static class Builder extends MetricsReporterBuilder {
        
        protected Builder(MetricRegistry registry) {
            super(registry);
        }
        
        @Override
        public NoOpMetricsReporter build(String host, int port) {
            return new NoOpMetricsReporter(registry, "noop-reporter", filter, rateUnit, durationUnit);
        }
        
    }
}
