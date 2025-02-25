package datawave.metrics;

import com.codahale.metrics.MetricRegistry;

public class StatsDMetricReporterFactory implements MetricsReporterFactory {
    @Override
    public MetricsReporterBuilder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }
    
    public static class Builder extends MetricsReporterBuilder {
        
        private Builder(MetricRegistry registry) {
            super(registry);
        }
        
        public StatsDMetricReporter build(String host, int port) {
            return build(new StatsDClient(prefix, host, port));
        }
        
        public StatsDMetricReporter build(StatsDClient statsDClient) {
            return new StatsDMetricReporter(registry, statsDClient, filter, rateUnit, durationUnit);
        }
    }
}
