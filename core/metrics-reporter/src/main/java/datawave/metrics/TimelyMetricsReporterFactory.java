package datawave.metrics;

import com.codahale.metrics.MetricRegistry;

public class TimelyMetricsReporterFactory implements MetricsReporterFactory {
    @Override
    public MetricsReporterBuilder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }
    
    public static class Builder extends MetricsReporterBuilder {
        
        protected Builder(MetricRegistry registry) {
            super(registry);
        }
        
        @Override
        public TimelyMetricsReporter build(String host, int port) {
            return new TimelyMetricsReporter(host, port, registry, "timely-reporter", filter, rateUnit, durationUnit);
        }
        
    }
}
