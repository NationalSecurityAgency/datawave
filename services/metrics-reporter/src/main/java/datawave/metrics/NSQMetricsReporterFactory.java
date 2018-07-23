package datawave.metrics;

import com.codahale.metrics.MetricRegistry;

public class NSQMetricsReporterFactory implements MetricsReporterFactory {
    @Override
    public MetricsReporterBuilder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }
    
    public static class Builder extends MetricsReporterBuilder {
        
        protected Builder(MetricRegistry registry) {
            super(registry);
        }
        
        @Override
        public NSQMetricsReporter build(String host, int port) {
            return new NSQMetricsReporter(host, port, registry, "nsq-timely-reporter", filter, rateUnit, durationUnit);
        }
        
    }
}
