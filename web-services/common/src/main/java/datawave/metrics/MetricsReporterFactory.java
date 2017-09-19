package datawave.metrics;

import com.codahale.metrics.MetricRegistry;

public interface MetricsReporterFactory {
    MetricsReporterBuilder forRegistry(MetricRegistry registry);
}
