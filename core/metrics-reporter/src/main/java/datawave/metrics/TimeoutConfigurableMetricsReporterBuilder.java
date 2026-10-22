package datawave.metrics;

import java.util.concurrent.TimeUnit;

/**
 * Capability for reporter builders that accept a network timeout.
 */
public interface TimeoutConfigurableMetricsReporterBuilder {
    MetricsReporterBuilder withTimeout(long timeout, TimeUnit unit);
}
