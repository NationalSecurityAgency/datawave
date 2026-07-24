package datawave.metrics;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;

public class NSQMetricsReporterFactory implements MetricsReporterFactory {
    @Override
    public Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    public static class Builder extends MetricsReporterBuilder {
        private int timeoutMillis = NSQMetricsReporter.DEFAULT_TIMEOUT_MILLIS;

        protected Builder(MetricRegistry registry) {
            super(registry);
        }

        public Builder withTimeout(long timeout, TimeUnit unit) {
            long millis = Objects.requireNonNull(unit, "unit").toMillis(timeout);
            if (millis <= 0 || millis > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("timeout must be between 1 and " + Integer.MAX_VALUE + " milliseconds");
            }
            timeoutMillis = (int) millis;
            return this;
        }

        @Override
        public NSQMetricsReporter build(String host, int port) {
            return new NSQMetricsReporter(host, port, registry, "nsq-timely-reporter", filter, rateUnit, durationUnit, timeoutMillis);
        }

    }
}
