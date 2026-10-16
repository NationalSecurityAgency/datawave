package datawave.metrics;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;

public class TimelyMetricsReporterFactory implements MetricsReporterFactory {
    @Override
    public Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    public static class Builder extends MetricsReporterBuilder {
        private int connectTimeoutMillis = TimelyMetricsReporter.DEFAULT_CONNECT_TIMEOUT_MILLIS;

        protected Builder(MetricRegistry registry) {
            super(registry);
        }

        public Builder withConnectTimeout(long timeout, TimeUnit unit) {
            long millis = Objects.requireNonNull(unit, "unit").toMillis(timeout);
            if (millis <= 0 || millis > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("timeout must be between 1 and " + Integer.MAX_VALUE + " milliseconds");
            }
            connectTimeoutMillis = (int) millis;
            return this;
        }

        @Override
        public TimelyMetricsReporter build(String host, int port) {
            return new TimelyMetricsReporter(host, port, registry, "timely-reporter", filter, rateUnit, durationUnit, connectTimeoutMillis);
        }

    }
}
