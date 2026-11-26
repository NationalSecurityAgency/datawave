package datawave.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;

class MetricsCdiConfigurationTest {

    @Test
    void appliesConfiguredTimeoutOnlyToCapableBuilders() {
        TimeoutBuilder capable = new TimeoutBuilder();
        MetricsCdiConfiguration.configureTimeout(capable, 125);
        assertEquals(125, capable.timeoutMillis);

        PlainBuilder plain = new PlainBuilder();
        MetricsCdiConfiguration.configureTimeout(plain, -1);
        IllegalArgumentException unsupported = assertThrows(IllegalArgumentException.class,
                        () -> MetricsCdiConfiguration.configureTimeout(plain, 125));
        assertEquals("dw.metrics.reporter.timeout.millis is not supported by " + PlainBuilder.class.getName(), unsupported.getMessage());

        IllegalArgumentException invalid = assertThrows(IllegalArgumentException.class,
                        () -> MetricsCdiConfiguration.configureTimeout(capable, 0));
        assertEquals("dw.metrics.reporter.timeout.millis must be positive", invalid.getMessage());
    }

    private static class PlainBuilder extends MetricsReporterBuilder {
        private PlainBuilder() {
            super(new MetricRegistry());
        }

        @Override
        public ScheduledReporter build(String host, int port) {
            return null;
        }
    }

    private static class TimeoutBuilder extends PlainBuilder implements TimeoutConfigurableMetricsReporterBuilder {
        private long timeoutMillis;

        @Override
        public MetricsReporterBuilder withTimeout(long timeout, TimeUnit unit) {
            timeoutMillis = unit.toMillis(timeout);
            return this;
        }
    }
}
