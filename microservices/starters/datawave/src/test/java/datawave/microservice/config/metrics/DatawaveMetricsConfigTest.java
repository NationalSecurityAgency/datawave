package datawave.microservice.config.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;

import datawave.metrics.MetricsReporterBuilder;
import datawave.metrics.TimeoutConfigurableMetricsReporterBuilder;

class DatawaveMetricsConfigTest {

    @Test
    void bindsOptionalReporterTimeout() {
        assertNull(bind(Map.of()).getTimeout());
        assertEquals(Duration.ofMillis(125), bind(Map.of("metrics.reporter.timeout", "125ms")).getTimeout());
        assertEquals(Duration.ofSeconds(2), bind(Map.of("metrics.reporter.timeout", "2")).getTimeout());
    }

    @Test
    void appliesTimeoutOnlyToCapableBuilders() {
        TimeoutBuilder capable = new TimeoutBuilder();
        DatawaveMetricsConfig.configureTimeout(capable, Duration.ofMillis(125));
        assertEquals(125, capable.timeoutMillis);

        PlainBuilder plain = new PlainBuilder();
        DatawaveMetricsConfig.configureTimeout(plain, null);
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                        () -> DatawaveMetricsConfig.configureTimeout(plain, Duration.ofMillis(125)));
        assertEquals("metrics.reporter.timeout is not supported by " + PlainBuilder.class.getName(), failure.getMessage());
    }

    private MetricsConfigurationProperties bind(Map<String,String> properties) {
        Binder binder = new Binder(new MapConfigurationPropertySource(properties));
        return binder.bind("metrics.reporter", Bindable.of(MetricsConfigurationProperties.class)).orElseGet(MetricsConfigurationProperties::new);
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
