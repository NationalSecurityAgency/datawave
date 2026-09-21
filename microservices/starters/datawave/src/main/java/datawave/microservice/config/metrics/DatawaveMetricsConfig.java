package datawave.microservice.config.metrics;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.springframework.context.annotation.Configuration;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.ryantenney.metrics.spring.config.annotation.EnableMetrics;
import com.ryantenney.metrics.spring.config.annotation.MetricsConfigurerAdapter;

import datawave.metrics.MetricsReporterFactory;
import datawave.metrics.MetricsReporterBuilder;
import datawave.metrics.TimeoutConfigurableMetricsReporterBuilder;

/**
 * Configuration for annotated DropWizard metrics.
 *
 * @see com.codahale.metrics.annotation.Timed
 * @see com.codahale.metrics.annotation.Counted
 */
@Configuration
@EnableMetrics
public class DatawaveMetricsConfig extends MetricsConfigurerAdapter {
    private final MetricsConfigurationProperties configProps;

    public DatawaveMetricsConfig(MetricsConfigurationProperties configProps) {
        this.configProps = configProps;
    }

    @Override
    public void configureReporters(MetricRegistry metricRegistry) {
        try {
            MetricsReporterFactory factory = configProps.getFactoryClass().getDeclaredConstructor().newInstance();
            MetricsReporterBuilder builder = factory.forRegistry(metricRegistry).convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS);
            configureTimeout(builder, configProps.getTimeout());
            ScheduledReporter reporter = builder.build(configProps.getHost(), configProps.getPort());
            registerReporter(reporter).start(configProps.getInterval(), configProps.getIntervalUnit());
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Unable to instantiate metrics reporter factory class " + configProps.getFactoryClass() + ": " + e.getMessage(), e);
        }
    }

    static void configureTimeout(MetricsReporterBuilder builder, Duration timeout) {
        if (timeout == null) {
            return;
        }
        if (!(builder instanceof TimeoutConfigurableMetricsReporterBuilder)) {
            throw new IllegalArgumentException("metrics.reporter.timeout is not supported by " + builder.getClass().getName());
        }
        ((TimeoutConfigurableMetricsReporterBuilder) builder).withTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }
}
