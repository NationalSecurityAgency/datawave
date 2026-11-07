package datawave.metrics;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeUnit;

import org.apache.deltaspike.core.api.config.ConfigProperty;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;

import datawave.metrics.cdi.DatawaveMetricRegistry;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Initialized;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Produces;

/**
 * Configuration for the CDI Dropwizard {@link MetricRegistry}. The registry produced here is the shared {@link DatawaveMetricRegistry}, which the
 * {@code @Timed} interceptor and the {@code @Metric} counter producer record into, and this class starts the reporter that publishes it.
 */
@ApplicationScoped
public class MetricsCdiConfiguration {
    private ScheduledReporter statsReporter;

    @Produces
    @ApplicationScoped
    public MetricRegistry metricRegistry(@ConfigProperty(name = "dw.metrics.reporter.host", defaultValue = "localhost") String reportHost,
                    @ConfigProperty(name = "dw.metrics.reporter.port", defaultValue = "54321") int reportPort,
                    @ConfigProperty(name = "dw.metrics.reporter.report.interval.value", defaultValue = "30") int reportInterval,
                    @ConfigProperty(name = "dw.metrics.reporter.report.interval.units", defaultValue = "SECONDS") String reportIntervalTimeUnit,
                    @ConfigProperty(name = "dw.metrics.reporter.class", defaultValue = "datawave.metrics.TimelyMetricsReporterFactory") String reporterClass) {
        MetricRegistry metricRegistry = DatawaveMetricRegistry.get();
        try {
            MetricsReporterFactory factory = MetricsReporterFactory.class.cast(Class.forName(reporterClass).getDeclaredConstructor().newInstance());
            statsReporter = factory.forRegistry(metricRegistry).convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build(reportHost,
                            reportPort);
            statsReporter.start(reportInterval, TimeUnit.valueOf(reportIntervalTimeUnit));
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(
                            "Metrics reporter class " + reporterClass + " does not exist or is not a " + MetricsReporterFactory.class.getName(), e);
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new IllegalArgumentException("Metrics reporter class " + reporterClass + " could not be instantiated: " + e.getMessage(), e);
        }
        return metricRegistry;
    }

    /**
     * Creates the registry, and with it the reporter, when the application starts. Nothing else injects the registry: metrics are recorded through
     * {@link DatawaveMetricRegistry} directly.
     */
    public void startReporting(@Observes @Initialized(ApplicationScoped.class) Object init, MetricRegistry metricRegistry) {
        metricRegistry.getNames();
    }

    @PreDestroy
    public void shutdown() {
        if (statsReporter != null) {
            statsReporter.stop();
        }
    }
}
