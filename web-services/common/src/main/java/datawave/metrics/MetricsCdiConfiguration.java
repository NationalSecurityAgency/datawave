package datawave.metrics;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import org.apache.deltaspike.core.api.config.ConfigProperty;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;

/**
 * Configuration for the CDI Dropwizard {@link MetricRegistry}.
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
                    @ConfigProperty(name = "dw.metrics.reporter.class", defaultValue = "datawave.metrics.TimelyMetricsReporterFactory") String reporterClass,
                    @ConfigProperty(name = "dw.metrics.reporter.timeout.millis", defaultValue = "-1") long reporterTimeoutMillis) {
        MetricRegistry metricRegistry = new MetricRegistry();
        try {
            MetricsReporterFactory factory = MetricsReporterFactory.class.cast(Class.forName(reporterClass).getDeclaredConstructor().newInstance());
            MetricsReporterBuilder builder = factory.forRegistry(metricRegistry).convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS);
            configureTimeout(builder, reporterTimeoutMillis);
            statsReporter = builder.build(reportHost, reportPort);
            statsReporter.start(reportInterval, TimeUnit.valueOf(reportIntervalTimeUnit));
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(
                            "Metrics reporter class " + reporterClass + " does not exist or is not a " + MetricsReporterFactory.class.getName(), e);
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new IllegalArgumentException("Metrics reporter class " + reporterClass + " could not be instantiated: " + e.getMessage(), e);
        }
        return metricRegistry;
    }

    public MetricRegistry metricRegistry(String reportHost, int reportPort, int reportInterval, String reportIntervalTimeUnit, String reporterClass) {
        return metricRegistry(reportHost, reportPort, reportInterval, reportIntervalTimeUnit, reporterClass, -1);
    }

    static void configureTimeout(MetricsReporterBuilder builder, long timeoutMillis) {
        if (timeoutMillis == -1) {
            return;
        }
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("dw.metrics.reporter.timeout.millis must be positive");
        }
        if (!(builder instanceof TimeoutConfigurableMetricsReporterBuilder)) {
            throw new IllegalArgumentException("dw.metrics.reporter.timeout.millis is not supported by " + builder.getClass().getName());
        }
        ((TimeoutConfigurableMetricsReporterBuilder) builder).withTimeout(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void shutdown() {
        if (statsReporter != null) {
            statsReporter.stop();
        }
    }
}
