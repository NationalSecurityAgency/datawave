package datawave.metrics;

import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import org.apache.deltaspike.core.api.config.ConfigProperty;

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
                    @ConfigProperty(name = "dw.metrics.reporter.class", defaultValue = "datawave.metrics.TimelyMetricsReporterFactory") String reporterClass) {
        MetricRegistry metricRegistry = new MetricRegistry();
        try {
            MetricsReporterFactory factory = MetricsReporterFactory.class.cast(Class.forName(reporterClass).newInstance());
            statsReporter = factory.forRegistry(metricRegistry).convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build(reportHost,
                            reportPort);
            statsReporter.start(reportInterval, TimeUnit.valueOf(reportIntervalTimeUnit));
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(
                            "Metrics reporter class " + reporterClass + " does not exist or is not a " + MetricsReporterFactory.class.getName(), e);
        } catch (InstantiationException | IllegalAccessException e) {
            throw new IllegalArgumentException("Metrics reporter class " + reporterClass + " could not be instantiated: " + e.getMessage(), e);
        }
        return metricRegistry;
    }
    
    @PreDestroy
    public void shutdown() {
        if (statsReporter != null) {
            statsReporter.stop();
        }
    }
}
