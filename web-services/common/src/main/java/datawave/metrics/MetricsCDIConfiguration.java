package datawave.metrics;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;

import datawave.configuration.spring.SpringBean;

/**
 * Configuration for the CDI Dropwizard {@link MetricRegistry}.
 */
@ApplicationScoped
public class MetricsCDIConfiguration {
    private ScheduledReporter statsReporter;

    @Inject
    @SpringBean
    private MetricsCDIProperties metricsCDIProperties;

    @Produces
    @ApplicationScoped
    public MetricRegistry metricRegistry() {
        MetricRegistry metricRegistry = new MetricRegistry();
        String reporterClass = metricsCDIProperties.getReporterClass();
        String reportHost = metricsCDIProperties.getReportHost();
        int reportPort = metricsCDIProperties.getReportPort();
        long reportInterval = metricsCDIProperties.getReportInterval();
        String reportIntervalTimeUnit = metricsCDIProperties.getReportIntervalTimeUnit();
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

    @PreDestroy
    public void shutdown() {
        if (statsReporter != null) {
            statsReporter.stop();
        }
    }
}
