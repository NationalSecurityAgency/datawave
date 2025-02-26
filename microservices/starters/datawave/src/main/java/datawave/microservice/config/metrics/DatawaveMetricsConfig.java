package datawave.microservice.config.metrics;

import java.util.concurrent.TimeUnit;

import org.springframework.context.annotation.Configuration;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.ryantenney.metrics.spring.config.annotation.EnableMetrics;
import com.ryantenney.metrics.spring.config.annotation.MetricsConfigurerAdapter;

import datawave.metrics.MetricsReporterFactory;

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
            MetricsReporterFactory factory = configProps.getFactoryClass().newInstance();
            ScheduledReporter reporter = factory.forRegistry(metricRegistry).convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS)
                            .build(configProps.getHost(), configProps.getPort());
            registerReporter(reporter).start(configProps.getInterval(), configProps.getIntervalUnit());
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException("Unable to instantiate metrics reporter factory class " + configProps.getFactoryClass() + ": " + e.getMessage(), e);
        }
    }
}
