package datawave.webservice.config;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;

import datawave.metrics.ConsoleMetricsReporterFactory;
import datawave.metrics.MetricsReporterFactory;
import datawave.metrics.NSQMetricsReporterFactory;
import datawave.metrics.NoOpMetricsReporterFactory;
import datawave.metrics.StatsDMetricReporterFactory;
import datawave.metrics.TimelyMetricsReporterFactory;
import datawave.webservice.DatawaveResourceFactory;

@Configuration
public class DropwizardMetricsConfiguration implements ApplicationContextAware {

    private ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    @Bean
    @ConfigurationProperties("datawave.metric.reporter")
    public DropwizardMetricsProperties dropwizardMetricsProperties() {
        return new DropwizardMetricsProperties();
    }

    @Bean
    public MetricRegistry metricRegistry() {
        return new MetricRegistry();
    }

    @Bean
    public DatawaveResourceFactory datawaveResourceFactory() {
        return new DatawaveResourceFactory();
    }

    @Bean
    @DependsOn({"consoleMetricsReporterFactory", "noOpMetricsReporterFactory", "nsqMetricsReporterFactory", "statsDMetricReporterFactory",
            "timelyMetricsReporterFactory"})
    public MetricsReporterFactory metricsReporterFactory(DatawaveResourceFactory datawaveResourceFactory) {
        String className = applicationContext.getEnvironment().getProperty("datawave.active-implementation.metrics-reporter-factory",
                        TimelyMetricsReporterFactory.class.getName());
        return (MetricsReporterFactory) datawaveResourceFactory.getBeanOfClass(applicationContext, MetricsReporterFactory.class, className,
                        List.of("metricsReporterFactory"));
    }

    @Bean
    public ScheduledReporter metricsReporter(MetricRegistry metricRegistry, MetricsReporterFactory metricsReporterFactory,
                    DropwizardMetricsProperties dropwizardMetricsProperties) {
        ScheduledReporter reporter = metricsReporterFactory.forRegistry(metricRegistry).convertRatesTo(TimeUnit.SECONDS)
                        .convertDurationsTo(TimeUnit.MILLISECONDS).build(dropwizardMetricsProperties.getHost(), dropwizardMetricsProperties.getPort());
        reporter.start(dropwizardMetricsProperties.getReportInterval(), TimeUnit.valueOf(dropwizardMetricsProperties.getReportIntervalTimeUnit()));
        return reporter;
    }

    // register all MetricsReportedFactory implementations as beans so that they can be referenced
    // by simple class name or fully qualified class name when using DatawaveResourceFactory
    // to create the MetricsReporterFactory bean

    @Bean
    public ConsoleMetricsReporterFactory consoleMetricsReporterFactory() {
        return new ConsoleMetricsReporterFactory();
    }

    @Bean
    public NoOpMetricsReporterFactory noOpMetricsReporterFactory() {
        return new NoOpMetricsReporterFactory();
    }

    @Bean
    public NSQMetricsReporterFactory nsqMetricsReporterFactory() {
        return new NSQMetricsReporterFactory();
    }

    @Bean
    public StatsDMetricReporterFactory statsDMetricReporterFactory() {
        return new StatsDMetricReporterFactory();
    }

    @Bean
    public TimelyMetricsReporterFactory timelyMetricsReporterFactory() {
        return new TimelyMetricsReporterFactory();
    }
}
