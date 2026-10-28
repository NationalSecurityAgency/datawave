package datawave.webservice.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WebserviceConfiguration {

    @Bean
    @ConfigurationProperties(prefix = "datawave.atom")
    public AtomProperties atomProperties() {
        return new AtomProperties();
    }

    @Bean
    @ConfigurationProperties(prefix = "datawave.model")
    public ModelProperties modelProperties() {
        return new ModelProperties();
    }

    @Bean
    @ConfigurationProperties(prefix = "datawave.query.metric.handler")
    public QueryMetricHandlerProperties queryMetricHandlerProperties() {
        return new QueryMetricHandlerProperties();
    }

    @Bean
    @ConfigurationProperties(prefix = "datawave.query.metric.writer")
    public QueryMetricsWriterProperties queryMetricsWriterProperties() {
        return new QueryMetricsWriterProperties();
    }

    @Bean
    @ConfigurationProperties(prefix = "datawave.query.metric.timely")
    public QueryMetricsTimelyProperties queryMetricsTimelyProperties() {
        return new QueryMetricsTimelyProperties();
    }
}
