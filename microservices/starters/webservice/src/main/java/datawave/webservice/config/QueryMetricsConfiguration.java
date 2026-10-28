package datawave.webservice.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class QueryMetricsConfiguration {
    @Bean
    @ConfigurationProperties("datawave.query.metric.writer")
    public QueryMetricsWriterProperties queryMetricsWriterProperties() {
        return new QueryMetricsWriterProperties();
    }

    @Bean
    @ConfigurationProperties("datawave.query.metric.timely")
    public QueryMetricsTimelyProperties queryMetricsTimelyProperties() {
        return new QueryMetricsTimelyProperties();
    }
}
