package datawave.webservice.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class QueryMetricIngestConfiguration {

    @Bean
    @ConfigurationProperties("datawave.query.metric.handler")
    public QueryMetricIngestProperties queryMetricIngestProperties() {
        return new QueryMetricIngestProperties();
    }
}
