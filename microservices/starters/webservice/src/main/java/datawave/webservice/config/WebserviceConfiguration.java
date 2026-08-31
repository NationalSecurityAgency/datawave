package datawave.webservice.config;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import datawave.core.common.cache.AccumuloTableCacheProperties;

@Configuration
public class WebserviceConfiguration {

    @Bean
    @ConfigurationProperties("datawave.table.cache")
    public AccumuloTableCacheProperties tableCacheProperties() {
        return new AccumuloTableCacheProperties();
    }

    @Bean(name = "cachedMetadataTableNames")
    public List<String> cachedMetadataTableNames(AccumuloTableCacheProperties tableCacheProperties) {
        return tableCacheProperties.getTableNames();
    }

    @Bean
    @ConfigurationProperties("datawave.query.metrics")
    public QueryMetricsWriterProperties queryMetricsWriterProperties() {
        return new QueryMetricsWriterProperties();
    }

    @Bean
    @ConfigurationProperties("datawave.query.metrics.timely")
    public QueryMetricsTimelyProperties queryMetricsTimelyProperties() {
        return new QueryMetricsTimelyProperties();
    }
}
