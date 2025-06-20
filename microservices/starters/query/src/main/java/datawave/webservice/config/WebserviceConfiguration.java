package datawave.webservice.config;

import java.util.List;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import datawave.core.common.cache.AccumuloTableCacheProperties;
import datawave.core.query.result.event.DefaultResponseObjectFactory;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.webservice.query.result.event.ResponseObjectFactory;

@Configuration
@EnableConfigurationProperties(TestDatawaveUserServiceProperties.class)
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

    @Bean
    public ResponseObjectFactory defaultResponseObjectFactory() {
        return new DefaultResponseObjectFactory();
    }

    @Bean
    public Map<SubjectIssuerDNPair,DatawaveUser> testDatawaveUsers(TestDatawaveUserServiceProperties testDatawaveUserServiceProperties) {
        return testDatawaveUserServiceProperties.getTestUsers().stream().collect(
                        java.util.stream.Collectors.toMap(TestDatawaveUserProperties::toSubjectIssuerDNPair, TestDatawaveUserProperties::toDatawaveUser));

    }
}
