package datawave.webservice.config;

import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import org.springframework.beans.factory.annotation.CustomAutowireConfigurer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.annotation.Metric;

import datawave.core.common.cache.AccumuloTableCacheProperties;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUserService;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.authorization.remote.RemoteDatawaveUserService;
import datawave.security.cert.SSLStores;
import datawave.webservice.common.json.ObjectMapperDecorator;
import datawave.webservice.common.remote.RemoteHttpServiceProperties;
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
    public Map<SubjectIssuerDNPair,DatawaveUser> testDatawaveUsers(TestDatawaveUserServiceProperties testDatawaveUserServiceProperties) {
        return testDatawaveUserServiceProperties.getTestUsers().stream().collect(
                        java.util.stream.Collectors.toMap(TestDatawaveUserProperties::toSubjectIssuerDNPair, TestDatawaveUserProperties::toDatawaveUser));

    }

    @Bean
    @Qualifier("serverUserDetailsSupplier")
    public Supplier<ProxiedUserDetails> serverUserDetailsSupplier(@Qualifier("remoteDatawaveUserService") DatawaveUserService datawaveUserService,
                    SSLStores sslStores) {
        return new Supplier<>() {
            @Override
            public ProxiedUserDetails get() {
                try {
                    return new DatawavePrincipal(datawaveUserService.lookup(Collections.singleton(lookupServerDN(sslStores))));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    @Bean
    @ConfigurationProperties("dw.remote-datawave-user-service")
    @Qualifier("remoteDatawaveUserServiceProperties")
    public RemoteHttpServiceProperties remoteDatawaveUserServiceProperties() {
        return new RemoteHttpServiceProperties();
    }

    @Bean
    public static CustomAutowireConfigurer metricQualifierConfigurer() {
        CustomAutowireConfigurer configurer = new CustomAutowireConfigurer();
        configurer.setCustomQualifierTypes(Collections.singleton(Metric.class));
        return configurer;
    }

    @Bean
    public MetricRegistry metricRegistry() {
        return new MetricRegistry();
    }

    @Bean
    @Metric(name = "dw.remoteDatawaveUserService.retries", absolute = true)
    public Counter remoteDatawaveUserServiceRetryCounter(MetricRegistry registry) {
        return registry.counter("dw.remoteDatawaveUserService.retries");
    }

    @Bean
    @Metric(name = "dw.remoteDatawaveUserService.failures", absolute = true)
    public Counter remoteDatawaveUserServiceFailureCounter(MetricRegistry registry) {
        return registry.counter("dw.remoteDatawaveUserService.failures");
    }

    @Bean
    @Qualifier("remoteDatawaveUserService")
    public DatawaveUserService remoteDatawaveUserService(
                    @Qualifier("remoteDatawaveUserServiceProperties") RemoteHttpServiceProperties remoteHttpServiceProperties, SSLStores sslStores,
                    ObjectMapperDecorator objectMapperDecorator, ResponseObjectFactory responseObjectFactory, ExecutorService executorService) {
        RemoteDatawaveUserService remoteDatawaveUserService = new RemoteDatawaveUserService();
        remoteDatawaveUserService.setConfig(remoteHttpServiceProperties);
        remoteDatawaveUserService.setSslStores(sslStores);
        remoteDatawaveUserService.setObjectMapperDecorator(objectMapperDecorator);
        remoteDatawaveUserService.setResponseObjectFactory(responseObjectFactory);
        remoteDatawaveUserService.setExecutorService(executorService);
        return remoteDatawaveUserService;
    }

    private SubjectIssuerDNPair lookupServerDN(SSLStores sslStores) throws KeyStoreException {
        if (sslStores == null) {
            throw new IllegalStateException("SSL Context not injected.");
        }

        KeyStore keystore = sslStores.getKeyStore();
        final X509Certificate cert = (X509Certificate) keystore.getCertificate(keystore.aliases().nextElement());
        final String serverDN = cert.getSubjectX500Principal().getName();
        final String serverIssuerDN = cert.getIssuerX500Principal().getName();
        return SubjectIssuerDNPair.of(serverDN, serverIssuerDN);
    }
}
