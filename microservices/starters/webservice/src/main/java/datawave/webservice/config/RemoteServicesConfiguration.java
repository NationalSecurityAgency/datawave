package datawave.webservice.config;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import datawave.core.query.result.event.DefaultResponseObjectFactory;
import datawave.security.cert.SSLStores;
import datawave.security.cert.SSLStoresImpl;
import datawave.webservice.DatawaveResourceFactory;
import datawave.webservice.common.json.DefaultMapperDecorator;
import datawave.webservice.common.json.ObjectMapperDecorator;
import datawave.webservice.common.remote.RemoteHttpServiceProperties;
import datawave.webservice.query.result.event.ResponseObjectFactory;

@Configuration
@EnableConfigurationProperties(SSLStoreProperties.class)
public class RemoteServicesConfiguration {

    public static ExecutorService createExecutorService(Environment environment, String prefix) {

        ThreadPoolExecutorProperties props = Binder.get(environment).bind(prefix, Bindable.of(ThreadPoolExecutorProperties.class))
                        .orElse(new ThreadPoolExecutorProperties());
        return new ThreadPoolExecutor(props.getCorePoolSize(), props.getMaxPoolSize(), props.getKeepAliveSeconds(), TimeUnit.SECONDS,
                        new ArrayBlockingQueue<>(props.getQueueSize()));
    }

    public static ObjectMapperDecorator createObjectMapperDecorator() {
        return new DefaultMapperDecorator();
    }

    @Bean
    @ConditionalOnMissingBean
    public ObjectMapperDecorator objectMapperDecorator() {
        return createObjectMapperDecorator();
    }

    public static SSLStores createSSLStores(SSLStoreProperties sslStoreProperties) throws Exception {
        String keyStoreUrl = sslStoreProperties.getKeyStore();
        String keyStorePassword = sslStoreProperties.getKeyStorePassword();
        String keyStoreType = sslStoreProperties.getKeyStoreType();
        String trustStoreUrl = sslStoreProperties.getTrustStore();
        String trustStorePassword = sslStoreProperties.getTrustStorePassword();
        String trustStoreType = sslStoreProperties.getTrustStoreType();

        // @formatter:off
        return SSLStoresImpl.builder()
                .withKeystore(keyStoreUrl, keyStorePassword, keyStoreType)
                .withTruststore(trustStoreUrl, trustStorePassword, trustStoreType)
                .build();
        // @formatter:on
    }

    public static SSLStores createSSLStores(Environment environment) throws Exception {
        SSLStoreProperties sslStoreProperties = Binder.get(environment).bind("server.ssl", Bindable.of(SSLStoreProperties.class))
                        .orElseThrow(() -> new IllegalStateException("Failed to bind SSLStoreProperties"));
        return createSSLStores(sslStoreProperties);
    }

    @Bean
    public SSLStores sslStores(SSLStoreProperties sslStoreProperties) throws Exception {
        return createSSLStores(sslStoreProperties);
    }

    @Bean
    public DefaultResponseObjectFactory defaultResponseObjectFactory() {
        return new DefaultResponseObjectFactory();
    }

    @Bean
    public DatawaveResourceFactory datawaveResourceFactory() {
        return new DatawaveResourceFactory();
    }

    @Bean
    public ResponseObjectFactory responseObjectFactory(DatawaveResourceFactory datawaveResourceFactory) {
        return datawaveResourceFactory.responseObjectFactory();
    }

    @Bean
    public ExecutorService remoteDictionaryServiceExecutorService(Environment environment) {
        return RemoteServicesConfiguration.createExecutorService(environment, "datawave.remote.dictionary-service.executor");
    }

    @Bean
    @ConfigurationProperties("datawave.remote.dictionary-service")
    public RemoteHttpServiceProperties remoteDictionaryServiceProperties() {
        return new RemoteHttpServiceProperties();
    }

    @Bean
    public ExecutorService remoteQueryMetricServiceExecutorService(Environment environment) {
        return RemoteServicesConfiguration.createExecutorService(environment, "datawave.remote.query-metric.executor");
    }

    @Bean
    @ConfigurationProperties("datawave.remote.query-metric")
    public RemoteHttpServiceProperties remoteQueryMetricServiceProperties() {
        return new RemoteHttpServiceProperties();
    }

    @Bean
    public ExecutorService remoteAccumuloExecutorService(Environment environment) {
        return RemoteServicesConfiguration.createExecutorService(environment, "datawave.remote.accumulo.executor");
    }

    @Bean
    @ConfigurationProperties("datawave.remote.accumulo")
    public RemoteHttpServiceProperties remoteAccumuloProperties() {
        return new RemoteHttpServiceProperties();
    }

    @Bean
    @ConfigurationProperties("datawave.remote.auditor")
    public RemoteHttpServiceProperties remoteAuditorProperties() {
        return new RemoteHttpServiceProperties();
    }

    @Bean
    public ExecutorService remoteAuditorExecutorService(Environment environment) {
        return RemoteServicesConfiguration.createExecutorService(environment, "datawave.remote.auditor.executor");
    }
}
