package datawave.webservice.config;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import datawave.core.query.result.event.DefaultResponseObjectFactory;
import datawave.security.cert.SSLStores;
import datawave.security.cert.SSLStoresImpl;
import datawave.webservice.common.json.DefaultMapperDecorator;
import datawave.webservice.common.json.ObjectMapperDecorator;
import datawave.webservice.query.result.event.ResponseObjectFactory;

@Configuration
@EnableConfigurationProperties(SSLStoreProperties.class)
public class RemoteServicesConfiguration {

    public static ExecutorService createExecutorService(Environment environment) {
        String corePoolSize = environment.getProperty("datawave.remote.executor.corePoolSize", String.class, "10");
        String maxPoolSize = environment.getProperty("datawave.remote.executor.maxPoolSize", String.class, "100");
        String keepAliveTime = environment.getProperty("datawave.remote.executor.keepAliveTime", String.class, "60");
        String timeUnit = environment.getProperty("datawave.remote.executor.timeUnit", String.class, "SECONDS");
        String queueSize = environment.getProperty("datawave.remote.executor.queueSize", String.class, "100");
        return new ThreadPoolExecutor(Integer.parseInt(corePoolSize), Integer.parseInt(maxPoolSize), Long.parseLong(keepAliveTime), TimeUnit.valueOf(timeUnit),
                        new ArrayBlockingQueue<>(Integer.parseInt(queueSize)));
    }

    @Bean
    @ConditionalOnMissingBean
    public ExecutorService executorService(Environment env) {
        return createExecutorService(env);
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
    @ConditionalOnMissingBean
    public SSLStores sslStores(SSLStoreProperties sslStoreProperties) throws Exception {
        return createSSLStores(sslStoreProperties);
    }

    public static ResponseObjectFactory createResponseObjectFactory() {
        return new DefaultResponseObjectFactory();
    }

    @Bean
    @ConditionalOnMissingBean
    public ResponseObjectFactory responseObjectFactory() {
        return createResponseObjectFactory();
    }
}
