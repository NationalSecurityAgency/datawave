package datawave.webservice.config;

import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUserService;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.authorization.remote.RemoteDatawaveUserService;
import datawave.security.cert.SSLStores;
import datawave.webservice.common.json.ObjectMapperDecorator;
import datawave.webservice.common.remote.RemoteHttpServiceProperties;
import datawave.webservice.query.result.event.ResponseObjectFactory;

@Configuration
@EnableConfigurationProperties(TestDatawaveUserServiceProperties.class)
public class DatawaveUserServiceConfiguration {

    @Bean
    public Map<SubjectIssuerDNPair,DatawaveUser> testDatawaveUsers(TestDatawaveUserServiceProperties testDatawaveUserServiceProperties) {
        return testDatawaveUserServiceProperties.getTestUsers().stream().collect(
                        java.util.stream.Collectors.toMap(TestDatawaveUserProperties::toSubjectIssuerDNPair, TestDatawaveUserProperties::toDatawaveUser));

    }

    @Bean
    @ConfigurationProperties("datawave.remote.user-service")
    @Qualifier("remoteDatawaveUserServiceProperties")
    public RemoteHttpServiceProperties remoteDatawaveUserServiceProperties() {
        return new RemoteHttpServiceProperties();
    }

    @Bean
    @Qualifier("remoteDatawaveUserExecutorService")
    public ExecutorService remoteDatawaveUserExecutorService(Environment environment) {
        return RemoteServicesConfiguration.createExecutorService(environment, RemoteDatawaveUserService.PROP_PREFIX + ".executor");
    }

    @Bean
    @Qualifier("remoteDatawaveUserService")
    public DatawaveUserService remoteDatawaveUserService(
                    @Qualifier("remoteDatawaveUserServiceProperties") RemoteHttpServiceProperties remoteHttpServiceProperties, SSLStores sslStores,
                    ObjectMapperDecorator objectMapperDecorator, ResponseObjectFactory responseObjectFactory,
                    ExecutorService remoteDatawaveUserExecutorService) {
        RemoteDatawaveUserService remoteDatawaveUserService = new RemoteDatawaveUserService();
        remoteDatawaveUserService.setConfig(remoteHttpServiceProperties);
        remoteDatawaveUserService.setSslStores(sslStores);
        remoteDatawaveUserService.setObjectMapperDecorator(objectMapperDecorator);
        remoteDatawaveUserService.setResponseObjectFactory(responseObjectFactory);
        remoteDatawaveUserService.setExecutorService(remoteDatawaveUserExecutorService);
        return remoteDatawaveUserService;
    }
}
