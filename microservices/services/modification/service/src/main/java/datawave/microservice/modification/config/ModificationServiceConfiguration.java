package datawave.microservice.modification.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.web.reactive.function.client.WebClient;

import datawave.core.common.cache.AccumuloTableCache;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.connection.AccumuloConnectionFactoryImpl;
import datawave.core.common.result.ConnectionPoolsProperties;
import datawave.microservice.modification.query.RemoteQueryService;
import datawave.modification.ModificationService;
import datawave.modification.cache.ModificationCache;
import datawave.modification.configuration.ModificationConfiguration;
import datawave.modification.query.ModificationQueryService;
import datawave.security.authorization.JWTTokenHandler;

@Configuration
@EnableConfigurationProperties({ModificationQueryProperties.class, ModificationDataProperties.class, ModificationHandlerProperties.class})
@ImportResource(locations = {"${datawave.modification.data.xmlBeansPath:classpath:ModificationServices.xml}"})
public class ModificationServiceConfiguration {
    
    @Bean
    @ConditionalOnMissingBean
    public ModificationService modificationServiceService(ModificationConfiguration modificationConfiguration, AccumuloConnectionFactory connectionFactory,
                    ModificationCache modificationCache, ModificationQueryService.ModificationQueryServiceFactory queryServiceFactory) {
        return new ModificationService(modificationConfiguration, modificationCache, connectionFactory, queryServiceFactory);
    }
    
    @Bean
    @ConditionalOnMissingBean
    public ModificationQueryService.ModificationQueryServiceFactory modificationQueryServiceFactory(ModificationQueryProperties modificationProperties,
                    WebClient.Builder webClientBuilder, JWTTokenHandler jwtTokenHandler) {
        return new RemoteQueryService.RemoteQueryServiceFactory(modificationProperties, webClientBuilder, jwtTokenHandler);
    }
    
    @Bean
    @ConditionalOnMissingBean(ConnectionPoolsProperties.class)
    @ConfigurationProperties("datawave.connection.factory")
    public ConnectionPoolsProperties poolProperties() {
        return new ConnectionPoolsProperties();
    }
    
    @Bean
    @ConditionalOnMissingBean(name = "accumuloConnectionFactory")
    public AccumuloConnectionFactory connectionFactory(AccumuloTableCache cache, ConnectionPoolsProperties config) {
        return AccumuloConnectionFactoryImpl.getInstance(cache, config);
    }
    
}
