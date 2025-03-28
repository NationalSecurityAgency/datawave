package datawave.microservice.query.edge.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.web.reactive.function.client.WebClient;

import datawave.core.common.edgedictionary.EdgeDictionaryProvider;
import datawave.edge.model.EdgeModelFieldsFactory;
import datawave.microservice.query.edge.DefaultEdgeDictionaryProvider;
import datawave.microservice.query.edge.EdgeModelFieldsFactoryImpl;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.query.model.edge.EdgeQueryModel;
import datawave.query.model.util.LoadModel;
import datawave.security.authorization.JWTTokenHandler;
import datawave.webservice.dictionary.edge.DefaultEdgeDictionary;

@Configuration
@ConditionalOnProperty(name = "datawave.query.logic.factory.enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties({EdgeDictionaryProviderProperties.class, EdgeModelProperties.class})
@ImportResource(locations = {"${datawave.query.edge.xmlBeansPath:classpath:EdgeQueryLogicFactory.xml}"})
public class EdgeQueryLogicFactoryConfiguration {
    
    @Bean
    public EdgeModelFieldsFactory edgeModelFieldsFactory(EdgeModelProperties edgeModelProperties) {
        return new EdgeModelFieldsFactoryImpl(edgeModelProperties);
    }
    
    @Bean
    public EdgeQueryModel edgeQueryModel(EdgeModelProperties edgeModelProperties, EdgeModelFieldsFactory edgeModelFieldsFactory) throws Exception {
        return new EdgeQueryModel(LoadModel.loadModelFromFieldMappings(edgeModelProperties.getFieldMappings()), edgeModelFieldsFactory.createFields());
    }
    
    @Bean
    public EdgeDictionaryProvider edgeDictionaryProvider(EdgeDictionaryProviderProperties edgeDictionaryProperties,
                    @Autowired(required = false) QueryStorageCache queryStorageCache, @Autowired(required = false) WebClient.Builder webClientBuilder,
                    @Autowired(required = false) JWTTokenHandler jwtTokenHandler) {
        if (queryStorageCache != null && webClientBuilder != null) {
            return new DefaultEdgeDictionaryProvider(edgeDictionaryProperties, queryStorageCache, webClientBuilder, jwtTokenHandler);
        } else {
            return (settings, metadataTableName) -> new DefaultEdgeDictionary();
        }
    }
}
