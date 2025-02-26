package datawave.microservice.query.logic.config;

import static org.springframework.beans.factory.config.ConfigurableBeanFactory.SCOPE_PROTOTYPE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.Scope;

import datawave.core.query.result.event.DefaultResponseObjectFactory;
import datawave.microservice.query.edge.config.EdgeModelProperties;
import datawave.microservice.query.lookup.LookupProperties;
import datawave.microservice.query.translateid.TranslateIdProperties;
import datawave.query.data.UUIDType;
import datawave.webservice.query.result.event.ResponseObjectFactory;

@Configuration
@ConditionalOnProperty(name = "datawave.query.logic.factory.enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties({QueryLogicFactoryProperties.class, QueryParserProperties.class, LookupProperties.class, TranslateIdProperties.class,
        EdgeModelProperties.class})
@ImportResource(locations = {"${datawave.query.logic.factory.xmlBeansPath:classpath:QueryLogicFactory.xml}"})
public class QueryLogicFactoryConfiguration {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    @Bean
    @ConditionalOnProperty(name = "datawave.defaults.ResponseObjectFactory.enabled", havingValue = "true", matchIfMissing = true)
    public ResponseObjectFactory responseObjectFactory() {
        return new DefaultResponseObjectFactory();
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public Set<String> tokenizedFields(QueryParserProperties queryParserProperties) {
        Set<String> tokenizedFields = new HashSet<>();
        if (queryParserProperties.getTokenizedFields() != null) {
            tokenizedFields.addAll(queryParserProperties.getTokenizedFields());
        }
        return tokenizedFields;
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public Set<String> skipTokenizeUnfieldedFields(QueryParserProperties queryParserProperties) {
        Set<String> skipTokenizeUnfieldedFields = new HashSet<>();
        if (queryParserProperties.getSkipTokenizeUnfieldedFields() != null) {
            skipTokenizeUnfieldedFields.addAll(queryParserProperties.getTokenizedFields());
        }
        return skipTokenizeUnfieldedFields;
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public Set<String> tokenizerStopwords(QueryParserProperties queryParserProperties) {
        Set<String> tokenizerStopwords = new HashSet<>();
        if (queryParserProperties.getTokenizerStopwords() != null) {
            tokenizerStopwords.addAll(queryParserProperties.getTokenizerStopwords());
        }
        return tokenizerStopwords;
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public List<UUIDType> uuidTypes(LookupProperties lookupProperties) {
        List<UUIDType> uuidTypes = new ArrayList<>();
        if (lookupProperties.getTypes() != null) {
            uuidTypes.addAll(lookupProperties.getTypes().values());
        }
        return uuidTypes;
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public Map<String,String> edgeModelBaseFieldMap(EdgeModelProperties edgeProperties) {
        Map<String,String> fieldMap = new HashMap<>();
        if (edgeProperties.getBaseFieldMap() != null) {
            fieldMap.putAll(edgeProperties.getBaseFieldMap());
        }
        return fieldMap;
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public Map<String,String> edgeModelKeyUtilFieldMap(EdgeModelProperties edgeProperties) {
        Map<String,String> fieldMap = new HashMap<>();
        if (edgeProperties.getKeyUtilFieldMap() != null) {
            fieldMap.putAll(edgeProperties.getKeyUtilFieldMap());
        }
        return fieldMap;
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public Map<String,String> edgeModelTransformFieldMap(EdgeModelProperties edgeProperties) {
        Map<String,String> fieldMap = new HashMap<>();
        if (edgeProperties.getTransformFieldMap() != null) {
            fieldMap.putAll(edgeProperties.getTransformFieldMap());
        }
        return fieldMap;
    }
    
}
