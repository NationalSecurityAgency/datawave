package datawave.microservice.query.logic.config;

import datawave.query.data.UUIDType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.Scope;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.springframework.beans.factory.config.ConfigurableBeanFactory.SCOPE_PROTOTYPE;

@Configuration
@ConditionalOnProperty(name = "query.logic.factory.enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties({QueryLogicFactoryProperties.class, QueryParserProperties.class})
@ImportResource("${query.logic.factory.xmlBeansPath:classpath:QueryLogicFactory.xml}")
public class QueryLogicFactoryConfiguration {
    
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
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
    public List<UUIDType> uuidTypes(QueryParserProperties queryParserProperties) {
        List<UUIDType> uuidTypes = new ArrayList<>();
        if (queryParserProperties.getUuidTypes() != null) {
            uuidTypes.addAll(queryParserProperties.getUuidTypes());
        }
        return uuidTypes;
    }
}
