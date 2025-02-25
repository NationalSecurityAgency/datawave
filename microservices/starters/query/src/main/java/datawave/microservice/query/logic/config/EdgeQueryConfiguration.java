package datawave.microservice.query.logic.config;

import static org.springframework.beans.factory.config.ConfigurableBeanFactory.SCOPE_PROTOTYPE;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import datawave.query.language.parser.QueryParser;

@Configuration
@ConditionalOnProperty(name = "datawave.query.logic.factory.enabled", havingValue = "true", matchIfMissing = true)
public class EdgeQueryConfiguration {
    
    @Autowired
    private ApplicationContext appContext;
    
    @Bean
    @ConfigurationProperties("datawave.query.logic.logics.edge-query")
    public EdgeQueryLogicProperties edgeQueryProperties() {
        return new EdgeQueryLogicProperties();
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public Map<String,QueryParser> edgeQuerySyntaxParsers() {
        Map<String,QueryParser> querySyntaxParsers = new HashMap<>();
        edgeQueryProperties().getQuerySyntaxParsers().forEach((key, value) -> {
            if (!key.isEmpty()) {
                querySyntaxParsers.put(key, (!value.isEmpty()) ? appContext.getBean(value, QueryParser.class) : null);
            }
        });
        return querySyntaxParsers;
    }
}
