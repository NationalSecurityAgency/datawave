package datawave.microservice.query.logic.config;

import static org.springframework.beans.factory.config.ConfigurableBeanFactory.SCOPE_PROTOTYPE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import datawave.query.config.IndexHole;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.language.parser.QueryParser;
import datawave.query.tables.QueryMacroFunction;
import datawave.query.transformer.EventQueryDataDecorator;
import datawave.query.transformer.EventQueryDataDecoratorTransformer;

@Configuration
@ConditionalOnProperty(name = "datawave.query.logic.factory.enabled", havingValue = "true", matchIfMissing = true)
public class BaseEventQueryConfiguration {
    
    @Autowired
    private ApplicationContext appContext;
    
    @Bean
    @ConfigurationProperties("datawave.query.logic.logics.base-event-query")
    public ShardQueryLogicProperties baseEventQueryProperties() {
        return new ShardQueryLogicProperties();
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public Map<String,String> baseEventQueryHierarchyFieldOptions() {
        return baseEventQueryProperties().getHierarchyFieldOptions();
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public List<String> baseEventQueryContentFieldNames() {
        return baseEventQueryProperties().getContentFieldNames();
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public List<String> baseEventQueryRealmSuffixExclusionPatterns() {
        return baseEventQueryProperties().getRealmSuffixExclusionPatterns();
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public List<String> baseEventQueryEnricherClassNames() {
        return baseEventQueryProperties().getEnricherClassNames();
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public List<String> baseEventQueryFilterClassNames() {
        return baseEventQueryProperties().getFilterClassNames();
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public Map<String,String> baseEventQueryFilterOptions() {
        return baseEventQueryProperties().getFilterOptions();
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public List<IvaratorCacheDirConfig> baseEventQueryIvaratorCacheDirConfigs() {
        return baseEventQueryProperties().getIvaratorCacheDirConfigs();
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public EventQueryDataDecoratorTransformer baseEventQueryEventQueryDataDecoratorTransformer() {
        EventQueryDataDecoratorTransformer eventQueryDataDecoratorTransformer = new EventQueryDataDecoratorTransformer();
        if (baseEventQueryProperties().getDataDecoratorTransformerConfig() != null) {
            ShardQueryLogicProperties.DataDecoratorTransformerConfig config = baseEventQueryProperties().getDataDecoratorTransformerConfig();
            
            List<String> requestedDecorators = new ArrayList<>();
            if (config.getRequestedDecorators() != null) {
                requestedDecorators.addAll(config.getRequestedDecorators());
            }
            eventQueryDataDecoratorTransformer.setRequestedDecorators(requestedDecorators);
            
            Map<String,EventQueryDataDecorator> dataDecorators = new LinkedHashMap<>();
            if (config.getDataDecorators() != null) {
                config.getDataDecorators().forEach((key, value) -> {
                    if (value != null) {
                        EventQueryDataDecorator eventQueryDataDecorator = new EventQueryDataDecorator();
                        eventQueryDataDecorator.setFieldName(key);
                        eventQueryDataDecorator.setPatternMap(new LinkedHashMap<>(value));
                        dataDecorators.put(key, eventQueryDataDecorator);
                    }
                });
            }
            eventQueryDataDecoratorTransformer.setDataDecorators(dataDecorators);
        }
        return eventQueryDataDecoratorTransformer;
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public Map<String,QueryParser> baseEventQuerySyntaxParsers() {
        Map<String,QueryParser> querySyntaxParsers = new HashMap<>();
        baseEventQueryProperties().getQuerySyntaxParsers().forEach((key, value) -> {
            if (!key.isEmpty()) {
                querySyntaxParsers.put(key, (!value.isEmpty()) ? appContext.getBean(value, QueryParser.class) : null);
            }
        });
        return querySyntaxParsers;
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public Set<String> baseEventQueryRequiredRoles() {
        return baseEventQueryProperties().getRequiredRoles();
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public List<String> baseEventQueryDocumentPermutations() {
        return baseEventQueryProperties().getDocumentPermutations();
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public QueryMacroFunction baseEventQueryQueryMacroFunction() {
        QueryMacroFunction queryMacroFunction = new QueryMacroFunction();
        queryMacroFunction.setQueryMacros(baseEventQueryProperties().getQueryMacroFunction());
        return queryMacroFunction;
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public List<IndexHole> baseEventQueryIndexHoles() {
        return baseEventQueryProperties().getIndexHoles();
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public Set<String> baseEventQueryWhindexMappingFields() {
        return baseEventQueryProperties().getWhindexMappingFields();
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public Map<String,Map<String,String>> baseEventQueryWhindexFieldMappings() {
        return baseEventQueryProperties().getWhindexFieldMappings();
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public Map<String,Long> baseEventQueryDnResultLimits() {
        return baseEventQueryProperties().getDnResultLimits();
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public Set<String> baseEventQueryDisallowlistedFields() {
        return baseEventQueryProperties().getDisallowlistedFields();
    }
}
