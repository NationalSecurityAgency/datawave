package datawave.microservice.query.logic.config;

import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.language.parser.QueryParser;
import datawave.query.transformer.EventQueryDataDecorator;
import datawave.query.transformer.EventQueryDataDecoratorTransformer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.springframework.beans.factory.config.ConfigurableBeanFactory.SCOPE_PROTOTYPE;

@Configuration
@ConditionalOnProperty(name = "datawave.query.logic.factory.enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties({QueryLogicFactoryProperties.class})
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
        Map<String,String> hierarchyFieldOptions = new HashMap<>();
        for (Map.Entry<String,String> entry : baseEventQueryProperties().getHierarchyFieldOptions().entrySet()) {
            if (!entry.getKey().isEmpty()) {
                hierarchyFieldOptions.put(entry.getKey(), entry.getValue());
            }
        }
        return hierarchyFieldOptions;
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public List<String> baseEventQueryContentFieldNames() {
        List<String> contentFieldNames = new ArrayList<>();
        if (baseEventQueryProperties().getContentFieldNames() != null) {
            contentFieldNames.addAll(baseEventQueryProperties().getContentFieldNames());
        }
        return contentFieldNames;
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public List<String> baseEventQueryRealmSuffixExclusionPatterns() {
        List<String> realmSuffixExclusionPatterns = new ArrayList<>();
        if (baseEventQueryProperties().getRealmSuffixExclusionPatterns() != null) {
            realmSuffixExclusionPatterns.addAll(baseEventQueryProperties().getRealmSuffixExclusionPatterns());
        }
        return realmSuffixExclusionPatterns;
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public List<String> baseEventQueryEnricherClassNames() {
        List<String> enricherClassNames = new ArrayList<>();
        if (baseEventQueryProperties().getEnricherClassNames() != null) {
            enricherClassNames.addAll(baseEventQueryProperties().getEnricherClassNames());
        }
        return enricherClassNames;
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public List<String> baseEventQueryFilterClassNames() {
        List<String> filterClassNames = new ArrayList<>();
        if (baseEventQueryProperties().getFilterClassNames() != null) {
            filterClassNames.addAll(baseEventQueryProperties().getFilterClassNames());
        }
        return filterClassNames;
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public Map<String,String> baseEventQueryFilterOptions() {
        Map<String,String> filterOptions = new HashMap<>();
        for (Map.Entry<String,String> entry : baseEventQueryProperties().getFilterOptions().entrySet()) {
            if (!entry.getKey().isEmpty()) {
                filterOptions.put(entry.getKey(), entry.getValue());
            }
        }
        return filterOptions;
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public List<IvaratorCacheDirConfig> baseEventQueryIvaratorCacheDirConfigs() {
        List<IvaratorCacheDirConfig> ivaratorCacheDirConfigs = new ArrayList<>();
        if (baseEventQueryProperties().getIvaratorCacheDirConfigs() != null) {
            ivaratorCacheDirConfigs = baseEventQueryProperties().getIvaratorCacheDirConfigs();
        }
        return ivaratorCacheDirConfigs;
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
                for (Map.Entry<String,Map<String,String>> entry : config.getDataDecorators().entrySet()) {
                    if (entry.getValue() != null) {
                        EventQueryDataDecorator eventQueryDataDecorator = new EventQueryDataDecorator();
                        eventQueryDataDecorator.setFieldName(entry.getKey());
                        eventQueryDataDecorator.setPatternMap(new LinkedHashMap<>(entry.getValue()));
                        dataDecorators.put(entry.getKey(), eventQueryDataDecorator);
                    }
                }
            }
            eventQueryDataDecoratorTransformer.setDataDecorators(dataDecorators);
        }
        return eventQueryDataDecoratorTransformer;
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public Map<String,QueryParser> baseEventQuerySyntaxParsers() {
        Map<String,QueryParser> querySyntaxParsers = new HashMap<>();
        for (Map.Entry<String,String> entry : baseEventQueryProperties().getQuerySyntaxParsers().entrySet()) {
            if (!entry.getKey().isEmpty()) {
                querySyntaxParsers.put(entry.getKey(), (!entry.getValue().isEmpty()) ? appContext.getBean(entry.getValue(), QueryParser.class) : null);
            }
        }
        return querySyntaxParsers;
    }
}
