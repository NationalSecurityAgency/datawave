package datawave.microservice.query.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;

import java.util.List;

@Configuration
@ConditionalOnProperty(name = "query.logic.factory.enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(QueryLogicFactoryProperties.class)
@ImportResource("${query.logic.factory.xmlBeansPath:classpath:QueryLogicFactory.xml}")
public class QueryLogicFactoryConfiguration {
    
    @Bean
    @Value("#{'${query.logic.logics.BaseEventQuery.contentFieldNames}'.split(',')}")
    public List<String> baseEventQueryContentFieldNames(List<String> contentFieldNames) {
        return contentFieldNames;
    }
    
    // @Bean
    // @Value("#{'${query.logic.logics.BaseEventQuery.realmSuffixExclusionPatterns}'.split(',')}")
    // public List<String> realmSuffixExclusionPatterns(List<String> realmSuffixExclusionPatterns) {
    // return new ArrayList<>();
    // }
    //
    // @Bean
    // @Value("#{'${query.logic.logics.BaseEventQuery.enricherClassNames}'.split(',')}")
    // public List<String> enricherClassNames(List<String> enricherClassNames) {
    // return new ArrayList<>();
    // }
    //
    // @Bean
    // @Value("#{'${query.logic.logics.BaseEventQuery.filterClassNames}'.split(',')}")
    // public List<String> filterClassNames(List<String> filterClassNames) {
    // return new ArrayList<>();
    // }
    //
    // @Bean
    // @Value("#{'${query.logic.logics.BaseEventQuery.querySyntaxParsers}'.split(',')}")
    // public Map<String, QueryParser> querySyntaxParsers() {
    // return new HashMap<>();
    // }
    //
    // @Bean
    // @Value("#{'${query.logic.logics.BaseEventQuery.baseEventQueryHierarchyFieldOptions}'.split(',')}")
    // public Map<String, String> baseEventQueryHierarchyFieldOptions() {
    // return new HashMap<>();
    // }
    //
    // @Bean
    // @Value("#{'${query.logic.logics.BaseEventQuery.baseEventQueryFilterOptions}'.split(',')}")
    // public Map<String, String> baseEventQueryFilterOptions() {
    // return new HashMap<>();
    // }
    //
    // @Bean
    // @Value("#{'${query.logic.logics.BaseEventQuery.enricherClassNames}'.split(',')}")
    // public List<IvaratorCacheDirConfig> ivaratorCacheDirConfigs() {
    // return new ArrayList<>();
    // }
    //
    // @Bean
    // @Value("#{'${query.logic.logics.BaseEventQuery.eventQueryDataDecorators}'.split(',')}")
    // public Map<String, EventQueryDataDecorator> eventQueryDataDecorators() {
    // return new HashMap<>();
    // }
    
}
