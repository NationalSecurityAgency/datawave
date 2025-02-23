package datawave.microservice.query.logic.config;

import static org.springframework.beans.factory.config.ConfigurableBeanFactory.SCOPE_PROTOTYPE;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.language.parser.QueryParser;

@Configuration
@ConditionalOnProperty(name = "datawave.query.logic.factory.enabled", havingValue = "true", matchIfMissing = true)
public class HitHighlightsConfiguration {
    
    @Autowired
    private ApplicationContext appContext;
    
    @Bean
    @ConfigurationProperties("datawave.query.logic.logics.hit-highlights")
    public ShardQueryLogicProperties hitHighlightsProperties() {
        return new ShardQueryLogicProperties();
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public List<IvaratorCacheDirConfig> hitHighlightsIvaratorCacheDirConfigs() {
        return hitHighlightsProperties().getIvaratorCacheDirConfigs();
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public Map<String,QueryParser> hitHighlightsSyntaxParsers() {
        Map<String,QueryParser> querySyntaxParsers = new HashMap<>();
        hitHighlightsProperties().getQuerySyntaxParsers().forEach((key, value) -> {
            if (!key.isEmpty()) {
                querySyntaxParsers.put(key, (!value.isEmpty()) ? appContext.getBean(value, QueryParser.class) : null);
            }
        });
        return querySyntaxParsers;
    }
}
