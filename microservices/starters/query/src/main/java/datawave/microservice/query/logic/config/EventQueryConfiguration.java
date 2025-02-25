package datawave.microservice.query.logic.config;

import static org.springframework.beans.factory.config.ConfigurableBeanFactory.SCOPE_PROTOTYPE;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import datawave.query.cardinality.CardinalityConfiguration;

@Configuration
@ConditionalOnProperty(name = "datawave.query.logic.factory.enabled", havingValue = "true", matchIfMissing = true)
public class EventQueryConfiguration {
    
    @Bean
    @ConfigurationProperties(prefix = "datawave.query.logic.logics.event-query")
    public ShardQueryLogicProperties eventQueryProperties() {
        return new ShardQueryLogicProperties();
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public CardinalityConfiguration eventQueryCardinalityConfiguration() {
        return eventQueryProperties().getCardinalityConfiguration();
    }
}
