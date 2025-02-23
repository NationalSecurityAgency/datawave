package datawave.microservice.query.logic.config;

import static org.springframework.beans.factory.config.ConfigurableBeanFactory.SCOPE_PROTOTYPE;

import java.util.List;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
@ConditionalOnProperty(name = "datawave.query.logic.factory.enabled", havingValue = "true", matchIfMissing = true)
public class TLDEventQueryConfiguration {
    
    @Bean
    @ConfigurationProperties("datawave.query.logic.logics.t-l-d-event-query")
    public ShardQueryLogicProperties tldEventQueryProperties() {
        return new ShardQueryLogicProperties();
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public List<String> tldEventQueryIndexFilteringClassNames() {
        return tldEventQueryProperties().getIndexFilteringClassNames();
    }
}
