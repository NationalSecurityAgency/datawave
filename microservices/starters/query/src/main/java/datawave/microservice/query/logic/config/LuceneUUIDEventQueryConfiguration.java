package datawave.microservice.query.logic.config;

import static org.springframework.beans.factory.config.ConfigurableBeanFactory.SCOPE_PROTOTYPE;

import java.util.Set;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
@ConditionalOnProperty(name = "datawave.query.logic.factory.enabled", havingValue = "true", matchIfMissing = true)
public class LuceneUUIDEventQueryConfiguration {
    
    @Bean
    @ConfigurationProperties(prefix = "datawave.query.logic.logics.lucene-u-u-i-d-event-query.event-query")
    public ShardQueryLogicProperties luceneUUIDEventQueryEventQueryProperties() {
        return new ShardQueryLogicProperties();
    }
    
    @Bean
    @ConfigurationProperties(prefix = "datawave.query.logic.logics.lucene-u-u-i-d-event-query.error-event-query")
    public ShardQueryLogicProperties luceneUUIDEventQueryErrorEventQueryProperties() {
        return new ShardQueryLogicProperties();
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public Set<String> luceneUUIDEventQueryEventQueryMandatoryQuerySyntax() {
        return luceneUUIDEventQueryEventQueryProperties().getMandatoryQuerySyntax();
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public Set<String> luceneUUIDEventQueryErrorEventQueryMandatoryQuerySyntax() {
        return luceneUUIDEventQueryEventQueryProperties().getMandatoryQuerySyntax();
    }
}
