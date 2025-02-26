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
public class InternalQueryMetricsQueryConfiguration {
    
    @Bean
    @ConfigurationProperties("datawave.query.logic.logics.internal-query-metrics-query")
    public ShardQueryLogicProperties internalQueryMetricsQueryProperties() {
        return new ShardQueryLogicProperties();
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public Set<String> internalQueryMetricsQueryRequiredRoles() {
        return internalQueryMetricsQueryProperties().getRequiredRoles();
    }
}
