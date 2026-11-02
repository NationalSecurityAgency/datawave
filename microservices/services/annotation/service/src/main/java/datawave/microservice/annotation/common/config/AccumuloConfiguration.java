package datawave.microservice.annotation.common.config;

import static datawave.microservice.annotation.config.AnnotationRoleConstants.ACCUMULO_WRITER_ENABLED;

import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.AnnotatedTypeMetadata;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.connection.AccumuloConnectionFactoryImpl;
import datawave.core.common.result.ConnectionPoolsProperties;
import datawave.microservice.annotation.config.AnnotationRole;

/** Accumulo connection classes used for obtaining connections to Accumulo */
@Configuration
@Conditional(AccumuloConfiguration.AccumuloRequiredCondition.class)
public class AccumuloConfiguration {
    @Bean
    @ConfigurationProperties("datawave.connection.factory")
    public ConnectionPoolsProperties connectionPoolsProperties() {
        return new ConnectionPoolsProperties();
    }

    @Bean
    @ConditionalOnMissingBean
    public AccumuloConnectionFactory accumuloConnectionFactory(ConnectionPoolsProperties connectionPoolsProperties) {
        // no need for metadata cache here.
        return AccumuloConnectionFactoryImpl.getInstance(null, connectionPoolsProperties);
    }

    public static class AccumuloRequiredCondition extends SpringBootCondition {
        @Override
        public ConditionOutcome getMatchOutcome(ConditionContext context, AnnotatedTypeMetadata metadata) {
            boolean required = AnnotationRole.from(context.getEnvironment()).includesApi()
                            || context.getEnvironment().getProperty(ACCUMULO_WRITER_ENABLED, Boolean.class, false);
            return new ConditionOutcome(required, "Accumulo is required for API reads or an enabled Accumulo writer");
        }
    }
}
