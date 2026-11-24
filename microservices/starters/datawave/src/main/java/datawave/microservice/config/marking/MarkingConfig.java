package datawave.microservice.config.marking;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import datawave.marking.ColumnVisibilitySecurityMarking;
import datawave.marking.SecurityMarking;

/**
 * Provides the default DATAWAVE {@link SecurityMarking}, which converts request parameters into the column visibility applied to data written on behalf of a
 * request.
 * <p>
 * Combining and flattening column visibilities is handled by {@code datawave.visibility.VisibilityFunctions}, which is discovered at runtime rather than
 * injected from the Spring Boot {@link org.springframework.context.ApplicationContext}.
 */
@Configuration
public class MarkingConfig {

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(name = "datawave.defaults.SecurityMarking.enabled", havingValue = "true", matchIfMissing = true)
    public SecurityMarking securityMarking() {
        return new ColumnVisibilitySecurityMarking();
    }
}
