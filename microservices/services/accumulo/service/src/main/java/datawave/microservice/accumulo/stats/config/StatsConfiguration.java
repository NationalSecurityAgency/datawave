package datawave.microservice.accumulo.stats.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import datawave.microservice.accumulo.stats.config.StatsConfiguration.JaxbProperties;

@Configuration
@EnableConfigurationProperties({JaxbProperties.class, StatsProperties.class})
@ConditionalOnProperty(name = "accumulo.stats.enabled", havingValue = "true", matchIfMissing = true)
public class StatsConfiguration {
    
    /**
     * This will allow {@link datawave.microservice.accumulo.stats.StatsService} to inject a datawave-specific namespace URI into XML originating from the
     * Accumulo monitor's servlet response, so that it can then be de/serialized as needed for DataWave clients
     * <p>
     * Historical Note:
     * <p>
     * Previously, our legacy <strong>{@code StatsBean}</strong> EJB made direct use of the <strong>{@code StatsProperties.NAMESPACE}</strong> constant to
     * perform this function, and <strong>{@code StatsProperties}</strong> relied on Maven resource filtering to set the value of the
     * <strong>{@code NAMESPACE}</strong> constant at build time, and that's a configuration strategy that we don't want to carry forward.
     */
    @ConfigurationProperties(prefix = "datawave.webservice")
    public static class JaxbProperties {
        
        private String namespace = null;
        
        public String getNamespace() {
            return namespace;
        }
        
        public void setNamespace(String namespace) {
            this.namespace = namespace;
        }
    }
}
