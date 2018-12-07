package datawave.microservice.cached;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceProvider;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.hazelcast.HazelcastAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Auto-configuration necessary to set up a Hazelcast client that connects to a Hazelcast cluster that has been discovered using Consul.
 */
@Configuration
@Profile("consul")
@ConditionalOnProperty(name = "hazelcast.client.enabled", matchIfMissing = true)
@ConditionalOnBean(DiscoveryServiceProvider.class)
@Conditional(HazelcastCacheCondition.class)
@AutoConfigureBefore({HazelcastAutoConfiguration.class, HazelcastDefaultClientAutoConfiguration.class})
@AutoConfigureAfter(HazelcastDiscoveryServiceAutoConfiguration.class)
@EnableConfigurationProperties(HazelcastClientProperties.class)
public class HazelcastDiscoveryClientAutoConfiguration extends HazelcastBaseClientConfiguration {
    @Bean
    public ClientConfig clientConfig(HazelcastClientProperties clientProperties, DiscoveryServiceProvider discoveryServiceProvider) {
        ClientConfig clientConfig = createClientConfig(clientProperties);
        
        if (!clientProperties.isSkipDiscoveryConfiguration()) {
            // Set up Consul discovery of cluster members.
            clientConfig.setProperty("hazelcast.discovery.enabled", Boolean.TRUE.toString());
            clientConfig.getNetworkConfig().setConnectionAttemptLimit(120);
            clientConfig.getNetworkConfig().getDiscoveryConfig().setDiscoveryServiceProvider(discoveryServiceProvider);
        }
        
        return clientConfig;
    }
    
    /**
     * Normally, just producing a Hazelcast Config object would be enough for Spring Boot to use it and create a {@link HazelcastInstance}. However, that code
     * doesn't handle a {@link ClientConfig}, so we must produce our own instance with the client configuration we produce.
     */
    @Bean
    @ConditionalOnMissingBean(HazelcastInstance.class)
    public HazelcastInstance hazelcastInstance(ClientConfig clientConfig) {
        return HazelcastClient.newHazelcastClient(clientConfig);
    }
}
