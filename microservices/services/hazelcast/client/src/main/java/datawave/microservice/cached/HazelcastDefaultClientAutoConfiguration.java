package datawave.microservice.cached;

import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.hazelcast.HazelcastAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;

/**
 * Auto-configuration necessary to set up a Hazelcast client that connects to a Hazelcast cluster that has been configured using completely custom (or no)
 * configuration.
 */
@Configuration
@ConditionalOnProperty(name = "hazelcast.client.enabled", matchIfMissing = true)
@ConditionalOnMissingBean(ClientConfig.class)
@Conditional(HazelcastCacheCondition.class)
@AutoConfigureBefore(HazelcastAutoConfiguration.class)
@AutoConfigureAfter({HazelcastDiscoveryClientAutoConfiguration.class, HazelcastK8sClientAutoConfiguration.class,
        HazelcastDiscoveryServiceAutoConfiguration.class})
@EnableConfigurationProperties(HazelcastClientProperties.class)
public class HazelcastDefaultClientAutoConfiguration extends HazelcastBaseClientConfiguration {
    @Bean
    public ClientConfig clientConfig(HazelcastClientProperties clientProperties) {
        return createClientConfig(clientProperties);
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
