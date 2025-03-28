package datawave.microservice.cached;

import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.cloud.client.CommonsClientAutoConfiguration;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.hazelcast.spi.discovery.integration.DiscoveryService;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceProvider;

/**
 * Auto-configuration for the Hazelcast {@link DiscoveryService} and {@link DiscoveryServiceProvider} classes that we use to create or join a Hazelcast cluster.
 * If the {@link DiscoveryClient} is not available, then this auto-configuration will not fire. Note that if the discovery client is available, but the cache
 * type in use is not Hazelcast, we will still create these beans.
 */
@Configuration
@ConditionalOnBean(DiscoveryClient.class)
@ConditionalOnClass({DiscoveryClient.class, DiscoveryService.class, DiscoveryServiceProvider.class})
@AutoConfigureAfter(CommonsClientAutoConfiguration.class)
public class HazelcastDiscoveryServiceAutoConfiguration {
    @Bean
    public HazelcastDiscoveryService hazelcastDiscoveryService(DiscoveryClient discoveryClient) {
        return new HazelcastDiscoveryService(discoveryClient);
    }
    
    @Bean
    public DiscoveryServiceProvider hazelcastDiscoveryServiceProvider(HazelcastDiscoveryService hazelcastDiscoveryService) {
        return discoveryServiceSettings -> hazelcastDiscoveryService;
    }
}
