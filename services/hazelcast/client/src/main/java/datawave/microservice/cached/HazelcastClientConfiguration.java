package datawave.microservice.cached;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;
import java.util.Map;

@Configuration
@ConditionalOnBean(DiscoveryServiceProvider.class)
public class HazelcastClientConfiguration {
    @Value("${hazelcast.clusterName:cache}")
    private String hazelcastCluster;
    
    @Bean
    public ClientConfig clientConfig(DiscoveryServiceProvider discoveryServiceProvider) {
        ClientConfig clientConfig = new ClientConfig();
        
        clientConfig.getGroupConfig().setName(hazelcastCluster);
        
        clientConfig.setProperty("hazelcast.discovery.enabled", Boolean.TRUE.toString());
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(120);
        clientConfig.getNetworkConfig().getDiscoveryConfig().addDiscoveryStrategyConfig(new DiscoveryStrategyConfig(EmptyDiscoveryStrategy.class.getName()));
        clientConfig.getNetworkConfig().getDiscoveryConfig().setDiscoveryServiceProvider(discoveryServiceProvider);
        
        return clientConfig;
    }
    
    @Bean
    @ConditionalOnMissingBean(HazelcastInstance.class)
    public HazelcastInstance hazelcastInstance(ClientConfig clientConfig) {
        return HazelcastClient.newHazelcastClient(clientConfig);
    }
    
    public static class EmptyDiscoveryStrategy extends AbstractDiscoveryStrategy {
        public EmptyDiscoveryStrategy(ILogger logger, Map<String,Comparable> properties) {
            super(logger, properties);
        }
        
        @Override
        public Iterable<DiscoveryNode> discoverNodes() {
            return Collections.emptyList();
        }
    }
}
