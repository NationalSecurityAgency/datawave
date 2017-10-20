package datawave.microservice.cached.server;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 *
 */
@Configuration
public class HazelcastConfiguration {
    
    @Value("${spring.application.name}")
    private String clusterName;
    
    @Bean
    public Config config(DiscoveryServiceProvider discoveryServiceProvider) {
        Config config = new Config();
        
        config.getGroupConfig().setName(clusterName);
        
        config.setProperty("hazelcast.discovery.enabled", Boolean.TRUE.toString());
        JoinConfig joinConfig = config.getNetworkConfig().getJoin();
        joinConfig.getMulticastConfig().setEnabled(false);
        joinConfig.getDiscoveryConfig().setDiscoveryServiceProvider(discoveryServiceProvider);
        
        return config;
    }
}
