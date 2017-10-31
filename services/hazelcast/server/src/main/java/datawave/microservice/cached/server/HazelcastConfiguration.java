package datawave.microservice.cached.server;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.ByteArrayInputStream;

/**
 * Defines the configuration we'll use for a Hazelcast cluster member (e.g., a server member).
 */
@Configuration
@EnableConfigurationProperties(HazelcastServerProperties.class)
public class HazelcastConfiguration {
    
    @Value("${spring.application.name}")
    private String clusterName;
    
    @Bean
    public Config config(HazelcastServerProperties serverProperties, DiscoveryServiceProvider discoveryServiceProvider) {
        
        Config config;
        
        if (serverProperties.getXmlConfig() == null) {
            config = new Config();
        } else {
            XmlConfigBuilder configBuilder = new XmlConfigBuilder(new ByteArrayInputStream(serverProperties.getXmlConfig().getBytes()));
            config = configBuilder.build();
        }
        
        // Set up some default configuration. Do this after we read the XML configuration (which is really intended just to be cache configurations).
        if (!serverProperties.isSkipDefaultConfiguration()) {
            config.getGroupConfig().setName(clusterName); // Set the cluster name
            config.setProperty("hazelcast.logging.type", "slf4j"); // Override the default log handler
            config.setProperty("hazelcast.rest.enabled", Boolean.TRUE.toString()); // Enable the REST endpoints so we can test/debug on them
            config.setProperty("hazelcast.phone.home.enabled", Boolean.FALSE.toString()); // Don't try to send stats back to Hazelcast
            config.setProperty("hazelcast.merge.first.run.delay.seconds", String.valueOf(serverProperties.getInitialMergeDelaySeconds()));
            config.getNetworkConfig().setReuseAddress(true); // Reuse addresses (so we can try to keep our port on a restart)
            
            // Enabled Consul-based discovery of cluster members
            config.setProperty("hazelcast.discovery.enabled", Boolean.TRUE.toString());
            JoinConfig joinConfig = config.getNetworkConfig().getJoin();
            joinConfig.getMulticastConfig().setEnabled(false);
            joinConfig.getDiscoveryConfig().setDiscoveryServiceProvider(discoveryServiceProvider);
        }
        
        return config;
    }
}
