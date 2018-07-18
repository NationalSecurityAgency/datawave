package datawave.microservice.cached;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.kubernetes.HazelcastKubernetesDiscoveryStrategyFactory;
import com.hazelcast.kubernetes.KubernetesProperties;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.hazelcast.HazelcastAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.io.ByteArrayInputStream;

/**
 * Auto-configuration necessary to set up a Hazelcast client that connects to a Hazelcast cluster that has been discovered using Kubernetes.
 */
@Configuration
@Profile("k8s")
@ConditionalOnProperty(name = "hazelcast.client.enabled", matchIfMissing = true)
@Conditional(HazelcastCacheCondition.class)
@AutoConfigureBefore({HazelcastAutoConfiguration.class, HazelcastDefaultClientAutoConfiguration.class})
@AutoConfigureAfter(HazelcastDiscoveryServiceAutoConfiguration.class)
@EnableConfigurationProperties(HazelcastClientProperties.class)
public class HazelcastK8sClientAutoConfiguration {
    @Bean
    public ClientConfig clientConfig(HazelcastClientProperties clientProperties) {
        ClientConfig clientConfig;
        if (clientProperties.getXmlConfig() == null) {
            clientConfig = new ClientConfig();
        } else {
            XmlClientConfigBuilder xmlBuilder = new XmlClientConfigBuilder(new ByteArrayInputStream(clientProperties.getXmlConfig().getBytes()));
            clientConfig = xmlBuilder.build();
        }
        
        if (!clientProperties.isSkipDefaultConfiguration()) {
            // Configure hazelcast properties
            clientConfig.setProperty("hazelcast.logging.type", "slf4j");
            clientConfig.setProperty("hazelcast.phone.home.enabled", Boolean.FALSE.toString());
            
            // Set our cluster name
            clientConfig.getGroupConfig().setName(clientProperties.getClusterName());
        }
        if (!clientProperties.isSkipDiscoveryConfiguration()) {
            // Set up Kubernetes discovery of cluster members.
            clientConfig.setProperty("hazelcast.discovery.enabled", Boolean.TRUE.toString());
            clientConfig.getNetworkConfig().setConnectionAttemptLimit(120);
            DiscoveryStrategyConfig discoveryStrategyConfig = new DiscoveryStrategyConfig(new HazelcastKubernetesDiscoveryStrategyFactory());
            discoveryStrategyConfig.addProperty(KubernetesProperties.SERVICE_DNS.key(), clientProperties.getK8s().getServiceDnsName());
            discoveryStrategyConfig.addProperty(KubernetesProperties.SERVICE_DNS_TIMEOUT.key(), clientProperties.getK8s().getServiceDnsTimeout());
            clientConfig.getNetworkConfig().getDiscoveryConfig().addDiscoveryStrategyConfig(discoveryStrategyConfig);
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
