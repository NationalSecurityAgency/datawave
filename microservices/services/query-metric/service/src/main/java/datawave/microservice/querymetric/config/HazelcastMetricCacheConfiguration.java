package datawave.microservice.querymetric.config;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.Cache;
import org.springframework.cloud.consul.discovery.ConsulDiscoveryProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.kubernetes.HazelcastKubernetesDiscoveryStrategyFactory;
import com.hazelcast.kubernetes.KubernetesProperties;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceProvider;
import com.hazelcast.spring.cache.HazelcastCache;
import com.hazelcast.spring.cache.HazelcastCacheManager;

import datawave.microservice.querymetric.ClusterMembershipListener;
import datawave.microservice.querymetric.MergeLockLifecycleListener;
import datawave.microservice.querymetric.persistence.AccumuloMapLoader;
import datawave.microservice.querymetric.persistence.AccumuloMapStore;
import datawave.microservice.querymetric.persistence.MetricMapListener;

@Configuration
@ConditionalOnProperty(name = "hazelcast.server.enabled", havingValue = "true")
@EnableConfigurationProperties({HazelcastMetricCacheProperties.class})
public class HazelcastMetricCacheConfiguration {
    
    private Logger log = LoggerFactory.getLogger(HazelcastMetricCacheConfiguration.class);
    public static final String INCOMING_METRICS = "incomingQueryMetrics";
    
    @Value("${hazelcast.clusterName:${spring.application.name}}")
    private String clusterName;
    
    @Bean(name = "queryMetricCacheManager")
    public HazelcastCacheManager queryMetricCacheManager(@Qualifier("metrics") HazelcastInstance instance) throws IOException {
        return new HazelcastCacheManager(instance);
    }
    
    @Bean
    @Qualifier("metrics")
    HazelcastInstance hazelcastInstance(Config config, @Qualifier("store") AccumuloMapStore mapStore, @Qualifier("loader") AccumuloMapLoader mapLoader,
                    MergeLockLifecycleListener lifecycleListener, @Qualifier("lastWrittenQueryMetrics") Cache lastWrittenCache) {
        // Autowire both the AccumuloMapStore and AccumuloMapLoader so that they both get created
        // Ensure that the lastWrittenQueryMetricCache is set into the MapStore before the instance is active and the writeLock is released
        lifecycleListener.writeLockRunnable.lock(LifecycleEvent.LifecycleState.STARTING);
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        
        try {
            HazelcastCacheManager cacheManager = new HazelcastCacheManager(instance);
            
            HazelcastCache incomingMetricsCache = (HazelcastCache) cacheManager.getCache(INCOMING_METRICS);
            incomingMetricsCache.getNativeCache().addEntryListener(new MetricMapListener(INCOMING_METRICS), true);
            
            mapStore.setLastWrittenQueryMetricCache(lastWrittenCache);
            System.setProperty("hzAddress", instance.getCluster().getLocalMember().getAddress().toString());
            System.setProperty("hzUuid", instance.getCluster().getLocalMember().getUuid().toString());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            lifecycleListener.writeLockRunnable.unlock(LifecycleEvent.LifecycleState.STARTED);
        }
        return instance;
    }
    
    @Bean
    @Profile("consul")
    public Config consulConfig(HazelcastMetricCacheProperties serverProperties, DiscoveryServiceProvider discoveryServiceProvider,
                    ConsulDiscoveryProperties consulDiscoveryProperties, MergeLockLifecycleListener lifecycleListener) {
        consulDiscoveryProperties.getMetadata().put("hzHost", System.getProperty("hazelcast.cluster.host"));
        consulDiscoveryProperties.getMetadata().put("hzPort", System.getProperty("hazelcast.cluster.port"));
        
        consulDiscoveryProperties.getTags().add("hzHost=" + System.getProperty("hazelcast.cluster.host"));
        consulDiscoveryProperties.getTags().add("hzPort=" + System.getProperty("hazelcast.cluster.port"));
        
        Config config = generateDefaultConfig(serverProperties, lifecycleListener);
        
        // Set up some default configuration. Do this after we read the XML configuration (which is really intended just to be cache configurations).
        if (!serverProperties.isSkipDiscoveryConfiguration()) {
            // Enable Consul-based discovery of cluster members
            config.setProperty("hazelcast.discovery.enabled", Boolean.TRUE.toString());
            JoinConfig joinConfig = config.getNetworkConfig().getJoin();
            joinConfig.getMulticastConfig().setEnabled(false);
            joinConfig.getDiscoveryConfig().setDiscoveryServiceProvider(discoveryServiceProvider);
        }
        return config;
    }
    
    @Bean
    @Profile("k8s")
    public Config k8sConfig(HazelcastMetricCacheProperties serverProperties, MergeLockLifecycleListener lifecycleListener) {
        
        Config config = generateDefaultConfig(serverProperties, lifecycleListener);
        
        if (!serverProperties.isSkipDiscoveryConfiguration()) {
            // Enable Kubernetes discovery
            config.setProperty("hazelcast.discovery.enabled", Boolean.TRUE.toString());
            JoinConfig joinConfig = config.getNetworkConfig().getJoin();
            joinConfig.getMulticastConfig().setEnabled(false);
            HazelcastKubernetesDiscoveryStrategyFactory factory = new HazelcastKubernetesDiscoveryStrategyFactory();
            DiscoveryStrategyConfig discoveryStrategyConfig = new DiscoveryStrategyConfig(factory);
            discoveryStrategyConfig.addProperty(KubernetesProperties.SERVICE_DNS.key(), serverProperties.getK8s().getServiceDnsName());
            discoveryStrategyConfig.addProperty(KubernetesProperties.SERVICE_DNS_TIMEOUT.key(),
                            Integer.toString(serverProperties.getK8s().getServiceDnsTimeout()));
            joinConfig.getDiscoveryConfig().addDiscoveryStrategyConfig(discoveryStrategyConfig);
        }
        
        return config;
    }
    
    @Bean
    @Profile("!consul & !k8s")
    public Config ipConfig(HazelcastMetricCacheProperties serverProperties, MergeLockLifecycleListener lifecycleListener) {
        Config config = generateDefaultConfig(serverProperties, lifecycleListener);
        if (!serverProperties.isSkipDiscoveryConfiguration()) {
            try {
                JoinConfig joinConfig = config.getNetworkConfig().getJoin();
                Collection<DiscoveryStrategyConfig> discoveryStrategyConfigs = joinConfig.getDiscoveryConfig().getDiscoveryStrategyConfigs();
                TcpIpConfig tcpIpConfig = joinConfig.getTcpIpConfig();
                // skip if there is a different discovery strategy configured or if ip discovery is configured in XML
                if (discoveryStrategyConfigs.isEmpty() && tcpIpConfig.getMembers().isEmpty()) {
                    // Disable multicast discovery, enable ip discovery
                    // When omitting the port, Hazelcast will look for members at ports 5701, 5702, etc
                    joinConfig.getMulticastConfig().setEnabled(false);
                    tcpIpConfig.addMember("127.0.0.1");
                    tcpIpConfig.setEnabled(true);
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        return config;
    }
    
    @Bean
    @ConditionalOnMissingBean(Config.class)
    public Config defaultConfig(HazelcastMetricCacheProperties serverProperties, MergeLockLifecycleListener lifecycleListener) {
        return generateDefaultConfig(serverProperties, lifecycleListener);
    }
    
    private Config generateDefaultConfig(HazelcastMetricCacheProperties cacheProperties, MergeLockLifecycleListener lifecycleListener) {
        Config config;
        
        if (cacheProperties.getXmlConfig() == null) {
            config = new Config();
        } else {
            XmlConfigBuilder configBuilder = new XmlConfigBuilder(new ByteArrayInputStream(cacheProperties.getXmlConfig().getBytes(UTF_8)));
            config = configBuilder.build();
        }
        
        // Set up some default configuration. Do this after we read the XML configuration (which is really intended just to be cache configurations).
        if (!cacheProperties.isSkipDefaultConfiguration()) {
            config.setClusterName(clusterName); // Set the cluster name
            config.setProperty("hazelcast.logging.type", "slf4j"); // Override the default log handler
            config.setProperty("hazelcast.rest.enabled", Boolean.TRUE.toString()); // Enable the REST endpoints so we can test/debug on them
            config.setProperty("hazelcast.phone.home.enabled", Boolean.FALSE.toString()); // Don't try to send stats back to Hazelcast
            config.setProperty("hazelcast.merge.first.run.delay.seconds", Integer.toString(cacheProperties.getMergeDelaySeconds()));
            config.setProperty("hazelcast.merge.next.run.delay.seconds", Integer.toString(cacheProperties.getMergeIntervalSeconds()));
            config.getNetworkConfig().setReuseAddress(true); // Reuse addresses (so we can try to keep our port on a restart)
        }
        ListenerConfig lifecycleListenerConfig = new ListenerConfig();
        lifecycleListenerConfig.setImplementation(lifecycleListener);
        config.addListenerConfig(lifecycleListenerConfig);
        
        ListenerConfig membershipListenerConfig = new ListenerConfig();
        membershipListenerConfig.setImplementation(new ClusterMembershipListener());
        config.addListenerConfig(membershipListenerConfig);
        
        Map<String,MapConfig> mapConfigs = config.getMapConfigs();
        for (Map.Entry<String,MapConfig> e : mapConfigs.entrySet()) {
            InMemoryFormat inMemoryFormat = e.getValue().getInMemoryFormat();
            if (!inMemoryFormat.equals(InMemoryFormat.OBJECT)) {
                log.info("overriding in-memory-format:" + inMemoryFormat + " for map " + e.getKey() + " to OBJECT");
                e.getValue().setInMemoryFormat(InMemoryFormat.OBJECT);
            }
        }
        return config;
    }
}
