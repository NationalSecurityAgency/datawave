package datawave.microservice.cached.server;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

/**
 * {@link ConfigurationProperties} for configuring our clustered Hazelcast server.
 */
@EnableConfigurationProperties(HazelcastServerProperties.class)
@ConfigurationProperties(prefix = "hazelcast.server")
public class HazelcastServerProperties {
    /**
     * The initial delay in seconds to wait before attempting to merge split-brain clusters. If multiple nodes are started simultaneously, it is possible they
     * will all end up creating their own clusters since each one will create its cluster before any of the others have registered with the discovery service.
     * Hazelcast will try to join such clusters together on a regular basis. We want to do so as soon as possible after application startup / registration with
     * the discovery service.
     */
    private int initialMergeDelaySeconds = 30;
    /**
     * If true, then the default configuration is skipped and only the XML configuration (plus discovery configuration) is used.
     */
    private boolean skipDefaultConfiguration = false;
    /**
     * If true, then configuration of discovery is skipped. Don't use this unless you really know what you are doing, since you will likely break clustering.
     */
    private boolean skipDiscoveryConfiguration = false;
    /**
     * A Hazelcast XML configuration. Ideally this should only define cache configurations.
     */
    private String xmlConfig;
    
    @NestedConfigurationProperty
    private KubernetesProperties k8s = new KubernetesProperties();
    
    public int getInitialMergeDelaySeconds() {
        return initialMergeDelaySeconds;
    }
    
    public void setInitialMergeDelaySeconds(int initialMergeDelaySeconds) {
        this.initialMergeDelaySeconds = initialMergeDelaySeconds;
    }
    
    public boolean isSkipDefaultConfiguration() {
        return skipDefaultConfiguration;
    }
    
    public void setSkipDefaultConfiguration(boolean skipDefaultConfiguration) {
        this.skipDefaultConfiguration = skipDefaultConfiguration;
    }
    
    public boolean isSkipDiscoveryConfiguration() {
        return skipDiscoveryConfiguration;
    }
    
    public void setSkipDiscoveryConfiguration(boolean skipDiscoveryConfiguration) {
        this.skipDiscoveryConfiguration = skipDiscoveryConfiguration;
    }
    
    public String getXmlConfig() {
        return xmlConfig;
    }
    
    public void setXmlConfig(String xmlConfig) {
        this.xmlConfig = xmlConfig;
    }
    
    public KubernetesProperties getK8s() {
        return k8s;
    }
    
    public void setK8s(KubernetesProperties k8s) {
        this.k8s = k8s;
    }
    
    public static class KubernetesProperties {
        private String serviceDnsName = "cache.datawave";
        private int serviceDnsTimeout = 10;
        
        public String getServiceDnsName() {
            return serviceDnsName;
        }
        
        public void setServiceDnsName(String serviceDnsName) {
            this.serviceDnsName = serviceDnsName;
        }
        
        public int getServiceDnsTimeout() {
            return serviceDnsTimeout;
        }
        
        public void setServiceDnsTimeout(int serviceDnsTimeout) {
            this.serviceDnsTimeout = serviceDnsTimeout;
        }
    }
}
