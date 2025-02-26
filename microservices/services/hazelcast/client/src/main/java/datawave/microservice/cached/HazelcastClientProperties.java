package datawave.microservice.cached;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

/**
 * {@link ConfigurationProperties} for configuring a client to connect to a Hazelcast server.
 */
@EnableConfigurationProperties(HazelcastClientProperties.class)
@ConfigurationProperties(prefix = "hazelcast.client")
public class HazelcastClientProperties {
    /**
     * The name of the Hazelcast cluster we are joining.
     */
    private String clusterName = "cache";
    /**
     * If true, then the default configuration is skipped and only the XML configuration is used.
     */
    private boolean skipDefaultConfiguration = false;
    /**
     * If true, then discovery configuration is skipped. Don't use this unless you really know what you are doing, since you will likely break clustering.
     */
    private boolean skipDiscoveryConfiguration = false;
    /**
     * A Hazelcast XML configuration. This can be used to configure, say, a NearCache.
     */
    private String xmlConfig;
    
    @NestedConfigurationProperty
    private KubernetesProperties k8s = new KubernetesProperties();
    
    public String getClusterName() {
        return clusterName;
    }
    
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
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
