package datawave.microservice.cached;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@link ConfigurationProperties} for configuring a client to connect to a Hazelcast server.
 */
@ConfigurationProperties(prefix = "hazelcast.client")
public class HazelcastClientProperties {
    /**
     * The name of the Hazelcast cluster we are joining.
     */
    private String clusterName = "cache";
    /**
     * If true, then the default configuration is skipped and only the XML configuration is used. Don't use this unless you really know what you are doing,
     * since you will likely break clustering.
     */
    private boolean skipDefaultConfiguration = false;
    /**
     * A Hazelcast XML configuration. This can be used to configure, say, a NearCache.
     */
    private String xmlConfig;
    
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
    
    public String getXmlConfig() {
        return xmlConfig;
    }
    
    public void setXmlConfig(String xmlConfig) {
        this.xmlConfig = xmlConfig;
    }
}
