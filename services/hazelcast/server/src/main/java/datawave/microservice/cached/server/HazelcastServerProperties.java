package datawave.microservice.cached.server;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.concurrent.TimeUnit;

/**
 * {@link ConfigurationProperties} for configuring our clustered Hazelcast server.
 */
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
     * If true, then the default configuration is skipped and only the XML configuration is used. Don't use this unless you really know what you are doing,
     * since you will likely break clustering.
     */
    private boolean skipDefaultConfiguration = false;
    /**
     * A Hazelcast XML configuration. Ideally this should only define cache configurations.
     */
    private String xmlConfig;
    
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
    
    public String getXmlConfig() {
        return xmlConfig;
    }
    
    public void setXmlConfig(String xmlConfig) {
        this.xmlConfig = xmlConfig;
    }
}
