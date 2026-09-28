package datawave.microservice.annotationCache.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/** Retention and topology-recovery settings for annotation-cache Hazelcast maps. */
@Configuration
@ConfigurationProperties(prefix = "annotation-cache")
public class AnnotationCacheProperties {
    private Duration maxCacheAge = Duration.ofHours(1);
    private Duration maxFetchAge = Duration.ofMinutes(5);
    private Duration federationLockWait = Duration.ofSeconds(5);
    private boolean topologyMonitoringEnabled = true;
    private Duration topologySettleDelay = Duration.ofSeconds(15);
    private long topologyPollIntervalMs = 5000;

    public Duration getMaxCacheAge() {
        return maxCacheAge;
    }

    public void setMaxCacheAge(Duration maxCacheAge) {
        this.maxCacheAge = maxCacheAge;
    }

    public Duration getMaxFetchAge() {
        return maxFetchAge;
    }

    public void setMaxFetchAge(Duration maxFetchAge) {
        this.maxFetchAge = maxFetchAge;
    }

    public Duration getFederationLockWait() {
        return federationLockWait;
    }

    public void setFederationLockWait(Duration federationLockWait) {
        this.federationLockWait = federationLockWait;
    }

    public boolean isTopologyMonitoringEnabled() {
        return topologyMonitoringEnabled;
    }

    public void setTopologyMonitoringEnabled(boolean topologyMonitoringEnabled) {
        this.topologyMonitoringEnabled = topologyMonitoringEnabled;
    }

    public Duration getTopologySettleDelay() {
        return topologySettleDelay;
    }

    public void setTopologySettleDelay(Duration topologySettleDelay) {
        this.topologySettleDelay = topologySettleDelay;
    }

    public long getTopologyPollIntervalMs() {
        return topologyPollIntervalMs;
    }

    public void setTopologyPollIntervalMs(long topologyPollIntervalMs) {
        this.topologyPollIntervalMs = topologyPollIntervalMs;
    }
}
