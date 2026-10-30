package datawave.microservice.annotationCache.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Retention, federation, and topology-recovery settings for the two fixed annotation-cache data maps. Annotation/fetch TTLs are independent positive whole
 * seconds, max-idle is disabled in map configuration, and fetch retention must not exceed annotation retention. Invalidation is best-effort; marker TTL remains
 * the fallback for stale successful-source-check metadata.
 */
@Configuration
@ConfigurationProperties(prefix = "annotation-cache")
public class AnnotationCacheProperties {
    /** {@code max-cache-age}: Maximum annotation-map lifetime (default: 1 hour). */
    private Duration maxCacheAge = Duration.ofHours(1);
    /** {@code max-fetch-age}: Maximum lifetime of a document fetch-freshness record (default: 5 minutes). */
    private Duration maxFetchAge = Duration.ofMinutes(5);
    /** {@code federation-lock-wait}: Maximum wait for an annotation lock during federation (default: 5 seconds). */
    private Duration federationLockWait = Duration.ofSeconds(5);
    /** {@code topology-monitoring-enabled}: Enable topology-triggered fetch-map invalidation (default: true). */
    private boolean topologyMonitoringEnabled = true;
    /** {@code topology-settle-delay}: Delay after a topology event before invalidation (default: 15 seconds). */
    private Duration topologySettleDelay = Duration.ofSeconds(15);
    /** {@code topology-poll-interval-ms}: Interval for checking deferred invalidation (default: 5000 ms). */
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
