package datawave.microservice.annotationCache.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/** Retention, federation, and topology-recovery settings for annotation-cache maps. */
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
    /** {@code map-cleanup-enabled}: Enable removal of empty per-document maps (default: false). */
    private boolean mapCleanupEnabled;
    /** {@code map-cleanup-interval}: Interval between empty-map cleanup scans (default: 5 minutes). */
    private Duration mapCleanupInterval = Duration.ofMinutes(5);
    /** {@code empty-map-grace-period}: Time a map must remain empty before destruction (default: 30 minutes). */
    private Duration emptyMapGracePeriod = Duration.ofMinutes(30);
    /** {@code cleanup-lock-wait}: Maximum wait for a document lifecycle lock during cleanup (default: 1 second). */
    private Duration cleanupLockWait = Duration.ofSeconds(1);

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

    /** @return whether empty-map cleanup is enabled */
    public boolean isMapCleanupEnabled() {
        return mapCleanupEnabled;
    }

    /**
     * @param mapCleanupEnabled
     *            whether empty-map cleanup is enabled
     */
    public void setMapCleanupEnabled(boolean mapCleanupEnabled) {
        this.mapCleanupEnabled = mapCleanupEnabled;
    }

    /** @return interval between empty-map cleanup scans */
    public Duration getMapCleanupInterval() {
        return mapCleanupInterval;
    }

    /**
     * @param mapCleanupInterval
     *            interval between empty-map cleanup scans
     */
    public void setMapCleanupInterval(Duration mapCleanupInterval) {
        this.mapCleanupInterval = mapCleanupInterval;
    }

    /** @return time a map must remain empty before destruction */
    public Duration getEmptyMapGracePeriod() {
        return emptyMapGracePeriod;
    }

    /**
     * @param emptyMapGracePeriod
     *            time a map must remain empty before destruction
     */
    public void setEmptyMapGracePeriod(Duration emptyMapGracePeriod) {
        this.emptyMapGracePeriod = emptyMapGracePeriod;
    }

    /** @return maximum wait for a document lifecycle lock during cleanup */
    public Duration getCleanupLockWait() {
        return cleanupLockWait;
    }

    /**
     * @param cleanupLockWait
     *            maximum wait for a document lifecycle lock during cleanup
     */
    public void setCleanupLockWait(Duration cleanupLockWait) {
        this.cleanupLockWait = cleanupLockWait;
    }
}
