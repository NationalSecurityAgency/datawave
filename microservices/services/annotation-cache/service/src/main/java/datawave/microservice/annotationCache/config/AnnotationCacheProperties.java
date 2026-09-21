package datawave.microservice.annotationCache.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/** Retention policy for annotation-cache Hazelcast maps. */
@Configuration
@ConfigurationProperties(prefix = "annotation-cache")
public class AnnotationCacheProperties {
    private Duration maxCacheAge = Duration.ofHours(1);
    private Duration maxFetchAge = Duration.ofMinutes(5);
    private Duration federationLockWait = Duration.ofSeconds(5);
    private boolean reconciliationEnabled = true;
    private Duration reconciliationInterval = Duration.ofMinutes(5);
    private Duration reconciliationSettleDelay = Duration.ofSeconds(15);
    private int reconciliationMaxMapsPerRun = 1000;

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

    public boolean isReconciliationEnabled() {
        return reconciliationEnabled;
    }

    public void setReconciliationEnabled(boolean reconciliationEnabled) {
        this.reconciliationEnabled = reconciliationEnabled;
    }

    public Duration getReconciliationInterval() {
        return reconciliationInterval;
    }

    public void setReconciliationInterval(Duration reconciliationInterval) {
        this.reconciliationInterval = reconciliationInterval;
    }

    public Duration getReconciliationSettleDelay() {
        return reconciliationSettleDelay;
    }

    public void setReconciliationSettleDelay(Duration reconciliationSettleDelay) {
        this.reconciliationSettleDelay = reconciliationSettleDelay;
    }

    public int getReconciliationMaxMapsPerRun() {
        return reconciliationMaxMapsPerRun;
    }

    public void setReconciliationMaxMapsPerRun(int reconciliationMaxMapsPerRun) {
        this.reconciliationMaxMapsPerRun = reconciliationMaxMapsPerRun;
    }
}
