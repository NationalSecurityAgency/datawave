package datawave.webservice.query.cache;

import datawave.configuration.RefreshableScope;

/**
 * Configuration file is located at: datawave/query/QueryExpiration.xml
 */
@RefreshableScope
public class QueryExpirationConfiguration {

    public static final int PAGE_TIMEOUT_MIN_DEFAULT = 60;
    public static final int IDLE_TIME_MIN_DEFAULT = 15;

    private long idleTimeMinutes = IDLE_TIME_MIN_DEFAULT;
    private long callTimeMinutes = PAGE_TIMEOUT_MIN_DEFAULT;
    private long pageSizeShortCircuitCheckTimeMinutes = PAGE_TIMEOUT_MIN_DEFAULT / 2;
    private long pageShortCircuitTimeoutMinutes = Math.round(0.97 * PAGE_TIMEOUT_MIN_DEFAULT);
    private int maxLongRunningTimeoutRetries = 3;

    public long getIdleTimeMinutes() {
        return idleTimeMinutes;
    }

    public long getIdleTimeInMS() {
        return idleTimeMinutes * 60 * 1000;
    }

    public void setIdleTime(long idleTimeMinutes) {
        this.idleTimeMinutes = idleTimeMinutes;
    }

    public void setIdleTimeMinutes(long idleTimeMinutes) {
        this.idleTimeMinutes = idleTimeMinutes;
    }

    public long getCallTimeMinutes() {
        return callTimeMinutes;
    }

    public long getCallTimeInMS() {
        return callTimeMinutes * 60 * 1000;
    }

    public void setCallTime(long callTimeMinutes) {
        this.callTimeMinutes = callTimeMinutes;
    }

    public void setCallTimeMinutes(long callTimeMinutes) {
        this.callTimeMinutes = callTimeMinutes;
    }

    public float getPageSizeShortCircuitCheckTimeMinutes() {
        return pageSizeShortCircuitCheckTimeMinutes;
    }

    public long getPageSizeShortCircuitCheckTimeInMS() {
        return pageSizeShortCircuitCheckTimeMinutes * 60 * 1000;
    }

    public void setPageSizeShortCircuitCheckTime(long pageSizeShortCircuitCheckTimeMinutes) {
        this.pageSizeShortCircuitCheckTimeMinutes = pageSizeShortCircuitCheckTimeMinutes;
    }

    public void setPageSizeShortCircuitCheckTimeMinutes(long pageSizeShortCircuitCheckTimeMinutes) {
        this.pageSizeShortCircuitCheckTimeMinutes = pageSizeShortCircuitCheckTimeMinutes;
    }

    public long getPageShortCircuitTimeoutMinutes() {
        return pageShortCircuitTimeoutMinutes;
    }

    public long getPageShortCircuitTimeoutInMS() {
        return pageShortCircuitTimeoutMinutes * 60 * 1000;
    }

    public void setPageShortCircuitTimeout(long pageShortCircuitTimeoutMinutes) {
        this.pageShortCircuitTimeoutMinutes = pageShortCircuitTimeoutMinutes;
    }

    public void setPageShortCircuitTimeoutMinutes(long pageShortCircuitTimeoutMinutes) {
        this.pageShortCircuitTimeoutMinutes = pageShortCircuitTimeoutMinutes;
    }

    public int getMaxLongRunningTimeoutRetries() {
        return maxLongRunningTimeoutRetries;
    }

    public void setMaxLongRunningTimeoutRetries(int maxLongRunningTimeoutRetries) {
        this.maxLongRunningTimeoutRetries = maxLongRunningTimeoutRetries;
    }
}
