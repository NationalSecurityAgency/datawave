package datawave.webservice.query.cache;

import datawave.configuration.RefreshableScope;

/**
 * Configuration file is located at: datawave/query/CachedResultsExpiration.xml
 */
@RefreshableScope
public class CachedResultsExpirationConfiguration {

    private static final long DEFAULT_CLOSE_CONNECTIONS_TIME_MINS = 15;
    private static final long DEFAULT_EVICTION_TIME_MINS = 15;

    private long closeConnectionsTimeMins = DEFAULT_CLOSE_CONNECTIONS_TIME_MINS;
    private long evictionTimeMins = DEFAULT_EVICTION_TIME_MINS;

    public long getCloseConnectionsTime() {
        return closeConnectionsTimeMins;
    }

    public long getCloseConnectionsTimeMs() {
        return closeConnectionsTimeMins * 60 * 1000;
    }

    public void setCloseConnectionsTime(long closeConnectionsTime) {
        this.closeConnectionsTimeMins = closeConnectionsTime;
    }

    public long getEvictionTime() {
        return evictionTimeMins;
    }

    public long getEvictionTimeMs() {
        return evictionTimeMins * 60 * 1000;
    }

    public void setEvictionTime(long evictionTime) {
        this.evictionTimeMins = evictionTime;
    }

}
