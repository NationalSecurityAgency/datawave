package datawave.webservice.query.limit;

/**
 * Represents the mode to start the {@link QueryLimiterImpl} up in when configuring it.
 */
public enum QueryLimiterMode {

    /**
     * The query limiter feature is fully disabled. Query information will not be tracked in Zookeeper, and query limits will not be enforced. Additionally, the
     * Zookeeper connection will not be created.
     */
    DISABLED(false),

    /**
     * Query information will be tracked in Zookeeper (including new queries), but limits will not be enforced.
     */
    TRACK_ONLY(true),

    /**
     * Query information will be tracked in Zookeeper and limits will be enforced.
     */
    ENFORCE(true);

    private final boolean trackingEnabled;

    QueryLimiterMode(boolean trackingEnabled) {
        this.trackingEnabled = trackingEnabled;
    }

    public boolean isTrackingEnabled() {
        return trackingEnabled;
    }
}
