package datawave.webservice.query.limit;

/**
 * Represents the mode in which the {@link QueryLimiterImpl} operates.
 */
public enum QueryLimiterMode {

    /**
     * The query limiter feature is fully disabled. Query information will not be tracked in Zookeeper, and query limits will not be enforced. Additionally, the
     * Zookeeper connection will not be created. Switching to this mode closes existing tracking resources and clears this server's tracked queries.
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
