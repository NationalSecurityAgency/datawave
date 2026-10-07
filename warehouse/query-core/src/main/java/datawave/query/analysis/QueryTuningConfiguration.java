package datawave.query.analysis;

import java.util.Objects;

/** Versioned immutable clustering settings, suitable for reuse with the same deployment-configured analyzer. */
public final class QueryTuningConfiguration {
    public static final int SCHEMA_VERSION = 1;
    public static final int FINGERPRINT_VERSION = QueryFingerprint.VERSION;

    private final QueryClusterer.Options clusteringOptions;

    public QueryTuningConfiguration(QueryClusterer.Options clusteringOptions) {
        this.clusteringOptions = Objects.requireNonNull(clusteringOptions, "clusteringOptions");
    }

    public int getSchemaVersion() {
        return SCHEMA_VERSION;
    }

    public int getFingerprintVersion() {
        return FINGERPRINT_VERSION;
    }

    public QueryClusterer.Options toClusteringOptions() {
        return clusteringOptions;
    }
}
