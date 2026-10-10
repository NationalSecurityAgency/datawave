package datawave.microservice.annotationCache.api;

/** Controls how the annotation MapStore handles an annotation write. */
public enum PersistenceMode {
    /** Publish local write-through annotations to RabbitMQ and wait for broker confirmation. */
    WRITE_THROUGH("write-through"),
    /** Skip RabbitMQ publication; the annotation remains cache-only. */
    CACHE_ONLY("cache-only");

    private final String value;

    PersistenceMode(String value) {
        this.value = value;
    }

    /** Returns the wire value used in the {@code persistence.mode} annotation parameter. */
    public String value() {
        return value;
    }

    /**
     * Parses a {@code persistence.mode} parameter value.
     *
     * @throws IllegalArgumentException
     *             if the value does not match a supported mode
     */
    public static PersistenceMode fromValue(String value) {
        for (PersistenceMode mode : values()) {
            if (mode.value.equals(value)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("Unknown persistence mode: " + value);
    }
}
