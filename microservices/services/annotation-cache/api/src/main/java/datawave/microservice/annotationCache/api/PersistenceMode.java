package datawave.microservice.annotationCache.api;

/** Controls whether inserting an annotation into Hazelcast should publish it for permanent storage. */
public enum PersistenceMode {
    WRITE_THROUGH("write-through"), CACHE_ONLY("cache-only");

    private final String value;

    PersistenceMode(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    public static PersistenceMode fromValue(String value) {
        for (PersistenceMode mode : values()) {
            if (mode.value.equals(value)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("Unknown persistence mode: " + value);
    }
}
