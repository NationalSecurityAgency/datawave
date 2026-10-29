package datawave.microservice.annotationCache.api;

import java.io.Serializable;

/**
 * Diagnostic details for a successful Datawave check stored under a {@link FetchKey}.
 *
 * <p>
 * Presence and TTL of the map entry determine marker freshness. The timestamp and count do not establish that the annotation cache contains a complete
 * snapshot. Hazelcast uses its default Java serialization for this DTO.
 */
public final class FetchRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    private final long fetchedAt;
    private final int annotationCountReturned;

    public FetchRecord(long fetchedAt, int annotationCountReturned) {
        this.fetchedAt = fetchedAt;
        this.annotationCountReturned = annotationCountReturned;
    }

    public long getFetchedAt() {
        return fetchedAt;
    }

    public int getAnnotationCountReturned() {
        return annotationCountReturned;
    }
}
