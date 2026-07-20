package datawave.annotation.data.v1;

import java.util.Optional;

import datawave.annotation.protobuf.v1.AnnotationSource;

/**
 * Read-only contract for annotation source data stores.
 * <p>
 * Implementations retrieve annotation sources by annotation source id. Methods return empty results when no matching data is visible to the implementation.
 */
public interface AnnotationSourceReader {

    /**
     * Retrieves the annotation source identified by its analytic hash.
     *
     * @param analyticHash
     *            the analytic hash assigned to the source
     * @return the matching annotation source, or {@link Optional#empty()} when it is not found
     */
    Optional<AnnotationSource> getAnnotationSource(String analyticHash);
}
