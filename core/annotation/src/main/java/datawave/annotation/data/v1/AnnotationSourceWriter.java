package datawave.annotation.data.v1;

import java.util.Optional;

import datawave.annotation.protobuf.v1.AnnotationSource;

/**
 * Write contract for annotation source data stores.
 * <p>
 * Implementations are responsible for validating writable annotation data, assigning store-managed identifiers when needed, and persisting annotation source
 * changes.
 */
public interface AnnotationSourceWriter {

    /**
     * Adds a new annotation source.
     *
     * @param annotationSource
     *            the annotation source to add; callers should not pre-populate store-managed ids
     * @return the persisted annotation source, including any ids assigned by the writer
     */
    Optional<AnnotationSource> addAnnotationSource(AnnotationSource annotationSource);

}
