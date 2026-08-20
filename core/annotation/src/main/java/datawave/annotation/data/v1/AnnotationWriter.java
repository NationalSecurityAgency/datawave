package datawave.annotation.data.v1;

import java.util.Optional;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationSource;

/**
 * Write contract for annotation data stores.
 * <p>
 * Implementations are responsible for validating writable annotation data, assigning store-managed identifiers when needed, and persisting annotation changes.
 */
public interface AnnotationWriter {

    /**
     * Adds a new annotation source.
     *
     * @param annotationSource
     *            the annotation source to add; callers should not pre-populate store-managed ids
     * @return the persisted annotation source, including any ids assigned by the writer
     */
    Optional<AnnotationSource> addAnnotationSource(AnnotationSource annotationSource);

    /**
     * Adds a new annotation.
     *
     * @param annotation
     *            the annotation to add; callers should not pre-populate store-managed annotation or segment ids
     * @return the persisted annotation, including any ids assigned by the writer
     */
    Optional<Annotation> addAnnotation(Annotation annotation);

    /**
     * Creates an update for an existing annotation.
     * <p>
     * Implementations may preserve previous annotation versions and link the new annotation back to {@code targetAnnotationId} instead of overwriting the
     * existing annotation in place.
     *
     * @param targetAnnotationId
     *            the id of the existing annotation being updated
     * @param annotation
     *            the updated annotation data to persist
     * @return the persisted update annotation, including any ids assigned by the writer
     */
    Optional<Annotation> updateAnnotation(String targetAnnotationId, Annotation annotation);

    /**
     * Deletes all stored entries for a document annotation id.
     *
     * @param shard
     *            the shard for the annotated document
     * @param datatype
     *            the datatype for the annotated document
     * @param uid
     *            the unique id for the annotated document
     * @param annotationId
     *            the annotation id to delete
     */
    void deleteAnnotation(String shard, String datatype, String uid, String annotationId);
}
