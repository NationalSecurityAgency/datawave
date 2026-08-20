package datawave.annotation.data.v1;

import java.util.Collection;
import java.util.Optional;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationSource;

/**
 * Read-only contract for annotation data stores.
 * <p>
 * Implementations retrieve annotations by document identity ({@code shard}, {@code datatype}, and {@code uid}), annotation type, annotation id, and annotation
 * source id. Methods return empty results when no matching data is visible to the implementation.
 */
public interface AnnotationReader {

    /**
     * Retrieves the annotation source identified by its analytic hash.
     *
     * @param analyticHash
     *            the analytic hash assigned to the source
     * @return the matching annotation source, or {@link Optional#empty()} when it is not found
     */
    Optional<AnnotationSource> getAnnotationSource(String analyticHash);

    /**
     * Retrieves a single annotation for a document when both the annotation type and annotation id are known.
     *
     * @param shard
     *            the shard for the annotated document
     * @param datatype
     *            the datatype for the annotated document
     * @param uid
     *            the unique id for the annotated document
     * @param annotationType
     *            the annotation type to search
     * @param annotationId
     *            the annotation id to retrieve
     * @return the matching annotation, or {@link Optional#empty()} when it is not found
     */
    Optional<Annotation> getAnnotation(String shard, String datatype, String uid, String annotationType, String annotationId);

    /**
     * Retrieves a single annotation for a document by annotation id without requiring the caller to know the annotation type.
     *
     * @param shard
     *            the shard for the annotated document
     * @param datatype
     *            the datatype for the annotated document
     * @param uid
     *            the unique id for the annotated document
     * @param annotationId
     *            the annotation id to retrieve
     * @return the matching annotation, or {@link Optional#empty()} when it is not found
     */
    Optional<Annotation> getAnnotation(String shard, String datatype, String uid, String annotationId);

    /**
     * Lists the distinct annotation types currently available for a document.
     *
     * @param shard
     *            the shard for the annotated document
     * @param datatype
     *            the datatype for the annotated document
     * @param uid
     *            the unique id for the annotated document
     * @return the distinct annotation types visible for the document, never {@code null}
     */
    Collection<String> getAnnotationTypes(String shard, String datatype, String uid);

    /**
     * Retrieves all annotations currently available for a document.
     *
     * @param shard
     *            the shard for the annotated document
     * @param datatype
     *            the datatype for the annotated document
     * @param uid
     *            the unique id for the annotated document
     * @return the annotations visible for the document, never {@code null}
     */
    Collection<Annotation> getAnnotations(String shard, String datatype, String uid);

    /**
     * Retrieves all annotations of a specific type for a document.
     *
     * @param shard
     *            the shard for the annotated document
     * @param datatype
     *            the datatype for the annotated document
     * @param uid
     *            the unique id for the annotated document
     * @param annotationType
     *            the annotation type to retrieve
     * @return the matching annotations visible for the document and type, never {@code null}
     */
    Collection<Annotation> getAnnotationsForType(String shard, String datatype, String uid, String annotationType);
}
