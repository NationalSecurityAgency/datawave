package datawave.microservice.annotationCache.api;

/** Shared Hazelcast map names and {@code AnnotationMessage} parameter keys for the annotation cache. */
public final class Constants {
    public static final String ANNOTATIONS_MAP = "annotations:";
    public static final String FETCH_MAP = "doc-fetch-record:";

    /** AnnotationMessage parameter containing the identifier type used in the document cache key. */
    public static final String ID_TYPE_PARAMETER = "id.type";

    /** AnnotationMessage parameter identifying the origin region, compared with the local {@code region.name}. */
    public static final String REGION_ID_PARAMETER = "region.id";

    /** AnnotationMessage parameter controlling whether a Hazelcast insertion should be written through to RabbitMQ. */
    public static final String PERSISTENCE_MODE_PARAMETER = "persistence.mode";

    private Constants() {}
}
