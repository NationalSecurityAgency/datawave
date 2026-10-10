package datawave.microservice.annotationCache.api;

/** Shared Hazelcast map names and {@code AnnotationMessage} parameter keys for the annotation cache. */
public final class Constants {
    /** Fixed shared map containing {@link AnnotationKey} to annotation entries. */
    public static final String ANNOTATIONS_MAP = "annotations";
    /** Fixed shared map containing {@link FetchKey} to {@link FetchRecord} entries. */
    public static final String FETCH_MAP = "doc-fetch-record";

    /** Hazelcast key-attribute path for identifier-type queries and indexes. */
    public static final String ID_TYPE_KEY_ATTRIBUTE = "__key.idType";
    /** Hazelcast key-attribute path for document queries and indexes. */
    public static final String DOCUMENT_ID_KEY_ATTRIBUTE = "__key.documentId";

    /** AnnotationMessage parameter containing the identifier type used in the document cache key. */
    public static final String ID_TYPE_PARAMETER = "id.type";

    /** AnnotationMessage parameter identifying the origin region, compared with the local {@code region.name}. */
    public static final String REGION_ID_PARAMETER = "region.id";

    /** AnnotationMessage parameter controlling whether a Hazelcast insertion should be written through to RabbitMQ. */
    public static final String PERSISTENCE_MODE_PARAMETER = "persistence.mode";

    private Constants() {}
}
