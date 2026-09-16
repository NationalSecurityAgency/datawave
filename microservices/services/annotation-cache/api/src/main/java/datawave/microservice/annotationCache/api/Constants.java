package datawave.microservice.annotationCache.api;

/** Shared Hazelcast map names used by the annotation cache clients and service. */
public final class Constants {
    public static final String ANNOTATIONS_MAP = "annotations:";
    public static final String DOC_ANNOTATIONS_MAP = "doc-annotations";
    public static final String FETCH_MAP = "doc-fetch-record:";

    /** AnnotationMessage parameter containing the identifier type used in the document cache key. */
    public static final String ID_TYPE_PARAMETER = "id.type";

    /** AnnotationMessage parameter identifying the region in which the message originated. */
    public static final String REGION_ID_PARAMETER = "region.id";

    private Constants() {}
}
