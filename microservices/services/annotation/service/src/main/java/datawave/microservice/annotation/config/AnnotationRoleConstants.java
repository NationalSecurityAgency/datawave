package datawave.microservice.annotation.config;

/**
 * Property names and function names shared by annotation role selection and backend configuration.
 */
public final class AnnotationRoleConstants {
    public static final String ROLE_OVERLAY_PREFIX = "annotation.deployment.overlays.";
    public static final String FUNCTION_DEFINITION = "spring.cloud.function.definition";
    public static final String STREAM_FUNCTION_DEFINITION = "spring.cloud.stream.function.definition";
    public static final String STREAM_FUNCTION_BINDINGS = "spring.cloud.stream.function.bindings";
    public static final String STREAM_FUNCTION_AUTODETECT = "spring.cloud.stream.function.autodetect";
    public static final String LOG_WRITER_ENABLED = "annotation.writers.log.enabled";
    public static final String ACCUMULO_WRITER_ENABLED = "annotation.writers.accumulo.enabled";
    public static final String DUMP_WRITER_ENABLED = "annotation.writers.dump.enabled";
    public static final String BUS_ENABLED = "spring.cloud.bus.enabled";
    public static final String BUS_PROPERTY_SOURCE = "springCloudBusOverridesProperties";
    public static final String ROLE_FUNCTION_PROPERTY_SOURCE = "annotationRoleFunctions";
    public static final String ANNOTATION_SOURCE = "annotationSource";
    public static final String LOG_ANNOTATION_SINK = "logAnnotationSink";
    public static final String ACCUMULO_ANNOTATION_SINK = "accumuloAnnotationSink";
    public static final String DUMP_ANNOTATION_SINK = "dumpAnnotationSink";
    public static final String BUS_CONSUMER = "busConsumer";

    private AnnotationRoleConstants() {}
}
