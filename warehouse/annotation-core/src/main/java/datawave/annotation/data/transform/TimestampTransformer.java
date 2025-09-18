package datawave.annotation.data.transform;

import java.util.Map;

/**
 * Used for transforming timestamps into the standard metadata format to the long representation used in the Accumulo Key. Implementations will define specific
 * source field name(s) to read from and write to.
 */
public interface TimestampTransformer {
    // TODO: we may not need toMetadataMap because the original timestamp metadata also be stored in Accumulo.
    Map<String,String> toMetadataMap(long timestamp) throws AnnotationTransformException;

    long toTimestamp(Map<String,String> visibilityMap) throws AnnotationTransformException;
}
