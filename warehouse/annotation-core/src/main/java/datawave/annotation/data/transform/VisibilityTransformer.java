package datawave.annotation.data.transform;

import java.util.Map;

import org.apache.accumulo.core.security.ColumnVisibility;

/**
 * Used for transforming visibilities into the standard metadata format to the ColumnVisibility representation used in the Accumulo Key. Implementations will
 * define specific source field name(s) to read from and write to.
 */
public interface VisibilityTransformer {
    // TODO: we may not need toMetadataMap because the original visibility metadata also be stored in Accumulo.
    Map<String,String> toMetadataMap(ColumnVisibility columnVisibility) throws AnnotationTransformException;

    ColumnVisibility toColumnVisibility(Map<String,String> metadataMap) throws AnnotationTransformException;
}
