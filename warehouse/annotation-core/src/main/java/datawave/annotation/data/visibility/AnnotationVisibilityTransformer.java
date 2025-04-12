package datawave.annotation.data.visibility;

import java.util.Map;

import org.apache.accumulo.core.security.ColumnVisibility;

public interface AnnotationVisibilityTransformer {
    // TODO: it's not totally clear that we'll need toVisibilityMap because the original visibility metadata will always be stored in Accumulo.
    Map<String,String> toVisibilityMap(ColumnVisibility columnVisibility) throws AnnotationVisibilityException;

    ColumnVisibility toColumnVisibility(Map<String,String> visibilityMap) throws AnnotationVisibilityException;
}
