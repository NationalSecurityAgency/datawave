package datawave.annotation.data.visibility;

import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.core.security.ColumnVisibility;

public class DefaultAnnotationVisibilityTransformer implements AnnotationVisibilityTransformer {
    @Override
    public ColumnVisibility toColumnVisibility(Map<String,String> visibilityMap) throws AnnotationVisibilityException {
        String visibilityExpression = visibilityMap.get("visibility");
        if (visibilityExpression == null) {
            visibilityExpression = "";
        }
        return new ColumnVisibility(visibilityExpression);
    }

    @Override
    public Map<String,String> toVisibilityMap(ColumnVisibility columnVisibility) throws AnnotationVisibilityException {
        if (columnVisibility == null) {
            return Collections.emptyMap();
        }
        String visibilityExpression = columnVisibility.toString();
        return Map.of("visibility", visibilityExpression);
    }
}
