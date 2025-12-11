package datawave.annotation.data.transform;

import java.util.Map;

import org.apache.accumulo.core.security.ColumnVisibility;

public class DefaultVisibilityTransformer implements VisibilityTransformer {

    public static final String VISIBILITY_METADATA_KEY = "visibility";

    /**
     * Finds the @{code CREATED_DATE_METADATA_KEY} in the metadataMap in ISO8601 zulu and transforms it into the number of milliseconds since the epoch.
     *
     * @param metadataMap
     *            a map containing metadata to read
     * @return the number of milliseconds since the epoch, 0 if the expected timestamp was not present.
     * @throws AnnotationTransformException
     *             if the created_date key is present but can't be parsed.
     */
    @Override
    public ColumnVisibility toColumnVisibility(Map<String,String> metadataMap) throws AnnotationTransformException {
        try {
            String visibilityExpression = metadataMap.get(VISIBILITY_METADATA_KEY);
            return new ColumnVisibility(visibilityExpression);
        } catch (Exception e) {
            throw new AnnotationTransformException("Exception parsing visibility for ColumnVisibility", e);
        }
    }

    @Override
    public Map<String,String> toMetadataMap(ColumnVisibility columnVisibility) throws AnnotationTransformException {
        try {
            String visibilityExpression = columnVisibility.toString();
            return Map.of(VISIBILITY_METADATA_KEY, visibilityExpression);
        } catch (Exception e) {
            throw new AnnotationTransformException("Exception transforming ColumnVisibility to metadata map", e);
        }
    }
}
