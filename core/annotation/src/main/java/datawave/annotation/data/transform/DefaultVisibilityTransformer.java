package datawave.annotation.data.transform;

import static datawave.annotation.data.transform.MetadataTransformerBase.maybeMergeSingleValue;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.security.ColumnVisibility;

public class DefaultVisibilityTransformer implements VisibilityTransformer {

    public static final String VISIBILITY_METADATA_KEY = "visibility";
    public static final Set<String> VISIBILITY_METADATA_FIELDS = Set.of(VISIBILITY_METADATA_KEY);

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
    public ColumnVisibility fromMetadataMap(Map<String,String> metadataMap) throws AnnotationTransformException {
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

    @Override
    public Set<String> getMetadataFields() {
        return VISIBILITY_METADATA_FIELDS;
    }

    @Override
    public Map<String,String> mergeMetadataMaps(Map<String,String> first, Map<String,String> second) {
        Map<String,String> result = maybeMergeSingleValue(VISIBILITY_METADATA_KEY, first, second);
        if (result == null) {
            // if we are here, we know that first and second have non-empty values for the field in question.
            // both values populated and different - need to return them combined, but in alpha order.
            SortedSet<String> orderedSet = new TreeSet<>();
            orderedSet.add(first.get(VISIBILITY_METADATA_KEY));
            orderedSet.add(second.get(VISIBILITY_METADATA_KEY));
            Iterator<String> it = orderedSet.iterator();
            return Map.of(VISIBILITY_METADATA_KEY, "(" + it.next() + "|" + it.next() + ")");
        }
        return result;
    }

}
