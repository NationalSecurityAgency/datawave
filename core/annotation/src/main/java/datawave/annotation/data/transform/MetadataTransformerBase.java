package datawave.annotation.data.transform;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Interface for implementations that transform data from the standard metadata format into a specific java type that corresponds to a portion of an Accumulo
 * key (e.g., ColumnVisibility or timestamp long). Typically implementations will be interested in specific values in the metadata map and only operate on those
 * values to generate the resulting object.
 * <p>
 * The contract implementation smust follow is that {@code toMetadataMap}/{@code fromMetadataMap} are symmetric - meaning that output from one can be provided
 * as input to the other and vice versa - without data loss. {@code getMetadataFields} must return the list of keys that will be written by @{code
 * toMetadataMap} and that any Maps returned by {@code toMetadataMap} or {@code mergeMetadataMaps} must only contain entries with keys returned by
 * {@code getMetadataFields}.
 * </p>
 *
 * @param <T>
 *            the destination type.
 */
public interface MetadataTransformerBase<T> {

    /**
     * Given a value, transform it to a string and return a Metadata map containing that object.
     *
     * @param input
     *            the value to transform.
     * @return the metadata map, an empty map if the input was null or 'empty'.
     * @throws AnnotationTransformException
     *             if there's an exception performing the transformation.
     */
    Map<String,String> toMetadataMap(T input) throws AnnotationTransformException;

    /**
     * Given a metadata map, return the corresponding value
     *
     * @param metadataMap
     *            the metadata map to transform into a value.
     * @return the value extracted from the metadata map, possibly null or a default value if the metadataMap is null or does not have an entry for the field
     *         the transformer implementation uses.
     * @throws AnnotationTransformException
     *             if there's an exception performing the transformation.
     */
    T fromMetadataMap(Map<String,String> metadataMap) throws AnnotationTransformException;

    /**
     * Get a set of the keys this transformer will consume/produce when {@code fromMetadataMap}/{@code toMetadataMap} is called.
     *
     * @return the set of fields.
     */
    Set<String> getMetadataFields();

    /**
     * Merge two metadata maps possibly containing entries for the same value.
     *
     * @param first
     *            the first metadata map to merge
     * @param second
     *            the second metadata map to merge.
     * @return a metadata map containing only the merged values and keys relevant to transformer implementation.
     */
    Map<String,String> mergeMetadataMaps(Map<String,String> first, Map<String,String> second);

    /**
     * Utility method to merge values associated with a specified key from two maps. This method expects that the specified key is not present in both maps and
     * will return null if the key is present in both. This indicates that a more complex merge algorithm is required.
     * <p>
     * Note: the map returned by this method will contain zero or one entries for the specified key (e.g., only if it is present in one of the supplied maps)
     * </p>
     * It is also worth noting that if this method returns null, we know that the specified field exists in both maps, is non-empty <i>and is different</i> in
     * both of the supplied maps.
     *
     * @param key
     *            the key that will be retrieved from each map
     * @param first
     *            the first map
     * @param second
     *            the second map
     * @return the result that should be used from this simple merge, containing a single entry from the specified key. This map is empty if the key is not
     *         present in either map and null if the value is present in both maps and requires a more sophisticated merge.
     */
    static Map<String,String> maybeMergeSingleValue(String key, Map<String,String> first, Map<String,String> second) {
        final String firstValue = (first == null || first.isEmpty()) ? "" : first.get(key);
        final String secondValue = (second == null || second.isEmpty()) ? "" : second.get(key);
        if (firstValue.isEmpty() && secondValue.isEmpty()) {
            // neither value is populated.
            return Collections.emptyMap();
        } else if (firstValue.isEmpty()) {
            // only second value is populated
            return Map.of(key, secondValue);
        } else if (secondValue.isEmpty()) {
            // only first value is populated
            return Map.of(key, firstValue);
        } else if (firstValue.equals(secondValue)) {
            // same value
            return Map.of(key, firstValue);
        }
        return null;
    }
}
