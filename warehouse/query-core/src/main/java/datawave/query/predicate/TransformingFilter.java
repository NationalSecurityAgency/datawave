package datawave.query.predicate;

import org.apache.accumulo.core.data.Key;

/**
 * a filter that may transform rejected keys to an acceptable form
 */
public interface TransformingFilter extends Filter {
    /**
     * Transform a Key rejected by keep() to an acceptable form
     *
     * @param toTransform
     *            the Key to transform
     * @return the transformed Key derived from toTransform or null if the Key cannot be transformed
     */
    Key transform(Key toTransform);
}
