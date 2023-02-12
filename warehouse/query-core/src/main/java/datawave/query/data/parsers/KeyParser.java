package datawave.query.data.parsers;

import org.apache.accumulo.core.data.Key;

/**
 * A KeyParser provides a reusable method of accessing logical components of a given key type.
 * <p>
 * The following key types are supported
 * <ul>
 * <li>{@link FieldIndexKey}</li>
 * <li>{@link EventKey}</li>
 * <li>{@link TermFrequencyKey}</li>
 * </ul>
 * <p>
 * Design Considerations & Constraints
 * <ul>
 * <li>Thread safety is not guaranteed</li>
 * <li>Invalid keys will throw an IllegalArgumentException</li>
 * <li>Key's backing objects are only traversed once</li>
 * <li>Repeated calls to the same method return the same object</li>
 * </ul>
 */
public interface KeyParser {
    
    /**
     * Clear existing state and set key for lazy parsing
     *
     * @param k
     *            a key
     */
    void parse(Key k);
    
    /**
     * Clears existing state
     */
    void clearState();
    
    /**
     * Get the datatype
     *
     * @return the datatype
     */
    String getDatatype();
    
    /**
     *
     * @return the uid
     */
    String getUid();
    
    /**
     * Get the uid
     * 
     * @return the root uid
     */
    String getRootUid();
    
    /**
     * Get the field name
     *
     * @return the field name
     */
    String getField();
    
    /**
     * Get the field value
     * 
     * @return the field value
     */
    String getValue();
}
