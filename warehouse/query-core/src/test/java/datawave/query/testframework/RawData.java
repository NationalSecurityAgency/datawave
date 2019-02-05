package datawave.query.testframework;

import datawave.data.normalizer.Normalizer;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Defines the actions for retrieval and processing of raw data.
 */
public interface RawData {
    
    /**
     * Retrieves a mapping of the the raw entries. Multi-value field will be expanded into multiple entries.
     * 
     * @return mapping of field/value entries for raw data
     */
    Set<Map<String,String>> getMapping();
    
    /**
     * Retrieves the first value for a specified field.
     *
     * @param field
     *            field name
     * @return value of field or null if the field does not exist
     */
    String getValue(String field);
    
    /**
     * Determines if the specified fields is contained within the headers for this datatype.
     * 
     * @param field
     *            name of field
     * @return true if field is valid for datatype
     */
    boolean containsField(String field);
    
    /**
     * Returns an ordered list of header fields for the raw data.
     * 
     * @return list of field names
     */
    List<String> getHeaders();
    
    /**
     * Returns the normalizer for the field.
     *
     * @param field
     *            name of the field
     * @return normalizer object
     */
    Normalizer<?> getNormalizer(String field);
    
    /**
     * Retrieves the set of values for a field.
     * 
     * @param field
     *            field name
     * @return set of values for the field or null if it does not exist
     */
    Set<String> getAllValues(String field);
    
    /**
     * Determines if a field is designated as a multi-value field.
     *
     * @param field
     *            name of field
     * @return true if multi-value
     */
    boolean isMultiValueField(String field);
    
    /**
     * Indicates that the field should be tokenized. By default all fields are not tokenized. This method should be implemented for any datatype that contains a
     * tokenized field.
     * 
     * @param field
     *            name of the field
     * @return true if field is marked for tokenization
     */
    boolean isTokenizedField(String field);
    
    /**
     * Sets the key for the query field. The default value is the value of the field. Datatypes that use grouping must override this method.
     * 
     * @param field
     *            field name
     * @return key value to use for accessing the raw data (should be the query field name)
     */
    String getKey(String field);
}
