package datawave.query.testframework;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;

/**
 * Defines the actions for retrieval and processing of raw data.
 */
public interface IRawData {
    
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
}
