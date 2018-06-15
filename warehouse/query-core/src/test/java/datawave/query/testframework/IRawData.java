package datawave.query.testframework;

import java.lang.reflect.Type;

/**
 * Defines the actions for retrieval and processing of raw data.
 */
public interface IRawData {
    
    /**
     * Retrieves the value for a specified field.
     *
     * @param field
     *            field name
     * @return value of field
     */
    String getValue(String field);
    
    /**
     * Determines if a field is designated as a multi-value field.
     *
     * @param field
     *            name of field
     * @return true if multi-value
     */
    boolean isMultiValueField(String field);
    
    /**
     * Returns the type for a field.
     *
     * @param field
     *            name of field
     * @return type
     */
    Type getFieldType(String field);
}
