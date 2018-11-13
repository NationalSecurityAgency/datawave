package datawave.webservice.query.result.util.protostuff;

/**
 */
public interface FieldParser {
    
    /**
     * @param fieldNumber
     *            The protostuff field number for serialization.
     * @return
     */
    Enum<?> parseFieldNumber(int fieldNumber);
    
    /**
     * @param fieldName
     * @return
     */
    Enum<?> parseFieldName(String fieldName);
    
}
