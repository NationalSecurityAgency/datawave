package nsa.datawave.webservice.query.result.util.protostuff;

/**
 */
public interface FieldParser {
    
    /**
     * @param fieldNumber
     *            The protostuff field number for serialization.
     * @return
     */
    public Enum<?> parseFieldNumber(int fieldNumber);
    
    /**
     * @param fieldName
     * @return
     */
    public Enum<?> parseFieldName(String fieldName);
    
}
