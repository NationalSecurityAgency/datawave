package datawave.webservice.query.result.util.protostuff;

public interface FieldParser {
    
    /**
     * @param fieldNumber
     *            The protostuff field number for serialization.
     * @return the protostuff message field enum value that corresponds to field number {@code fieldNumber}
     */
    Enum<?> parseFieldNumber(int fieldNumber);
    
    /**
     * @param fieldName
     *            The protostuff field name for serialization.
     * @return the protostuff message field enum value that corresponds to the named field {@code fieldName}
     */
    Enum<?> parseFieldName(String fieldName);
}
