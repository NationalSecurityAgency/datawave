package nsa.datawave.webservice.query.result.util.protostuff;

import java.util.HashMap;
import java.util.Map;

/**
 * Helper class to make protostuff serialization more type-safe. The enumeration prevents errors trying to keep protostuff strings and indices in sync across
 * protostuff serialization methods. Using this class, the string and indices can be defined in a single place, the enum.
 * 
 * NOTE: the {@code Enum<E>} MUST contain an entry for field number 0. No real way to enforce this at compile time.
 * 
 * @param <E>
 */
public class ProtostuffField<E extends Enum<E> & FieldAccessor> implements FieldParser {
    
    Map<Integer,E> numberMap = new HashMap<Integer,E>();
    Map<String,E> nameMap = new HashMap<String,E>();
    
    public ProtostuffField() {}
    
    public ProtostuffField(Class<E> enumClazz) {
        for (E field : enumClazz.getEnumConstants()) {
            numberMap.put(field.getFieldNumber(), field);
            nameMap.put(field.getFieldName(), field);
        }
        // Unfortunately no way to figure this out at compile time.
        if (!numberMap.containsKey(0)) {
            throw new IllegalArgumentException("Class<E extends Enum<E>> must contain a zero index field.");
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.webservice.query.result.util.protostuff.FieldParser#parseFieldNumber(int)
     */
    public E parseFieldNumber(int fieldNumber) {
        E ret = null;
        try {
            ret = numberMap.get(fieldNumber);
        } catch (Exception e) {
            ret = parseFieldNumber(0);
        }
        return ret;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.webservice.query.result.util.protostuff.FieldParser#parseFieldName(java.lang.String)
     */
    public E parseFieldName(String fieldName) {
        E ret = null;
        try {
            ret = nameMap.get(fieldName);
        } catch (Exception e) {
            ret = parseFieldNumber(0);
        }
        return ret;
    }
    
}
