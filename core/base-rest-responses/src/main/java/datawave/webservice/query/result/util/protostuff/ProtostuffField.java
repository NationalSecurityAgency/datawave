package datawave.webservice.query.result.util.protostuff;

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
    
    private Map<Integer,E> numberMap = new HashMap<>();
    private Map<String,E> nameMap = new HashMap<>();
    
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
    
    public E parseFieldNumber(int fieldNumber) {
        return parseFieldKey(numberMap, fieldNumber);
    }
    
    public E parseFieldName(String fieldName) {
        return parseFieldKey(nameMap, fieldName);
    }
    
    private E parseFieldKey(Map<?,E> map, Object fieldKey) {
        E ret;
        try {
            ret = map.get(fieldKey);
        } catch (Exception e) {
            ret = parseFieldNumber(0);
        }
        return ret;
    }
}
