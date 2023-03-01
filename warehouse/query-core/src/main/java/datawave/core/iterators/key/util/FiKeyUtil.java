package datawave.core.iterators.key.util;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;

/**
 * A utility for determining key type and accessing the field, value, datatype and uid components
 * <p>
 * Note: each method call will return a new object, values are not cached, state is not saved
 * <p>
 * A FieldIndex key structure is row:fi\0FIELD:value\0datatype\0uid
 */
public class FiKeyUtil {
    
    private FiKeyUtil() {
        // static utility
    }
    
    /**
     * Determines if the provided key is an instance of a FieldIndex key
     * 
     * @param k
     *            the key
     * @return true if this a FieldIndex key
     */
    public static boolean instanceOf(Key k) {
        ByteSequence bytes = k.getColumnFamilyData();
        return bytes != null && bytes.length() > 3 && bytes.byteAt(2) == '\u0000' && bytes.byteAt(1) == 'i' && bytes.byteAt(0) == 'f';
    }
    
    /**
     * Parses the field as a String
     * 
     * @param k
     *            the key
     * @return the field
     */
    public static String getFieldString(Key k) {
        ByteSequence bytes = k.getColumnFamilyData();
        return bytes.subSequence(3, bytes.length()).toString();
    }
    
    /**
     * Parses the value as a String
     * 
     * @param k
     *            the key
     * @return the value
     */
    public static String getValueString(Key k) {
        ByteSequence bytes = k.getColumnQualifierData();
        int nullCount = 0;
        for (int i = bytes.length() - 1; i >= 0; i--) {
            if (bytes.byteAt(i) == 0x00 && ++nullCount == 2) {
                return bytes.subSequence(0, i).toString();
            }
        }
        return throwParseException(k);
    }
    
    /**
     * Parses the datatype as a String
     * 
     * @param k
     *            the key
     * @return the datatype
     */
    public static String getDatatypeString(Key k) {
        ByteSequence bytes = k.getColumnQualifierData();
        int stop = -1;
        for (int i = bytes.length() - 1; i >= 0; i--) {
            if (bytes.byteAt(i) == 0x00) {
                if (stop == -1) {
                    stop = i;
                } else {
                    return bytes.subSequence(i + 1, stop).toString();
                }
                
            }
        }
        return throwParseException(k);
    }
    
    /**
     * Parses the uid as a String
     * 
     * @param k
     *            the key
     * @return the uid
     */
    public static String getUidString(Key k) {
        ByteSequence bytes = k.getColumnQualifierData();
        for (int i = bytes.length() - 1; i >= 0; i--) {
            if (bytes.byteAt(i) == 0x00) {
                return bytes.subSequence(i + 1, bytes.length()).toString();
            }
        }
        return throwParseException(k);
    }
    
    /**
     * Utility method for throwing an illegal argument exception
     * 
     * @param k
     *            the key
     * @return nothing
     */
    private static String throwParseException(Key k) {
        throw new IllegalArgumentException("Invalid number of null bytes in field index key column qualifier: " + k.getColumnQualifier().toString());
    }
}
