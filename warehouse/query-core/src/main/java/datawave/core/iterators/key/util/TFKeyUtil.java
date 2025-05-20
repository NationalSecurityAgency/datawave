package datawave.core.iterators.key.util;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;

/**
 * A utility for determining key type and accessing the field, value, datatype and uid components
 * <p>
 * Note: Each method call will return a new object. Values are not cached, no state is saved.
 * <p>
 * A TermFrequency key structure is row:tf:datatype\0uid\0value\0FIELD
 */
public class TFKeyUtil {

    private TFKeyUtil() {
        // static utility
    }

    /**
     * Determines if the provided key is an instance of a TermFrequency key
     *
     * @param k
     *            the key
     * @return true if this is a TermFrequency key
     */
    public static boolean instanceOf(Key k) {
        ByteSequence bytes = k.getColumnFamilyData();
        return bytes != null && bytes.length() == 2 && bytes.byteAt(0) == 't' && bytes.byteAt(1) == 'f';
    }

    /**
     * Parses the field as a String
     *
     * @param k
     *            the key
     * @return the field
     */
    public static String getFieldString(Key k) {
        ByteSequence bytes = k.getColumnQualifierData();
        for (int i = bytes.length() - 1; i >= 0; i--) {
            if (bytes.byteAt(i) == 0x00) {
                return bytes.subSequence(i + 1, bytes.length()).toString();
            }
        }
        return null;
    }

    /**
     * Parses the value as a String
     *
     * @param k
     *            the key
     * @return the value
     */
    public static String getValueString(Key k) {

        // value could contain nulls, so find the start and stop indices separately
        ByteSequence bytes = k.getColumnQualifierData();

        // find stop index
        int stop = -1;
        for (int i = bytes.length() - 1; i >= 0; i--) {
            if (bytes.byteAt(i) == 0x00) {
                stop = i;
                break;
            }
        }

        // find start index
        int start = -1;
        for (int i = 0; i < bytes.length(); i++) {
            if (bytes.byteAt(i) == 0x00) {
                if (start == -1) {
                    start = i;
                } else {
                    start = i;
                    break;
                }
            }
        }

        return bytes.subSequence(start + 1, stop).toString();
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

        for (int i = 0; i < bytes.length(); i++) {
            if (bytes.byteAt(i) == 0x00) {
                return bytes.subSequence(0, i).toString();
            }
        }
        return null;
    }

    /**
     * Parses the uid as a String
     *
     * @param k
     *            the key
     * @return the uid
     */
    public static String getUidString(Key k) {
        int start = -1;
        ByteSequence bytes = k.getColumnQualifierData();
        for (int i = 0; i < bytes.length(); i++) {
            if (bytes.byteAt(i) == 0x00) {
                if (start == -1) {
                    start = i;
                } else {
                    return bytes.subSequence(start + 1, i).toString();
                }
            }
        }
        return null;
    }
}
