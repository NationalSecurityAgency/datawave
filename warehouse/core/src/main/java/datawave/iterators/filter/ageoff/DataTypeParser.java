package datawave.iterators.filter.ageoff;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;

/**
 * Originally extracted from DataTypeAgeOffFilter
 *
 * Contains a single public static method for parsing the datatype (as a ByteSequence) from a key.
 *
 * It can parse the various shard schema key structures including the d, fi, tf, and event schemas.
 *
 * It can also parse an index table key.
 */
public class DataTypeParser {
    
    private static final char FI_CF_CHAR_1 = 'f';
    private static final char FI_CF_CHAR_2 = 'i';
    
    private static final char TF_CF_CHAR_2 = 'f';
    private static final char TF_CF_CHAR_1 = 't';
    
    private static final char DOC_CF_CHAR = 'd';
    
    private static final int NULL_BYTE = 0x00;
    
    // shard ID's minimum length = sum of the minimum length of its components: year + month + day + underscore + single digit partition
    private static final int MIN_LENGTH_SHARD_ID = 10;
    
    /**
     * Extracts the dataType from a key based on whether it's an indexTable key or shard key.
     * 
     * @param key
     *            Accumulo key to parse
     * @param isIndexTable
     *            true if the key is from an index table, false if the shard table
     * @return a ByteSequence containing the data type name, possibly returning null or empty for unexpected column data.
     */
    public static final ByteSequence parseKey(Key key, boolean isIndexTable) {
        if (isIndexTable) {
            return extractDatatypeForIndexTable(key);
        } else {
            return extractDatatypeForShardTable(key);
        }
    }
    
    private static final ByteSequence extractDatatypeForShardTable(Key k) {
        // Assumes that the key starts with a correctly sized byte array
        byte[] cf = k.getColumnFamilyData().getBackingArray();
        
        if (cf.length >= 3 && cf[0] == FI_CF_CHAR_1 && cf[1] == FI_CF_CHAR_2 && cf[2] == NULL_BYTE) {
            return parseDataTypeForFi(k);
        } else if (cf.length == 2 && cf[0] == TF_CF_CHAR_1 && cf[1] == TF_CF_CHAR_2) {
            return parseDataTypeForDocOrTf(k);
        } else if (cf.length == 1 && cf[0] == DOC_CF_CHAR) {
            return parseDataTypeForDocOrTf(k);
        } else {
            return parseDataTypeForEvent(cf);
        }
    }
    
    private static ArrayByteSequence parseDataTypeForEvent(byte[] cf) {
        int cfLength = cf.length;
        for (int i = 0; i < cfLength; i++) {
            if (cf[i] == NULL_BYTE) {
                return (i > 0) ? new ArrayByteSequence(cf, 0, i) : null;
            }
        }
        return null;
    }
    
    private static ArrayByteSequence parseDataTypeForDocOrTf(Key k) {
        // Assumes that the key starts with a correctly sized byte array
        final byte[] cq = k.getColumnQualifierData().getBackingArray();
        
        // don't need to check the last byte as we expect more than one null if formatted correctly
        for (int i = 0; i < cq.length - 1; i++) {
            if (cq[i] == NULL_BYTE) {
                return (i > 0) ? new ArrayByteSequence(cq, 0, i) : null;
            }
        }
        return null;
    }
    
    private static ArrayByteSequence parseDataTypeForFi(Key k) {
        // Assumes that the key starts with a correctly sized byte array
        final byte[] cq = k.getColumnQualifierData().getBackingArray();
        
        int lastNullIndex = -1;
        
        for (int i = cq.length - 1; i >= 0; i--) {
            if (cq[i] == NULL_BYTE) {
                if (lastNullIndex == -1)
                    lastNullIndex = i;
                else {
                    // already found the last nullIndex, so this is the second to last one
                    return new ArrayByteSequence(cq, i + 1, (lastNullIndex - (i + 1)));
                }
            }
        }
        
        return null;
    }
    
    private static ByteSequence extractDatatypeForIndexTable(Key k) {
        // Assumes that the key starts with a correctly sized byte array
        final byte[] cq = k.getColumnQualifierData().getBackingArray();
        
        int cqLength = cq.length;
        
        for (int i = MIN_LENGTH_SHARD_ID; i < cqLength; i++) {
            if (cq[i] == NULL_BYTE) {
                return new ArrayByteSequence(cq, i + 1, (cqLength - (i + 1)));
            }
        }
        
        return null;
    }
}
