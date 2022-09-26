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
     * @param key
     *            Accumulo key to parse
     * @param isIndexTable
     *            true if the key is from an index table, false if the shard table
     * @return a ByteSequence containing the data type name, possibly returning null or empty for unexpected column data.
     */
    public static final ByteSequence parseKey(Key key, boolean isIndexTable) {
        /**
         * Supports the shard and index table. There should not be a failure, however if either one is used on the incorrect table
         */
        if (isIndexTable) {
            return extractDatatypeForIndexTable(key);
        } else {
            return extractDatatypeForShardTable(key);
        }
    }
    
    private static final ByteSequence extractDatatypeForShardTable(Key k) {
        // ASSUMES THAT THE KEY STARTS WITH A CORRECTLY SIZED BYTE ARRAY
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
        int nullIndex = -1;
        
        int cfLength = cf.length;
        for (int i = 0; i < cfLength; i++) {
            if (cf[i] == NULL_BYTE) {
                nullIndex = i;
                break;
            }
        }
        // data column
        if (nullIndex > 0) {
            return new ArrayByteSequence(cf, 0, nullIndex);
        }
        return null;
    }
    
    private static ArrayByteSequence parseDataTypeForDocOrTf(Key k) {
        // get the column qualifier, so that we can use it throughout
        // ASSUMES THAT THE KEY STARTS WITH A CORRECTLY SIZED BYTE ARRAY
        final byte[] cq = k.getColumnQualifierData().getBackingArray();
        
        int cqLength = cq.length;
        
        int nullIndex = -1;
        
        // don't need to check the last byte as we expect more than one null if formatted correctly
        for (int i = 0; i < cqLength - 1; i++) {
            if (cq[i] == NULL_BYTE) {
                nullIndex = i;
                break;
            }
        }
        
        // the data type is the first part of this entry.
        if (nullIndex > 0) {
            return new ArrayByteSequence(cq, 0, nullIndex);
        }
        return null;
    }
    
    private static ArrayByteSequence parseDataTypeForFi(Key k) {
        // get the column qualifier, so that we can use it throughout
        // ASSUMES THAT THE KEY STARTS WITH A CORRECTLY SIZED BYTE ARRAY
        final byte[] cq = k.getColumnQualifierData().getBackingArray();
        
        int cqLength = cq.length;
        
        int nullIndex = -1;
        
        int uidIndex = -1;
        for (int i = cqLength - 1; i >= 0; i--) {
            if (cq[i] == NULL_BYTE) {
                if (uidIndex == -1)
                    uidIndex = i;
                else {
                    nullIndex = i + 1;
                }
                if (uidIndex > 0 && nullIndex > 0)
                    break;
            }
        }
        
        if (uidIndex > 0 && nullIndex > 0) {
            return new ArrayByteSequence(cq, nullIndex, (uidIndex - nullIndex));
        }
        return null;
    }
    
    private static ByteSequence extractDatatypeForIndexTable(Key k) {
        // get the column qualifier, so that we can use it throughout
        // ASSUMES THAT THE KEY STARTS WITH A CORRECTLY SIZED BYTE ARRAY
        final byte[] cq = k.getColumnQualifierData().getBackingArray();
        
        int cqLength = cq.length;
        
        int nullIndex = -1;
        
        for (int i = MIN_LENGTH_SHARD_ID; i < cqLength; i++) {
            if (cq[i] == NULL_BYTE) {
                nullIndex = i + 1;
                break;
            }
        }
        
        if (nullIndex > 0) {
            return new ArrayByteSequence(cq, nullIndex, (cqLength - nullIndex));
        }
        return null;
    }
}
