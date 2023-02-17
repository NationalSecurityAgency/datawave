package datawave.ingest.table.bloomfilter;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.file.keyfunctor.KeyFunctor;

/**
 * This is a function that will create a bloom filter key from a Key from the shard table. The bloom filter is based on the field name and field value if the
 * field index column. All other columns are not considered in the bloom filter.
 * 
 * 
 * 
 */
public class ShardKeyFunctor implements KeyFunctor {
    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final byte[] FIELD_INDEX_PREFIX = new byte[] {'f', 'i', '\0'};
    
    /**
     * Transform a accumulo key into a bloom filter key. This is expected to always return a value, however returning a zero length bloom filter key is
     * equivalent to ignoring the bloom filters for this key.
     */
    @Override
    public org.apache.hadoop.util.bloom.Key transform(org.apache.accumulo.core.data.Key cbKey) {
        
        if (isKeyInBloomFilter(cbKey)) {
            byte keyData[];
            
            // The column qualifier contains the field value
            ByteSequence cq = cbKey.getColumnQualifierData();
            int index = getIndexOf(cq, (byte) 0);
            
            // The column family is the field name
            ByteSequence cf = cbKey.getColumnFamilyData();
            
            keyData = new byte[index + cf.length() - 3];
            System.arraycopy(cf.getBackingArray(), 3, keyData, 0, cf.length() - 3);
            System.arraycopy(cq.getBackingArray(), 0, keyData, cf.length() - 3, index);
            
            return new org.apache.hadoop.util.bloom.Key(keyData, 1.0);
        }
        
        return new org.apache.hadoop.util.bloom.Key(EMPTY_BYTES, 1.0);
    }
    
    /**
     * Return bloom filter key for the start of the range. Returning null or an zero length key is equivalent to ignoring the bloom filters for this key.
     */
    @Override
    public org.apache.hadoop.util.bloom.Key transform(Range range) {
        if (isRangeInBloomFilter(range)) {
            return transform(range.getStartKey());
        }
        return null;
    }
    
    /**
     * Determine whether this range should be considered by the bloom filter.
     * 
     * @param range
     *            the range to check
     * @return true if it is to be considered, false otherwise
     */
    static boolean isRangeInBloomFilter(Range range) {
        
        /**
         * If the range has no start key or no end key, then ignore the bloom filters
         */
        if (range.getStartKey() == null || range.getEndKey() == null) {
            return false;
        }
        
        /**
         * If this key is not in the bloom filter, then ignore the bloom filters
         */
        if (!isKeyInBloomFilter(range.getStartKey())) {
            return false;
        }
        
        /**
         * If the start key and the end key are equal up to the depth being considered, then we should consider the bloom filter.
         */
        if (range.getStartKey().equals(range.getEndKey(), PartialKey.ROW_COLFAM_COLQUAL))
            return true;
        
        /**
         * If the end key is precisely the key immediately after the start key including everything up to the deleted flag, then we should consider the bloom
         * filter.
         */
        return range.getStartKey().followingKey(PartialKey.ROW_COLFAM_COLQUAL).equals(range.getEndKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME)
                        && !range.isEndKeyInclusive();
    }
    
    /**
     * A key is to be considered by the bloom filter if it is for a field index column and both the field name and field value are supplied.
     * 
     * @param cbKey
     *            the the key to check
     * @return whether the key is in the filter
     */
    static boolean isKeyInBloomFilter(org.apache.accumulo.core.data.Key cbKey) {
        byte[] cf = cbKey.getColumnFamilyData().getBackingArray();
        // if we have a column family with the field name and a column qualifier with the field value, then
        // we can use the bloom filter for this key
        return (cf.length > 3 && cf[0] == FIELD_INDEX_PREFIX[0] && cf[1] == FIELD_INDEX_PREFIX[1] && cf[2] == FIELD_INDEX_PREFIX[2] && cbKey
                        .getColumnQualifierData().length() > 0);
    }
    
    /**
     * Get the index of a byte in a byte sequence. The byte sequence length is returned if the byte is not found.
     * 
     * @param bytes
     *            the byte sequence
     * @param val
     *            the value
     * @return the index of a byte, or the byte sequence length if not found
     */
    static int getIndexOf(ByteSequence bytes, byte val) {
        byte[] data = bytes.getBackingArray();
        for (int i = 0; i < data.length; i++) {
            if (data[i] == val) {
                return i;
            }
        }
        return data.length;
    }
}
