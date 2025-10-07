package datawave.ingest.table.bloomfilter;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.file.keyfunctor.KeyFunctor;

/**
 * This is a function that will create a bloom filter key from a Key from the shard index or shard reverse index tables. The bloom filter is based on the field
 * name and field value.
 *
 *
 *
 */
public class ShardIndexKeyFunctor implements KeyFunctor {

    private static final byte[] EMPTY_BYTES = new byte[0];

    /**
     * Transform a Accumulo key into a bloom filter key. This is expected to always return a value, however returning a zero length bloom filter key is
     * equivalent to ignoring the bloom filters for this key.
     */
    @Override
    public org.apache.hadoop.util.bloom.Key transform(org.apache.accumulo.core.data.Key cbKey) {

        if (isKeyInBloomFilter(cbKey)) {
            byte keyData[];

            // The row is the field value
            ByteSequence row = cbKey.getRowData();

            // The column family is the field name
            ByteSequence cf = cbKey.getColumnFamilyData();

            keyData = new byte[row.length() + cf.length()];
            System.arraycopy(cf.getBackingArray(), 0, keyData, 0, cf.length());
            System.arraycopy(row.getBackingArray(), 0, keyData, cf.length(), row.length());

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
        if (range.getStartKey().equals(range.getEndKey(), PartialKey.ROW_COLFAM))
            return true;

        /**
         * If the end key is precisely the key immediately after the start key including everything up to the deleted flag, then we should consider the bloom
         * filter.
         */
        return range.getStartKey().followingKey(PartialKey.ROW_COLFAM).equals(range.getEndKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME)
                        && !range.isEndKeyInclusive();
    }

    /**
     * A key is to be considered by the bloom filter if the field name and field value are supplied.
     *
     * @param cbKey
     *            the key to check
     * @return if the key is in the filter
     */
    static boolean isKeyInBloomFilter(org.apache.accumulo.core.data.Key cbKey) {
        // if we have a row with the field name and a column familiy with the field value, then
        // we can use the bloom filter for this key
        return (cbKey.getRowData().length() > 0 && cbKey.getColumnFamilyData().length() > 0);
    }
}
