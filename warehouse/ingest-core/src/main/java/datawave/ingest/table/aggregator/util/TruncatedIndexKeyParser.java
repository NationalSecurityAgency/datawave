package datawave.ingest.table.aggregator.util;

import java.util.BitSet;

import org.apache.accumulo.core.data.Key;

import datawave.ingest.table.aggregator.TruncatedIndexConversionIterator;

/**
 * A parser that can handle either a shard index key in either standard format or truncated format
 * <p>
 * Converts a standard shard index key in the form
 *
 * <pre>
 * value FIELD:yyyyMMdd_shard0x00datatype (uid list)
 * </pre>
 *
 * to
 *
 * <pre>
 * value FIELD:yyyyMMdd0x00datatype (bitset offset)
 * </pre>
 *
 * Can be used in a map reduce job or in conjunction with the {@link TruncatedIndexConversionIterator}.
 */
public class TruncatedIndexKeyParser extends AbstractIndexKeyParser {

    @Override
    public Key convert() {
        if (isStandardKey()) {
            // might want to snag the delete flag as well in the future
            return new Key(getValue(), getField(), getDate() + NULL_CHAR + getDatatype(), key.getColumnVisibilityParsed(), key.getTimestamp());
        } else {
            // become a pass-through if the key is already truncated, or is a sharded key
            return key;
        }
    }

    public BitSet getBitset() {
        if (isTruncatedKey() || isShardedDayKey() || isShardedYearKey()) {
            // pass-though for truncated or sharded keys
            return null;
        }

        if (bitset == null) {
            String shardNumber = cq.substring(cqUnderscoreIndex + 1, cqNullIndex);
            int num = Integer.parseInt(shardNumber);
            bitset = new BitSet();
            bitset.set(num);
        }

        return bitset;
    }

}
