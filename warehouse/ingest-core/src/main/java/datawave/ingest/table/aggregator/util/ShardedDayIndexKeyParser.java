package datawave.ingest.table.aggregator.util;

import java.util.BitSet;

import org.apache.accumulo.core.data.Key;

/**
 * A parser that handles a converting a standard shard index key or a truncated shard index key to a sharded day index key.
 * <p>
 * Converts either a standard shard index key in the form
 *
 * <pre>
 * value FIELD:yyyyMMdd_shard0x00datatype (uid list)
 * </pre>
 *
 * or a truncated shard index key in the form
 *
 * <pre>
 * value FIELD:yyyyMMdd0x00datatype (bitset offset)
 * </pre>
 *
 * to a sharded day index key
 *
 * <pre>
 * yyyyMMdd0x00value FIELD:datatype (bitset offset)
 * </pre>
 */
public class ShardedDayIndexKeyParser extends AbstractIndexKeyParser {

    @Override
    public Key convert() {
        if (isShardedDayKey()) {
            return key; // pass-through
        }

        String nextRow = getDate() + NULL_CHAR + getValue();
        return new Key(nextRow, getField(), getDatatype(), key.getColumnVisibilityParsed(), key.getTimestamp());
    }

    public BitSet getBitset() {
        if (isTruncatedKey() || isShardedDayKey() || isShardedYearKey()) {
            // pass-through for truncated or sharded keys
            return null;
        }

        if (bitset == null && isStandardKey()) {
            String shardNumber = cq.substring(cqUnderscoreIndex + 1, cqNullIndex);
            int num = Integer.parseInt(shardNumber);
            bitset = new BitSet();
            bitset.set(num);
        }
        return bitset;
    }
}
