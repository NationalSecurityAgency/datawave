package datawave.ingest.table.aggregator.util;

import java.nio.charset.StandardCharsets;
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
@Deprecated(forRemoval = true, since = "7.40.0")
public class ShardedDayIndexKeyParser extends AbstractIndexKeyParser {

    @Override
    public Key convert() {
        if (isShardedDayKey()) {
            return key; // pass-through
        }

        // use a byte array constructor to avoid expensive parsing of the ColumnVisibility
        byte[] row = (getDate() + NULL_CHAR + getValue()).getBytes(StandardCharsets.UTF_8);
        byte[] cf = getField().getBytes(StandardCharsets.UTF_8);
        byte[] cq = getDatatype().getBytes(StandardCharsets.UTF_8);
        byte[] cv = key.getColumnVisibilityData().toArray();
        return new Key(row, cf, cq, cv, key.getTimestamp());
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
