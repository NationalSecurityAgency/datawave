package datawave.ingest.table.aggregator.util;

import org.apache.accumulo.core.data.Key;

/**
 * A key parser for shard index keys that supports conversion between index key types, i.e.
 * <ul>
 * <li>standard shard index key</li>
 * <li>truncated shard index key</li>
 * <li>sharded day index key</li>
 * <li>sharded year index key</li>
 * </ul>
 */
public interface IndexKeyParser {

    boolean isStandardKey();

    boolean isTruncatedKey();

    boolean isShardedDayKey();

    boolean isShardedYearKey();

    Key convert();
}
