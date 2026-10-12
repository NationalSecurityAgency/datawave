package datawave.helpers;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * A {@link TableWriter} implementation for a shard index and shard rindex tables.
 */
public class ShardIndexTableWriter extends TableWriter {

    @Override
    protected String formatTimestamp(long timestamp) {
        return PrintUtility.formatCompositeTimestamp(timestamp);
    }

    @Override
    protected String formatValue(Key key, Value value) {
        if (value == null || value.getSize() < 1) {
            return "";
        }

        try {
            // This is a UID list.
            return PrintUtility.decodeUidList(value);
        } catch (Exception e) {
            log.error("Failed to decode UID list for key: {}", key, e);
            return UNDECODED_VALUE;
        }
    }
}
