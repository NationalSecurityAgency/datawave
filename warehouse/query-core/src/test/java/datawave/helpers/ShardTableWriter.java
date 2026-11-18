package datawave.helpers;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * A {@link TableWriter} implementation for a shard table.
 */
public class ShardTableWriter extends TableWriter {

    @Override
    public String formatTimestamp(long timestamp) {
        return PrintUtility.formatCompositeTimestamp(timestamp);
    }

    @Override
    public String formatValue(Key key, Value value) {
        if (value == null || value.getSize() < 1) {
            return "";
        }

        String colFam = key.getColumnFamily().toString();
        switch (colFam) {
            case "tf":
                // This is a term frequency row.
                try {
                    return PrintUtility.decodeTermWeightInfo(value);
                } catch (Exception e) {
                    log.error("Failed to decode term weight for key: {}", key, e);
                    return UNDECODED_VALUE;
                }
            case "d":
                // This is a compressed and encoded document row.
                try {
                    return PrintUtility.decodeDocument(value);
                } catch (Exception e) {
                    log.error("Failed to decode document for key: {}", key, e);
                    return UNDECODED_VALUE;
                }
            default:
                return value.toString();
        }
    }
}
