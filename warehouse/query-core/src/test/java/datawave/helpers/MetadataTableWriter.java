package datawave.helpers;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import datawave.iterators.FrequencyMetadataAggregator;

/**
 * A {@link TableWriter} implementation for a datawave metadata table.
 */
public class MetadataTableWriter extends TableWriter {

    private static final String NULL_BYTE = "\0";

    @Override
    protected String formatValue(Key key, Value value) {
        if (value == null || value.getSize() < 1) {
            return "";
        }

        String columnFamily = key.getColumnFamily().toString();
        switch (columnFamily) {
            case "f":
            case "i":
            case "ri":
                String colQualifier = key.getColumnQualifier().toString();
                int separatorPos = colQualifier.indexOf(NULL_BYTE);
                String remainder = (separatorPos == -1 ? "" : colQualifier.substring((separatorPos + 1)));
                if (remainder.equalsIgnoreCase(FrequencyMetadataAggregator.AGGREGATED)) {
                    try {
                        return PrintUtility.decodeDateFrequencyMap(value);
                    } catch (Exception e) {
                        log.error("Failed to decode date frequency map for key: {}", key, e);
                        return UNDECODED_VALUE;
                    }
                } else {
                    try {
                        return PrintUtility.decodeLong(value);
                    } catch (Exception e) {
                        log.error("Failed to decode frequency value for key: {}", key, e);
                        return UNDECODED_VALUE;
                    }
                }
            case "edge":
                try {
                    return PrintUtility.decodeEdgeMetadata(value);
                } catch (Exception e) {
                    log.error("Failed to decode edge metadata value for key: {}", key, e);
                    return UNDECODED_VALUE;
                }

            default:
                return value.toString();
        }
    }
}
