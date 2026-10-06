package datawave.helpers;

import static datawave.helpers.PrintUtility.decodeBitset;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * A {@link TableWriter} implementation for a date index table.
 */
public class DateIndexTableWriter extends TableWriter {

    @Override
    protected String formatValue(Key key, Value value) {
        if (value == null || value.getSize() < 1) {
            return "";
        }

        try {
            return decodeBitset(value);
        } catch (Exception e) {
            log.error("Failed to decode bitset for key: {}", key, e);
            return UNDECODED_VALUE;
        }
    }
}
