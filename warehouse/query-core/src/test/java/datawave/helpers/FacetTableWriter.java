package datawave.helpers;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * A {@link TableWriter} for a facet table.
 */
public class FacetTableWriter extends TableWriter {

    @Override
    protected String formatValue(Key key, Value value) {
        if (value == null || value.getSize() < 1) {
            return "";
        }

        try {
            return PrintUtility.decodeHyperLogLogPlusCardinality(value);
        } catch (Exception e) {
            log.error("Failed to decode hyperLogLogPlus cardinality for key: {}", key, e);
            return UNDECODED_VALUE;
        }
    }
}
