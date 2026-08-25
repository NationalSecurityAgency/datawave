package datawave.helpers;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * A {@link TableWriter} implementation for an annotation source table.
 */
public class AnnotationSourceTableWriter extends TableWriter {

    @Override
    protected String formatValue(Key key, Value value) {
        if (value == null || value.getSize() < 1) {
            return "";
        }

        try {
            return PrintUtility.decodeAnnotationSource(value);
        } catch (Exception e) {
            log.error("Failed to decode annotation source for key: {}", key, e);
            return UNDECODED_VALUE;
        }
    }
}
