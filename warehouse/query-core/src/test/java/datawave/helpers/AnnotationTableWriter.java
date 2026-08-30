package datawave.helpers;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * A {@link TableWriter} for an annotation table.
 */
public class AnnotationTableWriter extends TableWriter {

    @Override
    protected String formatValue(Key key, Value value) {
        if (value == null || value.getSize() < 1) {
            return "";
        }

        try {
            return PrintUtility.decodeAnnotationSegment(value);
        } catch (Exception e) {
            log.error("Failed to decode annotation segment for key: {}", key, e);
            return UNDECODED_VALUE;
        }
    }
}
