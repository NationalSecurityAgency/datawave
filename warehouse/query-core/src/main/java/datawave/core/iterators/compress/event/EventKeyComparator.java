package datawave.core.iterators.compress.event;

import java.util.Comparator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;

/**
 * Compressed documents must group keys based on the following criteria
 * <ol>
 * <li>row: same shard</li>
 * <li>column family: same document id</li>
 * <li>column visibility: serializing visibility once is significant</li>
 * <li>timestamp: to handle updates to documents</li>
 * <li>delete flag: critical to separate delete markers</li>
 * </ol>
 */
public class EventKeyComparator implements Comparator<Key> {

    @Override
    public int compare(Key left, Key right) {
        int result = left.compareTo(right, PartialKey.ROW_COLFAM);
        if (result != 0) {
            return result;
        }

        // do not compare the event key's ColumnQualifier

        result = left.getColumnVisibility().compareTo(right.getColumnVisibility());
        if (result != 0) {
            return result;
        }

        // the accumulo key comparator inverts the comparator convention so the latest key is first
        result = Long.compare(right.getTimestamp(), left.getTimestamp());
        if (result != 0) {
            return result;
        }

        return Boolean.compare(left.isDeleted(), right.isDeleted());
    }
}
