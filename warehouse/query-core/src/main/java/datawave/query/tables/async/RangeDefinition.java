package datawave.query.tables.async;

import java.util.Collection;

import datawave.query.iterator.QueryIterator;
import org.apache.accumulo.core.data.Range;

public class RangeDefinition {
    public static boolean isDocSpecific(Range range) {
        return QueryIterator.isDocumentSpecificRange(range);
    }

    public static boolean allDocSpecific(Collection<Range> ranges) {
        for (Range range : ranges) {
            if (!isDocSpecific(range))
                return false;
        }
        return true;
    }
}
