package datawave.query.util;

import java.io.IOException;
import java.util.Collection;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;

/**
 * Extension of the {@link DateIndexIterator} that tracks the next and seek count
 */
public class StatEnabledDateIndexIterator extends DateIndexIterator {

    private int next = 0;
    private int seek = 0;

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        seek++;
        super.seek(range, columnFamilies, inclusive);
    }

    @Override
    public void next() throws IOException {
        next++;
        super.next();
    }

    public int getNextCount() {
        return next;
    }

    public int getSeekCount() {
        return seek;
    }
}
