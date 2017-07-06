package datawave.query.iterator;

import java.io.IOException;
import java.util.Collection;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;

/**
 * 
 */
public interface SeekableIterator {
    /**
     * @see org.apache.accumulo.core.iterators.SortedKeyValueIterator
     * @param range
     * @param columnFamilies
     * @param inclusive
     * @throws IOException
     */
    void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException;
}
