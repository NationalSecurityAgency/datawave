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
     *            the range
     * @param columnFamilies
     *            column families
     * @param inclusive
     *            flag for if the range is inclusive
     * @throws IOException
     *             for issues with read/write
     */
    void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException;
}
