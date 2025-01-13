package datawave.query.iterator;

import java.io.IOException;
import java.util.Collection;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;

/**
 * See {@link NestedIterator#seek(Range, Collection, boolean)} for examples of how this interface was previously used.
 */
@Deprecated(forRemoval = true, since = "7.13.0")
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
