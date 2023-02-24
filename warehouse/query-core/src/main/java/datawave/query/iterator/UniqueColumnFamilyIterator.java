package datawave.query.iterator;

import java.io.IOException;
import java.util.Collection;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

/**
 * For a given scan range, this iterator will return the first key in a {@code <row, column family>}. The sequence of keys output from this iterator will
 * therefore always contain a unique {@code <row, column family>} tuple.
 *
 */
public class UniqueColumnFamilyIterator extends org.apache.accumulo.core.iterators.WrappingIterator {
    private Range scanRange;
    private Collection<ByteSequence> cfs;
    private boolean inclusive;
    private static final Logger log = Logger.getLogger(UniqueColumnFamilyIterator.class);
    
    @Override
    public UniqueColumnFamilyIterator deepCopy(IteratorEnvironment env) {
        UniqueColumnFamilyIterator i = new UniqueColumnFamilyIterator();
        i.setSource(this.getSource().deepCopy(env));
        return i;
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> cfs, boolean inclusive) throws IOException {
        this.scanRange = range;
        this.cfs = cfs;
        this.inclusive = inclusive;
        super.seek(range, cfs, inclusive);
    }
    
    @Override
    public void next() throws IOException {
        if (getSource().hasTop()) {
            Key next = getSource().getTopKey().followingKey(PartialKey.ROW_COLFAM);
            moveTo(next, getSource(), scanRange, cfs, inclusive);
        }
    }
    
    /**
     * A bit of a hack, similar to the ColumnFamilySkippingIterator. This will call next a 32 times before finally seeking for the next {@code <row, colf>}.
     * 
     * The source iterator may or may not have a top after this method returns, and there is no guarantee of another viable top key/value being set.
     * 
     * @param key
     *            a key
     * @param iterator
     *            an iterator
     * @param cfs
     *            column families
     * @param scanRange
     *            the scan range
     * @param inclusive
     *            whether the range is considered inclusive
     * @throws IOException
     *             for issues with read/write
     */
    public static void moveTo(Key key, SortedKeyValueIterator<Key,Value> iterator, Range scanRange, Collection<ByteSequence> cfs, boolean inclusive)
                    throws IOException {
        int nexts = 0;
        boolean movedEnough = false;
        if (log.isTraceEnabled()) {
            log.trace("Iterator key is " + key);
            log.trace("Iterator has top?" + iterator.hasTop());
            if (iterator.hasTop())
                log.trace("Iterator top key?" + iterator.getTopKey());
            log.trace("ScanRange is ?" + scanRange);
            log.trace("Scan cfs are " + cfs + " is inclusive ? " + inclusive);
        }
        while (nexts < 32 && iterator.hasTop() && (movedEnough = iterator.getTopKey().compareTo(key) < 0)) {
            iterator.next();
            nexts++;
        }
        if (log.isTraceEnabled()) {
            log.trace("Nexts is " + nexts);
            log.trace("Moved enough? " + movedEnough);
        }
        if (!movedEnough && iterator.hasTop()) {
            iterator.seek(new Range(key, true, scanRange.getEndKey(), scanRange.isEndKeyInclusive()), cfs, inclusive);
        }
    }
}
