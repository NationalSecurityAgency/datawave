package datawave.iterators;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

/**
 *
 * A Java Iterator that iterates over the Values for a given Key from a source SortedKeyValueIterator.
 */
public class ValueCombiner implements Iterator<Value> {
    Key topKey;
    SortedKeyValueIterator<Key,Value> source;
    boolean hasNext;
    PartialKey keysToCombine = PartialKey.ROW_COLFAM_COLQUAL_COLVIS;

    private static final Logger log = Logger.getLogger(ValueCombiner.class);

    /**
     * Constructs an iterator over Values whose Keys are versions of the current topKey of the source SortedKeyValueIterator.
     *
     * @param source
     *            The {@code SortedKeyValueIterator<Key,Value>} from which to read data.
     */
    public ValueCombiner(SortedKeyValueIterator<Key,Value> source) {
        this(source, PartialKey.ROW_COLFAM_COLQUAL_COLVIS);
    }

    public ValueCombiner(SortedKeyValueIterator<Key,Value> source, PartialKey keysToCombine) {
        this.source = source;
        this.keysToCombine = keysToCombine;
        topKey = new Key(source.getTopKey());
        hasNext = _hasNext();
    }

    private boolean _hasNext() {
        if (log.isTraceEnabled()) {
            log.trace("source hastop ? " + source.hasTop() + " " + topKey);
            if (source.hasTop())
                log.trace(source.getTopKey());

        }
        return source.hasTop() && topKey.equals(source.getTopKey(), keysToCombine);
    }

    /**
     * @return <code>true</code> if there is another Value
     *
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext() {
        return hasNext;
    }

    /**
     * @return the next Value
     *
     * @see java.util.Iterator#next()
     */
    @Override
    public Value next() {
        if (!hasNext)
            throw new NoSuchElementException();
        Value topValue = new Value(source.getTopValue());
        try {
            source.next();
            hasNext = _hasNext();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return topValue;
    }

    /**
     * unsupported
     *
     * @see java.util.Iterator#remove()
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
