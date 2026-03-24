package datawave.test.util;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * A simple SortedKeyValueIterator implementation that wraps a SortedMap. This is a DataWave replacement for the non-public Accumulo
 * org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator class, used for testing iterator logic without needing a full Accumulo instance.
 */
public class SortedMapIterator implements SortedKeyValueIterator<Key,Value> {

    private SortedMap<Key,Value> map;
    private Iterator<Entry<Key,Value>> iter;
    private Entry<Key,Value> entry;
    private Range range;
    protected AtomicBoolean interruptFlag;

    public SortedMapIterator(SortedMap<Key,Value> map) {
        this.map = map;
        this.iter = null;
        this.range = new Range();
        this.entry = null;
    }

    public SortedMapIterator(SortedMapIterator other, IteratorEnvironment env) {
        this.map = new TreeMap<>(other.map);
        this.iter = null;
        this.range = new Range();
        this.entry = null;
        if (other.interruptFlag != null) {
            this.interruptFlag = other.interruptFlag;
        }
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new SortedMapIterator(this, env);
    }

    @Override
    public Key getTopKey() {
        return entry.getKey();
    }

    @Override
    public Value getTopValue() {
        return entry.getValue();
    }

    @Override
    public boolean hasTop() {
        return entry != null;
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void next() throws IOException {
        if (entry == null) {
            throw new IllegalStateException();
        }

        if (interruptFlag != null && interruptFlag.get()) {
            throw new IOException("interrupted");
        }

        if (iter.hasNext()) {
            entry = iter.next();
            if (range.afterEndKey(entry.getKey())) {
                entry = null;
            }
        } else {
            entry = null;
        }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        if (interruptFlag != null && interruptFlag.get()) {
            throw new IOException("interrupted");
        }

        this.range = range;

        Key startKey = range.getStartKey();
        if (startKey == null) {
            startKey = new Key();
        }

        // Get the tailMap starting from the start key
        SortedMap<Key,Value> tailMap = map.tailMap(startKey);
        iter = tailMap.entrySet().iterator();

        // If the range is not start key inclusive, skip the first entry if it equals the start key
        if (iter.hasNext()) {
            entry = iter.next();
            if (!range.isStartKeyInclusive() && entry.getKey().equals(startKey)) {
                if (iter.hasNext()) {
                    entry = iter.next();
                } else {
                    entry = null;
                }
            }
            if (entry != null && range.afterEndKey(entry.getKey())) {
                entry = null;
            }
        } else {
            entry = null;
        }
    }

    /**
     * Set an interrupt flag that will cause next() and seek() to throw an IOException if set.
     *
     * @param flag
     *            the interrupt flag
     */
    public void setInterruptFlag(AtomicBoolean flag) {
        this.interruptFlag = flag;
    }
}
