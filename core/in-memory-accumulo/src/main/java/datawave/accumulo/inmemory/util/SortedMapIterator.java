package datawave.accumulo.inmemory.util;

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

    private final SortedMap<Key,Value> map;
    private Iterator<Entry<Key,Value>> iter;
    private Entry<Key,Value> entry;
    private Range range;
    private AtomicBoolean interruptFlag;
    private int interruptCheckCount = 0;

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
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void next() throws IOException {
        if (entry == null) {
            throw new IllegalStateException();
        }

        // checking the interrupt flag on every call to next is costly, so as Accumulo does, check it on every 100th call
        if (interruptFlag != null && interruptCheckCount++ % 100 == 0 && interruptFlag.get()) {
            throw new IterationInterruptedException();
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
            throw new IterationInterruptedException();
        }

        this.range = range;

        Key key = range.getStartKey();
        if (key == null) {
            key = new Key();
        }

        iter = map.tailMap(key).entrySet().iterator();
        if (iter.hasNext()) {
            entry = iter.next();
            if (range.afterEndKey(entry.getKey())) {
                entry = null;
            }
        } else {
            entry = null;
        }

        // the tail map includes the start key, so move past any entries the range excludes, such as an exclusive start key
        while (hasTop() && range.beforeStartKey(getTopKey())) {
            next();
        }
    }

    /**
     * Set a flag that interrupts this iterator. While the flag is set, seek() throws an {@link IterationInterruptedException}. next() throws one too, but like
     * Accumulo's SortedMapIterator it only checks the flag on every 100th call, starting with the first. Copies made by deepCopy() after this call share the
     * flag.
     *
     * @param flag
     *            the interrupt flag
     */
    public void setInterruptFlag(AtomicBoolean flag) {
        this.interruptFlag = flag;
    }
}
