package datawave.webservice.query.iterator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.collections4.Transformer;
import org.apache.commons.collections4.iterators.TransformIterator;

/**
 * A {@link TransformIterator} that expects to receive {@link Key}/{@link Value} pairs as its input, and aggregates all entries with the same key into a list
 * before passing that along to the contained iterator.
 */
public class KeyAggregatingTransformIterator extends TransformIterator {
    private Comparator<Key> comparator;
    private Entry<Key,Value> nextEntry = null;

    /**
     * Constructs a new {@link KeyAggregatingTransformIterator} that iterates over {@code iterator} and transforms the aggregated entries using
     * {@code transformer}. For the purposes of determining whether or not two keys are the same, only the row portion is considered.
     *
     * @param iterator
     *            the iterator to use
     * @param transformer
     *            the transformer to use
     */
    public KeyAggregatingTransformIterator(Iterator<?> iterator, Transformer transformer) {
        this(PartialKey.ROW, iterator, transformer);
    }

    /**
     * Constructs a new {@link KeyAggregatingTransformIterator} that iterates over {@code iterator} and transforms the aggregated entries using
     * {@code transformer}. Keys in an entry are considered equal if they match according to {@code partial}
     *
     * @param partial
     *            the portion of the key to compare for determining whether or not to aggregate
     * @param iterator
     *            the iterator to use
     * @param transformer
     *            the transformer to use
     */
    public KeyAggregatingTransformIterator(PartialKey partial, Iterator<?> iterator, Transformer transformer) {
        this(new PartialComparator(partial), iterator, transformer);
    }

    /**
     * Constructs a new {@link KeyAggregatingTransformIterator} that iterates over {@code iterator} and transforms the aggregated entries using
     * {@code transformer}. Keys in an entry are considered to be equal if they match according to {@code comparator}.
     *
     * @param comparator
     *            the comparator to use when determining whether or not to aggregate
     * @param iterator
     *            the iterator to use
     * @param transformer
     *            the transformer to use
     */
    public KeyAggregatingTransformIterator(Comparator<Key> comparator, Iterator<?> iterator, Transformer transformer) {
        super(iterator, transformer);
        this.comparator = comparator;
    }

    @Override
    public boolean hasNext() {
        return nextEntry != null || getIterator().hasNext();
    }

    @Override
    public Object next() {

        ArrayList<Entry<Key,Value>> entries = new ArrayList<>();

        // Add all entries from the source iterator that share the same key
        // (up to the comparison point specified in the member variable partial)
        // Save the entry we read last when we find a key that doesn't match, so
        // we know to use that first on the next pass.
        Entry<Key,Value> prevEntry = nextEntry();
        while (prevEntry != null) {
            entries.add(prevEntry);

            Entry<Key,Value> entry = nextEntry();
            if (entry == null || comparator.compare(prevEntry.getKey(), entry.getKey()) != 0) {
                nextEntry = entry;
                break;
            }
            prevEntry = entry;
        }

        return transform(entries);
    }

    @SuppressWarnings("unchecked")
    protected Entry<Key,Value> nextEntry() {
        // Use the saved next entry, if there is one. Otherwise,
        // return the next entry off the source iterator if there is one.
        Entry<Key,Value> next = nextEntry;
        if (next == null && getIterator().hasNext()) {
            Object obj = getIterator().next();
            if (!(obj instanceof Entry))
                throw new IllegalArgumentException("Unsupported input type " + (obj == null ? "null" : obj.getClass()) + " is not an Entry");
            next = (Entry<Key,Value>) obj;
        }
        nextEntry = null;
        return next;
    }

    public static class PartialComparator implements Comparator<Key> {
        private PartialKey partial;

        public PartialComparator(PartialKey partial) {
            this.partial = partial;
        }

        @Override
        public int compare(Key k1, Key k2) {
            return k1.compareTo(k2, partial);
        }
    }
}
