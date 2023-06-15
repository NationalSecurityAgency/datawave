package datawave.query.ancestor;

import datawave.query.data.parsers.DatawaveKey;
import datawave.query.function.Equality;
import datawave.query.Constants;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * <p>
 * Create a single pass iterator that takes an underlying fi key iterator and a lexicographically sorted list of children and using equality generate fi keys
 * for all children that were not actually selected but should be based on the ancestor having the field.
 * </p>
 *
 * <p>
 * Iterator should return all keys in sorted order while only evaluating each key one time. Simulated keys will have the same visibility and timestamp as the
 * parent that it was based upon.
 * </p>
 *
 * <p>
 * This type of iterator is necessary specifically for index only fields since the later evaluation of the document has no way to know that the document didn't
 * really include the term.
 * </p>
 */
public class AncestorChildExpansionIterator implements SortedKeyValueIterator<Key,Value> {
    private final List<String> children;
    private final Equality equality;

    private SortedKeyValueIterator<Key,Value> iterator;
    private BaseIteratorInfo iteratorKeyInfo;
    private Key topKey;
    private Value topValue;
    private int lastChildIndex;
    private boolean initialized = false;
    private boolean seeked = false;

    /**
     *
     * @param iterator
     *            the base iterator to expand
     * @param children
     *            lexographically sorted list of children
     * @param equality
     *            the equality to use to compare children
     */
    public AncestorChildExpansionIterator(SortedKeyValueIterator<Key,Value> iterator, List<String> children, Equality equality) {
        this.iterator = iterator;
        this.children = children;
        this.equality = equality;
        lastChildIndex = -1;
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        this.iterator = source;
        reset();
    }

    @Override
    public boolean hasTop() {
        if (!seeked) {
            throw new IllegalStateException("cannot hasTop(), iterator has not been seeked");
        }

        if (!initialized) {
            nextChild();
        }

        return topKey != null;
    }

    @Override
    public void next() throws IOException {
        if (!seeked) {
            throw new IllegalStateException("cannot next(), iterator has not been seeked");
        }

        if (initialized && topKey == null) {
            throw new NoSuchElementException("cannot next(), iterator is empty");
        }

        nextChild();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        // seek the underlying range
        iterator.seek(range, columnFamilies, inclusive);

        // clear the last values to re-initialize
        reset();

        seeked = true;
    }

    @Override
    public Key getTopKey() {
        if (!seeked) {
            throw new IllegalStateException("cannot getTopKey(), iterator has not been seeked");
        }

        if (topKey == null) {
            throw new NoSuchElementException("top key does not exist");
        }

        return topKey;
    }

    @Override
    public Value getTopValue() {
        if (!seeked) {
            throw new IllegalStateException("cannot getTopKey(), iterator has not been seeked");
        }

        if (topValue == null) {
            throw new NoSuchElementException("top value does not exist");
        }

        return topValue;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        final SortedKeyValueIterator<Key,Value> internalCopy = iterator.deepCopy(env);
        return new AncestorChildExpansionIterator(internalCopy, children, equality);
    }

    /**
     * Reset internal state
     */
    private void reset() {
        lastChildIndex = -1;
        topKey = null;
        topValue = null;
        initialized = false;
    }

    /**
     * Update topKey and topValue to point to the next valid fi key either directly from the underlying iterator, or if a child exists that was not on the
     * iterator, replicate the fi for the child until no more children match, then move on to the next item on the iterator that is after the current key,
     * advancing both iterators until they have been exhausted. There should only ever be one pass made through either iterator.
     *
     */
    private void nextChild() {
        // an attempt has been made to reach the topKey
        initialized = true;

        // decide if starting from topKey or the first item in the iterator if topKey has never been assigned
        if (topKey == null && iterator.hasTop()) {
            final Key iteratorTop = iterator.getTopKey();
            final DatawaveKey datawaveKey = new DatawaveKey(iteratorTop);
            iteratorKeyInfo = new BaseIteratorInfo(iteratorTop, datawaveKey, getPartOfKey(iteratorTop, datawaveKey.getDataType(), datawaveKey.getUid(),
                            datawaveKey.getFieldName(), datawaveKey.getFieldValue()));
            topKey = iteratorKeyInfo.getKey();
            topValue = iterator.getTopValue();

            return;
        }

        // if there is nothing in the iterator, bail out
        if (topKey == null) {
            return;
        }

        // parse out components to generate new keys
        final DatawaveKey datawaveKey = iteratorKeyInfo.getDatawaveKey();
        final String uidHit = datawaveKey.getUid();
        final String field = datawaveKey.getFieldName();
        final String value = datawaveKey.getFieldValue();
        final String dataType = datawaveKey.getDataType();

        // find the next suitable top key, either from the children or the underlying iterator. The goal is to make only
        // a single pass through the children and the iterator unless there is a seek. This is possible because the children
        // are assumed to be lexicographically sorted
        Key next = null;
        for (int i = lastChildIndex + 1; next == null && i < children.size(); i++) {
            final String child = children.get(i);
            // Only evaluate children that come after the uid of the iterator.
            if (uidHit.compareTo(child) < 0) {
                // test for equality generate keys that match the shard event keys for testing
                final Key iteratorTestKey = iteratorKeyInfo.getPartOfKey();
                final Key childTestKey = getPartOfKey(topKey, dataType, child, field, value);
                if (equality.partOf(childTestKey, iteratorTestKey)) {
                    // generate an fi matching the previous key but with the new uid
                    next = new Key(topKey.getRow().toString(), topKey.getColumnFamily().toString(),
                                    value + Constants.NULL_BYTE_STRING + dataType + Constants.NULL_BYTE_STRING + child, topKey.getColumnVisibilityParsed(),
                                    topKey.getTimestamp());
                } else {
                    // since the child came after the current topKey but was not a child, the next top will have to come off
                    // of the iterator
                    next = advanceBaseIterator();
                }
            }

            // no need to evaluate this child again since everything is sorted
            lastChildIndex++;
        }

        // if the next top key hasn't been found, but we ran out of children double check that the iterator is empty
        // there should be a direct relationship between the children and parent, but do this as a safety check
        if (next == null && iterator.hasTop()) {
            next = advanceBaseIterator();
        }

        // assign the next Key to the topKey
        topKey = next;
    }

    /**
     * Attempt to advance the base iterator beyond the current topKey
     *
     * @return the first item from the iterator that is beyond the current topKey, or null
     * @throws RuntimeException
     *             if an iterator fails to advance
     */
    private Key advanceBaseIterator() {
        Key next = null;

        // set the next returned key to the value from the iterator as long as there are more and its less than the current key
        try {
            while (iterator.hasTop() && iterator.getTopKey().compareTo(topKey) <= 0) {
                iterator.next();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to advance base iterator");
        }

        // as long as we didn't exhaust the iterator looking for a key following the topKey grab it and set it to the top
        if (iterator.hasTop()) {
            // there was another value on the iterator, keep going
            final Key iteratorTop = iterator.getTopKey();
            final DatawaveKey datawaveKey = new DatawaveKey(iteratorTop);
            iteratorKeyInfo = new BaseIteratorInfo(iteratorTop, datawaveKey, getPartOfKey(iteratorTop, datawaveKey.getDataType(), datawaveKey.getUid(),
                            datawaveKey.getFieldName(), datawaveKey.getFieldValue()));
            next = iteratorKeyInfo.getKey();
            topValue = iterator.getTopValue();
        } else {
            // if there is not a current top, there is no other top, so we have reached the end. Set the lastChildIndex
            // so that subsequent calls won't have to determine this a second time and bail out
            lastChildIndex = children.size();
        }

        return next;
    }

    /**
     * Generate an accurate key for use in the equality tests
     *
     * @param key
     *            a key
     * @param dataType
     *            a data type
     * @param uid
     *            the uid
     * @param field
     *            the field
     * @param value
     *            the value
     * @return a key
     */
    protected Key getPartOfKey(Key key, String dataType, String uid, String field, String value) {
        return new Key(key.getRow().toString(), dataType + Constants.NULL_BYTE_STRING + uid, field + Constants.NULL_BYTE_STRING + value);
    }

    /**
     * Store this information for easy retrieval
     */
    private static class BaseIteratorInfo {
        private final Key key;
        private final DatawaveKey datawaveKey;
        private final Key partOfKey;

        public BaseIteratorInfo(Key key, DatawaveKey datawaveKey, Key partOfKey) {
            this.key = key;
            this.datawaveKey = datawaveKey;
            this.partOfKey = partOfKey;
        }

        public Key getKey() {
            return key;
        }

        public DatawaveKey getDatawaveKey() {
            return datawaveKey;
        }

        public Key getPartOfKey() {
            return partOfKey;
        }
    }
}
