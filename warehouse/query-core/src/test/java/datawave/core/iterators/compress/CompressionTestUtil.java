package datawave.core.iterators.compress;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.apache.commons.lang3.tuple.Pair;

import datawave.data.hash.UID;

/**
 * Utility methods for event compression and field index compression tests.
 */
public class CompressionTestUtil {

    private static final Value EMPTY_VALUE = new Value();

    /**
     * Create a {@link UID} using the provided integer as the backing data
     *
     * @param n
     *            an integer
     * @return a UID
     */
    public static String uid(int n) {
        String data = "uid-" + n;
        return UID.builder().newId(data.getBytes(), (Date) null).toString();
    }

    /**
     * Convenience method that copies and sorts input keys, useful when keys contain a generated UID
     *
     * @param keys
     *            the input keys
     * @return a sorted copy
     */
    public static List<Key> list(Key... keys) {
        List<Key> copy = new ArrayList<>(Arrays.asList(keys));
        Collections.sort(copy);
        return copy;
    }

    /**
     * Convenience method that copies and sorts input keys, useful when keys contain a generated UID
     *
     * @param keys
     *            the input keys
     * @return a sorted copy
     */
    public static List<Key> list(Set<Key> keys) {
        List<Key> copy = new ArrayList<>(keys);
        Collections.sort(copy);
        return copy;
    }

    /**
     * Copy and shuffle the provided keys
     *
     * @param keys
     *            a list of keys
     * @return a shuffled copy
     */
    public static List<Key> shuffle(List<Key> keys) {
        List<Key> shuffled = new ArrayList<>(keys);
        Collections.shuffle(shuffled);
        return shuffled;
    }

    /**
     * Create a {@link SortedMapIterator} from the input key list
     *
     * @param keys
     *            the input keys
     * @return a {@link SortedKeyValueIterator}
     */
    public static SortedKeyValueIterator<Key,Value> iterator(List<Key> keys) throws IOException {
        SortedMap<Key,Value> data = toSortedMap(keys);
        SortedKeyValueIterator<Key,Value> iter = new SortedMapIterator(data);
        iter.seek(new Range(), Collections.emptySet(), false);
        return iter;
    }

    /**
     * Get an iterator for the provided data
     *
     * @param data
     *            a sorted map
     * @return a {@link SortedKeyValueIterator}
     */
    public static SortedKeyValueIterator<Key,Value> iterator(SortedMap<Key,Value> data) throws IOException {
        SortedKeyValueIterator<Key,Value> iter = new SortedMapIterator(data);
        iter.seek(new Range(), Collections.emptySet(), false);
        return iter;
    }

    /**
     * Get an iterator for the provided data
     *
     * @param data
     *            a list of key value pairs
     * @return a {@link SortedKeyValueIterator}
     */
    public static SortedKeyValueIterator<Key,Value> iterator(Pair<Key,Value>... data) throws IOException {
        SortedMap<Key,Value> map = toSortedMapFromPairs(List.of(data));
        return iterator(map);
    }

    /**
     * Create a {@link SortedMap} with {@link #EMPTY_VALUE} from the provided keys
     *
     * @param keys
     *            the input keys
     * @return a sorted map
     */
    public static SortedMap<Key,Value> toSortedMap(List<Key> keys) {
        SortedMap<Key,Value> data = new TreeMap<>();
        keys.forEach(key -> data.put(key, EMPTY_VALUE));
        return data;
    }

    /**
     * Create a {@link SortedMap} with {@link #EMPTY_VALUE} from the provided key value pairs
     *
     * @param pairs
     *            the input keys
     * @return a sorted map
     */
    public static SortedMap<Key,Value> toSortedMapFromPairs(List<Pair<Key,Value>> pairs) {
        SortedMap<Key,Value> data = new TreeMap<>();
        for (Pair<Key,Value> pair : pairs) {
            data.put(pair.getKey(), pair.getValue());
        }
        return data;
    }

    /**
     * Drive an iterator and store the results in a sorted map
     *
     * @param iter
     *            the iterator
     * @return the results
     * @throws IOException
     *             if something goes wrong
     */
    public static TreeMap<Key,Value> iterate(SortedKeyValueIterator<Key,Value> iter) throws IOException {
        TreeMap<Key,Value> results = new TreeMap<>();
        while (iter.hasTop()) {
            results.put(iter.getTopKey(), iter.getTopValue());
            iter.next();
        }
        return results;
    }
}
