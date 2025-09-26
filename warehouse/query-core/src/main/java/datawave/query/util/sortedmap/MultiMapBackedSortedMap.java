package datawave.query.util.sortedmap;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.collections4.keyvalue.UnmodifiableMapEntry;
import org.apache.commons.lang3.builder.EqualsBuilder;

import com.google.common.collect.Iterators;

import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

/*
 * This is a sorted map that is backed by multiple underlying sorted maps.  It is assumed that the underlying
 * sorted maps contain the same type of underlying value, and they use the same comparator.  The rewrite
 * strategy will be used if the underlying sorted maps are RewritableSortedMap implementations.
 * @param <K>
 *            key of the map
 * @param <V>
 *            value of the map
 */
public class MultiMapBackedSortedMap<K,V> extends AbstractMap<K,V> implements RewritableSortedMap<K,V> {
    protected List<SortedMap<K,V>> maps = new ArrayList<>();
    protected Comparator<K> comparator = null;
    protected FileSortedMap.RewriteStrategy<K,V> rewriteStrategy = null;

    public MultiMapBackedSortedMap() {}

    public MultiMapBackedSortedMap(List<SortedMap<K,V>> maps) {
        for (SortedMap<K,V> map : maps) {
            addMap(map);
        }
    }

    public void addMap(SortedMap<K,V> map) {
        if (maps.isEmpty()) {
            updateConfiguration(map);
        } else {
            verifyConfiguration(map);
        }
        maps.add(map);
    }

    private void updateConfiguration(SortedMap<K,V> map) {
        comparator = getComparator(map);
        rewriteStrategy = getRewriteStrategy(map);
    }

    private void verifyConfiguration(SortedMap<K,V> map) {
        if (!(new EqualsBuilder().append(getClass(comparator), getClass(getComparator(map)))
                        .append(getClass(rewriteStrategy), getClass(getRewriteStrategy(map))).isEquals())) {
            throw new IllegalArgumentException("map being added does not match the comparator and rewriteStrategy of the existing maps");
        }
    }

    private Class getClass(Object obj) {
        return (obj == null ? null : obj.getClass());
    }

    private FileSortedMap.RewriteStrategy<K,V> getRewriteStrategy(SortedMap<K,V> map) {
        if (map instanceof RewritableSortedMap) {
            return ((RewritableSortedMap) map).getRewriteStrategy();
        }
        return null;
    }

    private Comparator<K> getComparator(SortedMap<K,V> map) {
        return (Comparator<K>) (map.comparator());
    }

    /**
     * Get the underlying maps
     *
     * @return the maps
     */
    public List<SortedMap<K,V>> getMaps() {
        return maps;
    }

    /**
     * Return the size of this map. NOTE that this is somewhat expensive as we require iterating over the maps to determine the true value (see
     * MergeSortIterator);
     */
    @Override
    public int size() {
        return Iterators.size(iterator());
    }

    @Override
    public boolean isEmpty() {
        if (maps == null) {
            return true;
        }
        for (SortedMap<K,V> map : maps) {
            if (map != null && !map.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean containsKey(Object o) {
        for (SortedMap<K,V> map : maps) {
            if (map.containsKey(o)) {
                return true;
            }
        }
        return false;
    }

    protected Iterator<Entry<K,V>> iterator() {
        return new MergeSortIterator();
    }

    @Override
    public V remove(Object o) {
        V value = null;
        for (SortedMap<K,V> map : maps) {
            V testValue = map.remove(o);
            if (testValue != null) {
                if (value != null) {
                    if (rewriteStrategy == null || rewriteStrategy.rewrite((K) o, value, testValue)) {
                        value = testValue;
                    }
                } else {
                    value = testValue;
                }
            }
        }
        return value;
    }

    @Override
    public void clear() {
        for (SortedMap<K,V> map : this.maps) {
            try {
                map.clear();
            } catch (Exception e) {
                // error clearing sorted map
                // possibility of FileNotFoundException, etc being
                // caught and re-thrown as an exception
            }
        }
        this.maps.clear();
    }

    @Override
    public Set<Entry<K,V>> entrySet() {
        return new AbstractSet<>() {

            @Override
            public Iterator<Entry<K,V>> iterator() {
                return MultiMapBackedSortedMap.this.iterator();
            }

            @Override
            public int size() {
                return MultiMapBackedSortedMap.this.size();
            }
        };
    }

    @Override
    public Comparator<? super K> comparator() {
        return comparator;
    }

    @Override
    public RewritableSortedMap<K,V> subMap(K fromElement, K toElement) {
        MultiMapBackedSortedMap<K,V> submap = new MultiMapBackedSortedMap<>();
        submap.setRewriteStrategy(rewriteStrategy);
        for (SortedMap<K,V> map : maps) {
            submap.addMap(map.subMap(fromElement, toElement));
        }
        return submap;
    }

    @Override
    public RewritableSortedMap<K,V> headMap(K toElement) {
        MultiMapBackedSortedMap<K,V> submap = new MultiMapBackedSortedMap<>();
        submap.setRewriteStrategy(rewriteStrategy);
        for (SortedMap<K,V> map : maps) {
            submap.addMap(map.headMap(toElement));
        }
        return submap;
    }

    @Override
    public RewritableSortedMap<K,V> tailMap(K fromElement) {
        MultiMapBackedSortedMap<K,V> submap = new MultiMapBackedSortedMap<>();
        submap.setRewriteStrategy(rewriteStrategy);
        for (SortedMap<K,V> map : maps) {
            submap.addMap(map.tailMap(fromElement));
        }
        return submap;
    }

    @Override
    public K firstKey() throws NoSuchElementException {
        if (maps == null || maps.isEmpty()) {
            throw new NoSuchElementException("No elements in input maps");
        }
        SortedSet<K> firstSet = new TreeSet<>(comparator());
        for (SortedMap<K,V> map : maps) {
            if (map != null && !map.isEmpty()) {
                K s = map.firstKey();
                firstSet.add(s);
            }
        }
        if (firstSet.isEmpty()) {
            throw new NoSuchElementException("No elements in input maps");
        }
        return firstSet.first();
    }

    @Override
    public K lastKey() throws NoSuchElementException {
        if (maps == null || maps.isEmpty()) {
            throw new NoSuchElementException("No elements in input maps");
        }
        SortedSet<K> lastSet = new TreeSet<>(comparator());
        for (SortedMap<K,V> map : maps) {
            if (map != null && !map.isEmpty()) {
                K s = map.lastKey();
                lastSet.add(s);
            }
        }
        if (lastSet.isEmpty()) {
            throw new NoSuchElementException("No elements in input maps");
        }
        return lastSet.last();
    }

    @Override
    public FileSortedMap.RewriteStrategy<K,V> getRewriteStrategy() {
        return rewriteStrategy;
    }

    @Override
    public void setRewriteStrategy(FileSortedMap.RewriteStrategy<K,V> rewriteStrategy) {
        this.rewriteStrategy = rewriteStrategy;
    }

    @Override
    public V get(Object o) {
        V value = null;
        for (SortedMap<K,V> map : maps) {
            V testValue = map.get(o);
            if (testValue != null) {
                if (value != null) {
                    if (rewriteStrategy == null || rewriteStrategy.rewrite((K) o, value, testValue)) {
                        value = testValue;
                    }
                } else {
                    value = testValue;
                }
            }
        }
        return value;
    }

    /**
     * This is an iterator that will return a sorted map of items (no dups) from an underlying map of sorted maps.
     */
    public class MergeSortIterator implements Iterator<Entry<K,V>> {

        // this is the entire set of iterators
        private List<Iterator<Entry<K,V>>> iterators = new ArrayList<>();
        // this is the list of the last key from each of the iterators available to use
        private List<K> lastList = new ArrayList<>();
        // booleans denoting if an iterator has been completely used up
        private boolean[] finished = null;
        // This map holds the key/values to be returned next.
        private SortedMap<K,V> map = null;
        private boolean populated = false;
        private K nextKey = null;
        private V nextValue = null;
        // This is the set of iterators that contributed to the last value returned
        private List<Iterator<Entry<K,V>>> nextIterators = new ArrayList<>();

        public MergeSortIterator() {
            for (SortedMap<K,V> map : maps) {
                Iterator<Entry<K,V>> it = map.entrySet().iterator();
                iterators.add(it);
                nextIterators.add(it);
                lastList.add(null);
            }
            this.map = new TreeMap(comparator);
            this.finished = new boolean[iterators.size()];
        }

        @Override
        public boolean hasNext() {
            if (!map.isEmpty()) {
                return true;
            }
            for (Iterator<Entry<K,V>> it : nextIterators) {
                if (it != null && it.hasNext()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Entry<K,V> next() {
            populate();
            if (!populated) {
                QueryException qe = new QueryException(DatawaveErrorCode.FETCH_NEXT_ELEMENT_ERROR);
                throw (NoSuchElementException) (new NoSuchElementException().initCause(qe));
            }
            return new UnmodifiableMapEntry<>(nextKey, nextValue);
        }

        @Override
        public void remove() {
            if (!populated) {
                throw new IllegalStateException();
            }
            Exception e = null;
            for (Iterator<Entry<K,V>> it : nextIterators) {
                if (it != null) {
                    try {
                        it.remove();
                    } catch (UnsupportedOperationException uoe) {
                        e = uoe;
                    }
                }
            }
            populated = false;
            if (e != null) {
                throw new UnsupportedOperationException("One or more of the underlying sets does not support this operation", e);
            }
        }

        /* Some utility methods */
        private boolean equals(K o1, K o2) {
            if (o1 == null) {
                return o2 == null;
            } else if (o2 == null) {
                return false;
            } else {
                if (map.comparator() == null) {
                    return o1.equals(o2);
                } else {
                    return map.comparator().compare(o1, o2) == 0;
                }
            }
        }

        private void populate() {
            populated = false;

            // update the last value for those iterators contributing to
            // the last returned value
            for (int i = 0; i < nextIterators.size(); i++) {
                if (nextIterators.get(i) != null) {
                    Iterator<Entry<K,V>> it = nextIterators.get(i);
                    if (it.hasNext()) {
                        Entry<K,V> val = it.next();
                        // remember the last key returned
                        lastList.set(i, val.getKey());
                        if ((rewriteStrategy == null) || (!map.containsKey(val.getKey()))
                                        || (rewriteStrategy.rewrite(val.getKey(), map.get(val.getKey()), val.getValue()))) {
                            // update the map if the rewrite policy allows (or a new key)
                            map.put(val.getKey(), val.getValue());
                        }
                    } else {
                        // remember that we are done with this iterator
                        lastList.set(i, null);
                        finished[i] = true;
                    }
                }
            }

            if (!map.isEmpty()) {
                // now get the next key/value from the map
                nextKey = map.firstKey();
                nextValue = map.remove(nextKey);
                // and update the list of iterators that contributed to this next key
                for (int i = 0; i < iterators.size(); i++) {
                    if (!finished[i] && equals(nextKey, lastList.get(i))) {
                        nextIterators.set(i, iterators.get(i));
                    } else {
                        // if the iterator is finished, or did not contribute to the value being returned
                        // then null it out since the value returned is already in the map to compare
                        // on the next round
                        nextIterators.set(i, null);
                    }
                }
                populated = true;
            }
        }
    }
}
