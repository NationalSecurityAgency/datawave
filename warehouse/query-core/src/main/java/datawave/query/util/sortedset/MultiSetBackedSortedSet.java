package datawave.query.util.sortedset;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.SortedSet;

import org.apache.commons.lang3.builder.EqualsBuilder;

import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

/*
 * This is a sorted set that is backed by multiple underlying sorted sets.  It is assumed that the underlying
 * sorted sets contain the same type of underlying value, and they use the same comparator.  The rewrite
 * strategy will be used if the underlying sorted sets are RewriteableSortedSet implementations.
 */
public class MultiSetBackedSortedSet<E> extends AbstractSet<E> implements RewritableSortedSet<E> {
    protected List<SortedSet<E>> sets = new ArrayList<>();
    protected Comparator<E> comparator = null;
    protected RewriteStrategy<E> rewriteStrategy = null;

    public MultiSetBackedSortedSet() {}

    public MultiSetBackedSortedSet(List<SortedSet<E>> sets) {
        for (SortedSet<E> set : sets) {
            addSet(set);
        }
    }

    public void addSet(SortedSet<E> set) {
        if (sets.isEmpty()) {
            updateConfiguration(set);
        } else {
            verifyConfiguration(set);
        }
        sets.add(set);
    }

    private void updateConfiguration(SortedSet<E> set) {
        comparator = getComparator(set);
        rewriteStrategy = getRewriteStrategy(set);
    }

    private void verifyConfiguration(SortedSet<E> set) {
        if (!(new EqualsBuilder().append(getClass(comparator), getClass(getComparator(set)))
                        .append(getClass(rewriteStrategy), getClass(getRewriteStrategy(set))).isEquals())) {
            throw new IllegalArgumentException("Set being added does not match the comparator and rewriteStrategy of the existing sets");
        }
    }

    private Class getClass(Object obj) {
        return (obj == null ? null : obj.getClass());
    }

    private RewriteStrategy<E> getRewriteStrategy(SortedSet<E> set) {
        if (set instanceof RewritableSortedSet) {
            return ((RewritableSortedSet) set).getRewriteStrategy();
        }
        return null;
    }

    private Comparator<E> getComparator(SortedSet<E> set) {
        return (Comparator<E>) (set.comparator());
    }

    /**
     * Get the underlying sets
     *
     * @return the sets
     */
    public List<SortedSet<E>> getSets() {
        return sets;
    }

    /**
     * Return the size of this set. NOTE that this is somewhat expensive as we require iterating over the sets to determine the true value (see
     * MergeSortIterator);
     */
    @Override
    public int size() {
        int size = 0;
        for (@SuppressWarnings("unused")
        E t : this) {
            size++;
        }
        return size;
    }

    @Override
    public boolean isEmpty() {
        if (sets == null) {
            return true;
        }
        for (SortedSet<E> set : sets) {
            if (set != null && !set.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean contains(Object o) {
        for (SortedSet<E> set : sets) {
            if (set.contains(o)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Iterator<E> iterator() {
        return new MergeSortIterator();
    }

    @Override
    public boolean add(E e) {
        throw new UnsupportedOperationException("Please use addSet to add a sorted set or add this item to one of the existing underlying sets");
    }

    @Override
    public boolean remove(Object o) {
        boolean removed = false;
        for (SortedSet<E> set : sets) {
            if (set.remove(o)) {
                removed = true;
            }
        }
        return removed;
    }

    @Override
    public void clear() {
        for (SortedSet<E> set : this.sets) {
            try {
                set.clear();
            } catch (Exception e) {
                // error clearing sorted set
                // possibility of FileNotFoundException, etc being
                // caught and re-thrown as an exception
            }
        }
        this.sets.clear();
    }

    @Override
    public Comparator<? super E> comparator() {
        return comparator;
    }

    @Override
    public RewritableSortedSet<E> subSet(E fromElement, E toElement) {
        MultiSetBackedSortedSet<E> subSet = new MultiSetBackedSortedSet<>();
        for (SortedSet<E> set : sets) {
            subSet.addSet(set.subSet(fromElement, toElement));
        }
        return subSet;
    }

    @Override
    public RewritableSortedSet<E> headSet(E toElement) {
        MultiSetBackedSortedSet<E> subSet = new MultiSetBackedSortedSet<>();
        for (SortedSet<E> set : sets) {
            subSet.addSet(set.headSet(toElement));
        }
        return subSet;
    }

    @Override
    public RewritableSortedSet<E> tailSet(E fromElement) {
        MultiSetBackedSortedSet<E> subSet = new MultiSetBackedSortedSet<>();
        for (SortedSet<E> set : sets) {
            subSet.addSet(set.tailSet(fromElement));
        }
        return subSet;
    }

    @Override
    public E first() throws NoSuchElementException {
        if (sets == null || sets.isEmpty()) {
            throw new NoSuchElementException("No elements in input sets");
        }
        SortedSet<E> firstSet = new RewritableSortedSetImpl<>(comparator, rewriteStrategy);
        for (SortedSet<E> set : sets) {
            if (set != null && !set.isEmpty()) {
                E s = set.first();
                firstSet.add(s);
            }
        }
        if (firstSet.isEmpty()) {
            throw new NoSuchElementException("No elements in input sets");
        }
        return firstSet.first();
    }

    @Override
    public E last() throws NoSuchElementException {
        if (sets == null || sets.isEmpty()) {
            throw new NoSuchElementException("No elements in input sets");
        }
        SortedSet<E> lastSet = new RewritableSortedSetImpl<>(comparator, rewriteStrategy);
        for (SortedSet<E> set : sets) {
            if (set != null && !set.isEmpty()) {
                E s = set.last();
                lastSet.add(s);
            }
        }
        if (lastSet.isEmpty()) {
            throw new NoSuchElementException("No elements in input sets");
        }
        return lastSet.last();
    }

    @Override
    public RewriteStrategy getRewriteStrategy() {
        return rewriteStrategy;
    }

    @Override
    public E get(E e) {
        return null;
    }

    /**
     * This is an iterator that will return a sorted set of items (no dups) from an underlying set of sorted sets.
     */
    public class MergeSortIterator implements Iterator<E> {

        private List<Iterator<E>> iterators = new ArrayList<>();
        private List<E> lastList = new ArrayList<>();
        private boolean[] finished = null;
        private RewritableSortedSet<E> set = null;
        private boolean populated = false;
        private E next = null;
        private List<Iterator<E>> nextIterators = new ArrayList<>();

        public MergeSortIterator() {
            for (SortedSet<E> set : sets) {
                Iterator<E> it = set.iterator();
                iterators.add(it);
                nextIterators.add(it);
                lastList.add(null);
            }
            this.set = new RewritableSortedSetImpl<>(comparator, rewriteStrategy);
            this.finished = new boolean[iterators.size()];
        }

        @Override
        public boolean hasNext() {
            if (!set.isEmpty()) {
                return true;
            }
            for (Iterator<E> it : nextIterators) {
                if (it != null && it.hasNext()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public E next() {
            populate();
            if (!populated) {
                QueryException qe = new QueryException(DatawaveErrorCode.FETCH_NEXT_ELEMENT_ERROR);
                throw (NoSuchElementException) (new NoSuchElementException().initCause(qe));
            }
            return next;
        }

        @Override
        public void remove() {
            if (!populated) {
                throw new IllegalStateException();
            }
            Exception e = null;
            for (Iterator<E> it : nextIterators) {
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
        private boolean equals(E o1, E o2) {
            if (o1 == null) {
                return o2 == null;
            } else if (o2 == null) {
                return false;
            } else {
                if (set.comparator() == null) {
                    return o1.equals(o2);
                } else {
                    return set.comparator().compare(o1, o2) == 0;
                }
            }
        }

        private void populate() {
            populated = false;

            // update the last value for those iterators contributing to
            // the last returned value
            for (int i = 0; i < nextIterators.size(); i++) {
                if (nextIterators.get(i) != null) {
                    Iterator<E> it = nextIterators.get(i);
                    if (it.hasNext()) {
                        E val = it.next();
                        lastList.set(i, val);
                        set.add(val);
                    } else {
                        lastList.set(i, null);
                        finished[i] = true;
                    }
                }
            }

            if (!set.isEmpty()) {
                next = set.first();
                set.remove(next);
                for (int i = 0; i < iterators.size(); i++) {
                    if (!finished[i] && equals(next, lastList.get(i))) {
                        nextIterators.set(i, iterators.get(i));
                    } else {
                        // if the iterator is finished, or did not contribute to the value being returned
                        // then null it out since the value returned is already in the set to compare
                        // on the next round
                        nextIterators.set(i, null);
                    }
                }
                populated = true;
            }
        }
    }
}
