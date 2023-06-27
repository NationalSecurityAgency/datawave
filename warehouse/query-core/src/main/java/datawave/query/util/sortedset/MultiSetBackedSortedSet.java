package datawave.query.util.sortedset;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/*
 * This is a sorted set that is backed by multiple underlying sorted sets.  It is assumed that the underlying
 * sorted sets contain the same type of underlying value, and they use the same comparator.
 * This will support null contained in the underlying sets iff the underlying sets use a comparator
 * that can handle null values.
 */
public class MultiSetBackedSortedSet<E> extends AbstractSet<E> implements SortedSet<E> {
    protected List<SortedSet<E>> sets = new ArrayList<>();

    /**
     * Add a set to the underlying sets
     *
     * @param set
     *            the set
     */
    public void addSet(SortedSet<E> set) {
        sets.add(set);
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
        for (SortedSet<E> set : sets) {
            if (!set.isEmpty()) {
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
        return new MergeSortIterator<>(sets);
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
        if (!sets.isEmpty()) {
            return sets.iterator().next().comparator();
        }
        return null;
    }

    @Override
    public SortedSet<E> subSet(E fromElement, E toElement) {
        MultiSetBackedSortedSet<E> subSet = new MultiSetBackedSortedSet<>();
        for (SortedSet<E> set : sets) {
            subSet.addSet(set.subSet(fromElement, toElement));
        }
        return subSet;
    }

    @Override
    public SortedSet<E> headSet(E toElement) {
        MultiSetBackedSortedSet<E> subSet = new MultiSetBackedSortedSet<>();
        for (SortedSet<E> set : sets) {
            subSet.addSet(set.headSet(toElement));
        }
        return subSet;
    }

    @Override
    public SortedSet<E> tailSet(E fromElement) {
        MultiSetBackedSortedSet<E> subSet = new MultiSetBackedSortedSet<>();
        for (SortedSet<E> set : sets) {
            subSet.addSet(set.tailSet(fromElement));
        }
        return subSet;
    }

    @Override
    public E first() {
        SortedSet<E> firstSet = new TreeSet<>(comparator());
        for (SortedSet<E> set : sets) {
            E s = set.first();
            if (s != null) {
                firstSet.add(s);
            }
        }
        return firstSet.first();
    }

    @Override
    public E last() {
        SortedSet<E> lastSet = new TreeSet<>(comparator());
        for (SortedSet<E> set : sets) {
            E s = set.last();
            if (s != null) {
                lastSet.add(s);
            }
        }
        return lastSet.last();
    }
}
