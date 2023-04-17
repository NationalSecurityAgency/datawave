package datawave.query.util.sortedset;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

/**
 * This is an iterator that will return a sorted set of items (no dups) from an underlying set of sorted sets. This will support null contained in the
 * underlying sets iff the underlying sets use a comparator that can handle null values.
 * 
 * 
 * 
 * @param <T>
 *            type for the iterator
 */
public class MergeSortIterator<T> implements Iterator<T> {
    
    private List<Iterator<T>> iterators = new ArrayList<>();
    private List<T> lastList = new ArrayList<>();
    private boolean[] finished = null;
    private SortedSet<T> set = null;
    private boolean populated = false;
    private T next = null;
    private List<Iterator<T>> nextIterators = new ArrayList<>();
    
    public MergeSortIterator(Collection<? extends SortedSet<T>> sets) {
        Comparator<? super T> comparator = null;
        for (SortedSet<T> set : sets) {
            comparator = set.comparator();
            Iterator<T> it = set.iterator();
            iterators.add(it);
            nextIterators.add(it);
            lastList.add(null);
        }
        this.set = new TreeSet<>(comparator);
        this.finished = new boolean[iterators.size()];
    }
    
    @Override
    public boolean hasNext() {
        if (!set.isEmpty()) {
            return true;
        }
        for (Iterator<T> it : nextIterators) {
            if (it != null && it.hasNext()) {
                return true;
            }
        }
        return false;
    }
    
    @Override
    public T next() {
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
        for (Iterator<T> it : nextIterators) {
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
    private boolean equals(T o1, T o2) {
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
                Iterator<T> it = nextIterators.get(i);
                if (it.hasNext()) {
                    T val = it.next();
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
