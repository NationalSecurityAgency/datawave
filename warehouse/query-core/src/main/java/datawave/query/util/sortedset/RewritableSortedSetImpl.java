package datawave.query.util.sortedset;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;

import org.apache.log4j.Logger;

import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import it.unimi.dsi.fastutil.ints.IntIterator;

/**
 * A sorted set that can use a replacement strategy to determine when adding an element that is already in the set should result in replacing that element. This
 * will support null contained in the underlying sets iff a comparator is supplied that can handle null values.
 *
 * @param <E>
 *            type of set
 */
public class RewritableSortedSetImpl<E> implements RewritableSortedSet<E>, Cloneable {
    private static Logger log = Logger.getLogger(RewritableSortedSetImpl.class);
    // using a map to enable replacement of the actual set member (see uses of collisionSelection)
    protected NavigableMap<E,E> set = null;
    // When the set contains X and we are adding Y where X == Y, then use this strategy
    // to decide which to keep.
    protected RewriteStrategy<E> rewriteStrategy = null;

    /**
     * Create the rewritable sorted set
     *
     * @param comparator
     * @param rewriteStrategy
     */
    public RewritableSortedSetImpl(Comparator<E> comparator, RewriteStrategy rewriteStrategy) {
        this.set = new TreeMap<>(comparator);
        this.rewriteStrategy = rewriteStrategy;
    }

    /**
     * Create the rewritable sorted set
     *
     * @param rewriteStrategy
     */
    public RewritableSortedSetImpl(RewriteStrategy rewriteStrategy) {
        this(null, rewriteStrategy);
    }

    /**
     * Create the rewritable sorted set
     *
     * @param comparator
     */
    public RewritableSortedSetImpl(Comparator<E> comparator) {
        this(comparator, null);
    }

    /**
     * Create the rewritable sorted set
     */
    public RewritableSortedSetImpl() {
        this(null, null);
    }

    /**
     * Create a file sorted set from another one
     *
     * @param other
     *            the other sorted set
     */
    public RewritableSortedSetImpl(RewritableSortedSetImpl<E> other) {
        this.set = new TreeMap<>(other.set);
        this.rewriteStrategy = other.rewriteStrategy;
    }

    /**
     * Create a file sorted subset from another one
     *
     * @param other
     *            the other sorted set
     * @param from
     *            the from key
     * @param to
     *            the to key
     */
    public RewritableSortedSetImpl(RewritableSortedSetImpl<E> other, E from, E to) {
        this(other);
        if (from != null || to != null) {
            if (to == null) {
                this.set = this.set.tailMap(from, true);
            } else if (from == null) {
                this.set = this.set.headMap(to, false);
            } else {
                this.set = this.set.subMap(from, true, to, false);
            }
        }
    }

    @Override
    public RewriteStrategy getRewriteStrategy() {
        return rewriteStrategy;
    }

    /**
     * Get the size of the set. Note if the set has been persisted, then this may be an upper bound on the size.
     *
     * @return the size upper bound
     */
    @Override
    public int size() {
        return set.size();
    }

    @Override
    public boolean isEmpty() {
        return set.isEmpty();
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean contains(Object o) {
        return set.containsKey(o);
    }

    @Override
    public Iterator<E> iterator() {
        return set.keySet().iterator();
    }

    @Override
    public Object[] toArray() {
        return set.keySet().toArray();
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public <T> T[] toArray(T[] a) {
        return set.keySet().toArray(a);
    }

    @Override
    public boolean add(E e) {
        return addResolvingCollisions(e);
    }

    @Override
    public E get(E e) {
        return set.get(e);
    }

    private boolean addResolvingCollisions(E e) {
        if ((rewriteStrategy != null) && set.containsKey(e) && rewriteStrategy.rewrite(set.get(e), e)) {
            set.remove(e);
        }
        // return true if this is a new element to the set
        return (set.put(e, e) == null);
    }

    @Override
    public boolean remove(Object o) {
        return (set.remove(o) != null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean containsAll(Collection<?> c) {
        if (c.isEmpty()) {
            return true;
        }
        return set.keySet().containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        if (!c.isEmpty()) {
            return c.stream().map(m -> add(m)).reduce((l, r) -> l || r).get();
        }
        return false;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return set.keySet().retainAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return set.keySet().removeAll(c);
    }

    @Override
    public boolean removeIf(Predicate<? super E> filter) {
        return set.keySet().removeIf(filter);
    }

    @Override
    public void clear() {
        set.clear();
    }

    @Override
    public Comparator<? super E> comparator() {
        return set.comparator();
    }

    @Override
    public RewritableSortedSet<E> subSet(E fromElement, E toElement) {
        return new RewritableSortedSetImpl(this, fromElement, toElement);
    }

    @Override
    public RewritableSortedSet<E> headSet(E toElement) {
        return new RewritableSortedSetImpl(this, null, toElement);
    }

    @Override
    public RewritableSortedSet<E> tailSet(E fromElement) {
        return new RewritableSortedSetImpl(this, fromElement, null);
    }

    @Override
    public E first() {
        boolean gotFirst = false;
        E first = null;
        if (!set.isEmpty()) {
            first = set.firstKey();
            gotFirst = true;
        }
        if (!gotFirst) {
            QueryException qe = new QueryException(DatawaveErrorCode.FETCH_FIRST_ELEMENT_ERROR);
            throw (NoSuchElementException) (new NoSuchElementException().initCause(qe));
        } else {
            return first;
        }
    }

    @Override
    public E last() {
        boolean gotLast = false;
        E last = null;
        if (!set.isEmpty()) {
            last = set.lastKey();
            gotLast = true;
        }
        if (!gotLast) {
            QueryException qe = new QueryException(DatawaveErrorCode.FETCH_LAST_ELEMENT_ERROR);
            throw (NoSuchElementException) (new NoSuchElementException().initCause(qe));
        } else {
            return last;
        }
    }

    @Override
    public String toString() {
        return set.toString();
    }

    /**
     * Extending classes must implement cloneable
     *
     * @return A clone
     */
    public RewritableSortedSetImpl<E> clone() {
        return new RewritableSortedSetImpl(this);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof Set)) {
            return false;
        } else {
            Set<?> s = (Set) o;
            return s.size() != this.size() ? false : this.containsAll(s);
        }
    }

    @Override
    public int hashCode() {
        int h = 0;
        int n = this.size();

        int k;
        for (Iterator i = this.iterator(); n-- != 0; h += k) {
            k = i.next().hashCode();
        }

        return h;
    }

    /* Some utilities */
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

    private int compare(E a, E b) {
        if (this.set.comparator() != null) {
            return this.set.comparator().compare(a, b);
        } else {
            return ((Comparable<E>) a).compareTo(b);
        }
    }

}
