package datawave.query.jexl;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import datawave.query.iterator.IndexOnlyFunctionIterator;

/**
 * Uses the IndexOnlyFunctioniterator to lazily fetch index-only values when accessors are triggered, such as {@code iterator()} or {@code size()}. Some
 * accessors, particularly {@code iterator()} and {@code isEmpty()}, are optimized to fetch only as much data is immediately needed, which helps to miminize the
 * potentially huge memory footprint associated with index-only fields. For example, {@code isEmpty()} fetches only the first matching term-frequency (tf)
 * field, and the default {@code Iterator<E>} returned by {@code iterator()} fetches only a single matching record at a time for each invocation of its
 * {@code next()} method. Such an optimization does not apply to most other methods, which automatically trigger the complete fetching of matching records into
 * memory, such as {@code size()} and {@code toArray()}.
 *
 * @param <E>
 *            The type of values stored in the set
 * @param <T>
 *            The type of input applied to the IndexOnlyFunctionIterator
 */
public class IndexOnlyLazyFetchingSet<E,T> extends HashSet<E> {
    private static final long serialVersionUID = 3076926385111060416L;

    private final IndexOnlyFunctionIterator<T> iterator;

    private boolean fetched;

    private final String fieldName;

    private boolean retainIteratedValuesByDefault;

    private final String unfetchedToString;

    public IndexOnlyLazyFetchingSet(final String fieldName, final IndexOnlyFunctionIterator<T> iterator) {
        checkNotNull(fieldName, this.getClass().getSimpleName() + " cannot be initialized with a null field name");
        checkNotNull(iterator, this.getClass().getSimpleName() + " cannot be initialized with a null " + IndexOnlyFunctionIterator.class.getSimpleName());
        this.fieldName = fieldName;
        this.iterator = iterator;
        this.unfetchedToString = "[unknown quantity of unfetched, index-only, " + this.fieldName + " values]";
        this.retainIteratedValuesByDefault = false;
    }

    @Override
    public void clear() {
        this.fetchIndexOnlyValues(true, true, false);
    }

    @Override
    public boolean contains(final Object o) {
        if (!super.contains(o)) {
            this.fetchIndexOnlyValues(false, true, true);
        }
        return super.contains(o);
    }

    @Override
    public boolean containsAll(final Collection<?> c) {
        if (!super.containsAll(c)) {
            this.fetchIndexOnlyValues(false, true, true);
        }
        return super.containsAll(c);
    }

    /*
     * Fetch the index-only values via the IndexOnlyFunctionIterator
     *
     * @param cleared If true, clear the contents and the reset the fetched flag
     *
     * @param fetchAll If true, fetch everything into memory. If not, only fetch one value to be able to determine whether there are any values at all
     */
    private void fetchIndexOnlyValues(boolean reset, boolean fetchAll, boolean retainIteratedValuesInMemory) {
        // Clear the contents of the hash
        if (reset) {
            super.clear();
            this.fetched = false;
        }
        // Conditionally fetch the index-only values
        else if (!this.fetched) {
            super.clear();
            final Iterator<E> iterator = this.iterator(retainIteratedValuesInMemory);
            while (iterator.hasNext()) {
                iterator.next();
                if (!fetchAll) {
                    break;
                }
            }
        }
    }

    /**
     * Return the index-only field name associated with this Set of values
     *
     * @return the index-only field name associated with this Set of values
     */
    public String getFieldName() {
        return this.fieldName;
    }

    /*
     * Determine whether or not a next value exists in the two iterators. The in-memory iterator is checked first. If no further values exist, the fetching
     * iterator is checked. If no fetched values exist, the fetched flag is set to indicate that loading is complete.
     *
     * @param inMemoryIterator An in-memory iterator obtained from the superclass
     *
     * @param fetchingIterator An iterator capable of fetching values from a tf section of the shard table
     *
     * @return If true, a next value exists in-memory or from fetched values
     */
    private <U> boolean hasNext(final Iterator<U> inMemoryIterator, final Iterator<U> fetchingIterator, final boolean retainIteratedValues) {
        // Check the in-memory iterator first
        boolean hasNext = inMemoryIterator.hasNext();

        // If not satisfied, try the fetching iterator
        if (!hasNext) {
            hasNext = fetchingIterator.hasNext();
        }

        // If fetching is complete, set the flag
        if (!hasNext && retainIteratedValues) {
            this.fetched = true;
        }

        return hasNext;
    }

    @Override
    public boolean isEmpty() {
        this.fetchIndexOnlyValues(false, false, true);
        return super.isEmpty();
    }

    @Override
    public Iterator<E> iterator() {
        return this.iterator(this.retainIteratedValuesByDefault);
    }

    /*
     * Create and return an iterator that can lazily fetch index-only values for the instance's matching field name. The boolean flag allows iterated values to
     * "drop on the floor" in order to reduce the potentially large memory footprint of index-only fields.
     *
     * @param retainIteratedValuesInMemory If true, add iterated values to the parent Set instance. Otherwise, do not retain iterated values and assume that the
     * iterator's caller only needs each returned value for a short time (i.e., during the lifespan of a typical iteration sequence).
     *
     * @return an iterator that can lazily fetch index-only values for the instance's matching field name
     */
    private Iterator<E> iterator(boolean retainIteratedValuesInMemory) {
        final Iterator<E> iterator;

        final Iterator<E> inmemoryIterator = super.iterator();
        if (!this.fetched) {
            iterator = new IteratorsWrapper<>(inmemoryIterator, retainIteratedValuesInMemory);
        } else {
            iterator = inmemoryIterator;
        }

        return iterator;
    }

    @SuppressWarnings("unchecked")
    private <U> U next(final Iterator<U> inMemoryIterator, final Iterator<U> fetchingIterator, final boolean retainIteratedValues) {
        synchronized (this.iterator) {
            // Initialize the return value
            final U next;

            // Try the in-memory iterator first
            boolean fetched = false;
            if (inMemoryIterator.hasNext()) {
                next = inMemoryIterator.next();
            }
            // Otherwise, try to fetch the next value
            else {
                next = fetchingIterator.next();
                fetched = true;
            }

            // If fetched and instructed to retain the iterated value, add it to the parent Set
            // to avoid fetching if asked again
            if (fetched && retainIteratedValues) {
                // If fetching is complete, set the flag
                if (!fetchingIterator.hasNext()) {
                    this.fetched = true;
                }

                // Add to the parent
                super.add((E) next);
            }

            return next;
        }
    }

    @Override
    public boolean remove(final Object o) {
        this.fetchIndexOnlyValues(false, true, true);
        return super.remove(o);
    }

    @Override
    public boolean removeAll(final Collection<?> c) {
        this.fetchIndexOnlyValues(false, true, true);
        return super.removeAll(c);
    }

    @Override
    public boolean retainAll(final Collection<?> c) {
        this.fetchIndexOnlyValues(false, true, true);
        return super.retainAll(c);
    }

    /**
     * Set a flag to retain iterated values in-memory by default.
     *
     * @param keepIteratedValuesInMemory
     *            If true, retain each iterated value in-memory. The default value is to NOT retain values in memory, which is intended to reduce the
     *            potentially large memory footprint of index-only fields.
     */
    public void setKeepIteratedValuesInMemory(boolean keepIteratedValuesInMemory) {
        this.retainIteratedValuesByDefault = keepIteratedValuesInMemory;
    }

    @Override
    public int size() {
        this.fetchIndexOnlyValues(false, true, true);
        return super.size();
    }

    @Override
    public Object[] toArray() {
        this.fetchIndexOnlyValues(false, true, true);
        return super.toArray();
    }

    @Override
    public <U> U[] toArray(final U[] a) {
        this.fetchIndexOnlyValues(false, true, true);
        return super.toArray(a);
    }

    @Override
    public String toString() {
        final String toString;
        if (this.fetched) {
            toString = super.toString();
        } else {
            toString = this.unfetchedToString;
        }

        return toString;
    }

    private class IteratorsWrapper<U> implements Iterator<U> {
        private final Iterator<U> inMemoryIterator;

        private Iterator<U> fetchingIterator;

        private final boolean retainIteratedValues;

        public IteratorsWrapper(final Iterator<U> inMemoryIterator, boolean retainIteratedValuesInMemory) {
            this.inMemoryIterator = inMemoryIterator;
            this.retainIteratedValues = retainIteratedValuesInMemory;
        }

        @Override
        public boolean hasNext() {
            synchronized (this.inMemoryIterator) {
                if (null == this.fetchingIterator) {
                    this.fetchingIterator = IndexOnlyLazyFetchingSet.this.iterator.newLazyFetchingIterator(IndexOnlyLazyFetchingSet.this.fieldName);
                }
            }

            return IndexOnlyLazyFetchingSet.this.hasNext(this.inMemoryIterator, this.fetchingIterator, this.retainIteratedValues);
        }

        @Override
        public U next() {
            synchronized (this.inMemoryIterator) {
                if (null == this.fetchingIterator) {
                    this.fetchingIterator = IndexOnlyLazyFetchingSet.this.iterator.newLazyFetchingIterator(IndexOnlyLazyFetchingSet.this.fieldName);
                }
            }

            return IndexOnlyLazyFetchingSet.this.next(this.inMemoryIterator, this.fetchingIterator, this.retainIteratedValues);
        }

        @Override
        public void remove() {
            // No op
        }
    }
}
