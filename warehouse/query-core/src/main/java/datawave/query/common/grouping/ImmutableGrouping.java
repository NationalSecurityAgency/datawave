package datawave.query.common.grouping;

import java.util.Collection;
import java.util.function.Predicate;

/**
 * This class represents an immutable version of {@link Grouping} that cannot be modified.
 */
public class ImmutableGrouping extends Grouping {

    public ImmutableGrouping(Collection<? extends GroupingAttribute<?>> collection) {
        super();
        for (GroupingAttribute<?> groupingAttribute : collection) {
            // Do not use super.addAll, otherwise ImmutableGrouping.add() will be subsequently called and an exception will be thrown.
            // noinspection UseBulkOperation
            super.add(groupingAttribute);
        }
    }

    /**
     * Throws {@link UnsupportedOperationException}.
     *
     * @param groupingAttribute
     *            element whose presence in this collection is to be ensured
     * @throws UnsupportedOperationException
     *             always
     */
    @Override
    public boolean add(GroupingAttribute<?> groupingAttribute) {
        throw new UnsupportedOperationException();
    }

    /**
     * Throws {@link UnsupportedOperationException}.
     *
     * @param collection
     *            collection containing elements to be added to this collection
     * @throws UnsupportedOperationException
     *             always
     */
    @Override
    public boolean addAll(Collection<? extends GroupingAttribute<?>> collection) {
        throw new UnsupportedOperationException();
    }

    /**
     * Throws {@link UnsupportedOperationException}.
     *
     * @param o
     *            object to be removed from this set, if present
     * @throws UnsupportedOperationException
     *             always
     */
    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    /**
     * Throws {@link UnsupportedOperationException}.
     *
     * @param collection
     *            collection containing elements to be removed from this set
     * @throws UnsupportedOperationException
     *             always
     */
    @Override
    public boolean removeAll(Collection<?> collection) {
        throw new UnsupportedOperationException();
    }

    /**
     * Throws {@link UnsupportedOperationException}.
     *
     * @param filter
     *            a predicate which returns {@code true} for elements to be removed
     * @throws UnsupportedOperationException
     *             always
     */
    @Override
    public boolean removeIf(Predicate<? super GroupingAttribute<?>> filter) {
        throw new UnsupportedOperationException();
    }

    /**
     * Throws {@link UnsupportedOperationException}.
     *
     * @throws UnsupportedOperationException
     *             always
     */
    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }
}
