package datawave.query.common.grouping;

import java.util.Collection;
import java.util.HashSet;
import java.util.function.Predicate;

/**
 * This class represents a {@link HashSet} of {@link GroupingAttribute} elements that maintains a cached hashcode that is calculated once at instantiation, and
 * subsequently recalculated any time this set is modified. This class is used as a key within maps and as such, the cached hashcode allows us to avoid
 * calculating the hashcode each time a search operation is performed on the keys of the maps.
 */
public class Grouping extends HashSet<GroupingAttribute<?>> {

    // The cached hashcode.
    private int cachedHashcode;

    /**
     * Return a new {@link Grouping} instance containing the elements of the given collection.
     *
     * @param collection
     *            the collection
     * @return the new grouping
     */
    public static Grouping of(Collection<? extends GroupingAttribute<?>> collection) {
        return new Grouping(collection);
    }

    public Grouping() {
        super();
        updateCachedHashcode();
    }

    public Grouping(GroupingAttribute<?> attribute) {
        super();
        add(attribute);
        updateCachedHashcode();
    }

    public Grouping(Collection<? extends GroupingAttribute<?>> collection) {
        super(collection);
        updateCachedHashcode();
    }

    @Override
    public boolean add(GroupingAttribute<?> groupingAttribute) {
        boolean modified = super.add(groupingAttribute);
        if (modified) {
            updateCachedHashcode();
        }
        return modified;
    }

    @Override
    public boolean addAll(Collection<? extends GroupingAttribute<?>> collection) {
        boolean modified = super.addAll(collection);
        if (modified) {
            updateCachedHashcode();
        }
        return modified;
    }

    @Override
    public boolean remove(Object o) {
        boolean modified = super.remove(o);
        if (modified) {
            updateCachedHashcode();
        }
        return modified;
    }

    @Override
    public boolean removeAll(Collection<?> collection) {
        boolean modified = super.removeAll(collection);
        if (modified) {
            updateCachedHashcode();
        }
        return modified;
    }

    @Override
    public boolean removeIf(Predicate<? super GroupingAttribute<?>> filter) {
        boolean modified = super.removeIf(filter);
        if (modified) {
            updateCachedHashcode();
        }
        return modified;
    }

    @Override
    public void clear() {
        super.clear();
        updateCachedHashcode();
    }

    /**
     * Returns the cached hashcode.
     *
     * @return the hashcode
     */
    @Override
    public int hashCode() {
        return cachedHashcode;
    }

    /**
     * Update the cached hashcode based on the current elements.
     */
    private void updateCachedHashcode() {
        cachedHashcode = super.hashCode();
    }
}
