package datawave.core.query.jexl.lookups;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import datawave.query.jexl.lookups.ExceededThresholdException;

/**
 * A class that thresholds the number of items, and maintains state if the threshold was exceeded. Once the threshold is exceeded, the list is cleared and
 * nothing can be added to it.
 */
public class ValueSet implements Set<String>, Serializable {

    private static final long serialVersionUID = -2272124724019410449L;
    private Set<String> values = new HashSet<>();
    private boolean exceededThreshold = false;
    private int threshold = -1;

    public ValueSet(int _threshold) {
        super();
        this.threshold = _threshold;
    }

    public boolean isThresholdExceeded() {
        return this.exceededThreshold;
    }

    @Override
    public boolean add(String e) {
        testExceeded(e);
        if (exceededThreshold) {
            return false;
        }
        return values.add(e);
    }

    @Override
    public boolean addAll(Collection<? extends String> c) {
        for (String value : c) {
            if (!add(value)) {
                return false;
            }
        }
        return true;
    }

    public int size() {
        checkExceededAndThrow("size");
        return values.size();
    }

    public boolean isEmpty() {
        return !exceededThreshold && values.isEmpty();
    }

    public boolean contains(Object o) {
        checkExceededAndThrow("contains");
        return values.contains(o);
    }

    public Iterator<String> iterator() {
        checkExceededAndThrow("iterator");
        return values.iterator();
    }

    public Object[] toArray() {
        checkExceededAndThrow("toArray");
        return values.toArray();
    }

    public <T> T[] toArray(T[] a) {
        checkExceededAndThrow("toArray");
        return values.toArray(a);
    }

    public boolean remove(Object o) {
        checkExceededAndThrow("remove");
        return values.remove(o);
    }

    public boolean containsAll(Collection<?> c) {
        checkExceededAndThrow("containsAll");
        return values.containsAll(c);
    }

    public boolean removeAll(Collection<?> c) {
        checkExceededAndThrow("removeAll");
        return values.removeAll(c);
    }

    public boolean retainAll(Collection<?> c) {
        checkExceededAndThrow("retainAll");
        return values.retainAll(c);
    }

    public void clear() {
        checkExceededAndThrow("clear");
        // cheaper than clear
        values = new HashSet<>();
    }

    public boolean equals(Object o) {
        if (!(o instanceof ValueSet))
            return false;
        return values.equals(o) && exceededThreshold == ((ValueSet) o).exceededThreshold && threshold == ((ValueSet) o).threshold;
    }

    public int hashCode() {
        return values.hashCode() + Boolean.valueOf(exceededThreshold).hashCode() + threshold;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        if (exceededThreshold) {
            builder.append("ExceededThreshold of ").append(threshold);
        } else {
            builder.append(values);
        }
        return builder.toString();
    }

    private void testExceeded(String e) {
        if (threshold > 0 && !exceededThreshold && !values.contains(e) && (size() + 1 > threshold)) {
            markExceeded();
        }
    }

    private void markExceeded() {
        clear();
        exceededThreshold = true;
    }

    private void checkExceededAndThrow(String method) {
        if (exceededThreshold) {
            throw new ExceededThresholdException("Cannot perform " + method + " operation once the list has exceeded its threshold");
        }
    }

    /**
     * Sets the exceed threshold marker
     */
    public void setThresholdExceeded() {
        exceededThreshold = true;

    }

}
