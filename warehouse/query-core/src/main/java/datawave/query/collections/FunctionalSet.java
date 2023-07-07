package datawave.query.collections;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.query.attributes.ValueTuple;
import datawave.query.jexl.DatawavePartialInterpreter.State;
import datawave.util.OperationEvaluator;

public class FunctionalSet<T extends ValueTuple> implements Set<T> {

    private static final long serialVersionUID = 8759102050325550844L;
    protected Set<T> delegate;

    /**
     * The empty set (immutable). This set is serializable.
     *
     * @see #emptySet()
     */
    @SuppressWarnings("unchecked")
    public static final FunctionalSet EMPTY_SET = new EmptySet<>();

    protected static final Logger log = Logger.getLogger(FunctionalSet.class);

    protected static final FunctionalSet EMPTY = new FunctionalSet();

    public FunctionalSet() {
        delegate = new LinkedHashSet<>();
    }

    public FunctionalSet(Comparator<T> comparator) {
        if (comparator == null) {
            delegate = new LinkedHashSet<>();
        } else {
            delegate = new TreeSet<>(comparator);
        }
    }

    public FunctionalSet(Collection<? extends T> c) {
        delegate = new LinkedHashSet<>(c);
    }

    public FunctionalSet(Collection<? extends T> c, Comparator<T> comparator) {
        this(comparator);
        addAll(c);
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        if (o instanceof ValueTuple) {
            if (delegate.contains(o)) {
                return true;
            } else {
                return contains(((ValueTuple) o).getFieldName(), String.valueOf(((ValueTuple) o).getValue()))
                                || contains(((ValueTuple) o).getFieldName(), String.valueOf(((ValueTuple) o).getNormalizedValue()));
            }
        } else {
            // a special case for doing value containership (see RefactoredDefaultPlanneQueryTableTestForCompositeFunctions)
            String s = String.valueOf(o);
            if (contains(null, s)) {
                return true;
            }
            int split = s.indexOf(':');
            if (split >= 0) {
                return contains(s.substring(0, split), s.substring(split + 1));
            }
        }
        return false;
    }

    @Override
    public Iterator<T> iterator() {
        return delegate.iterator();
    }

    @Override
    public Object[] toArray() {
        return delegate.toArray();
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        return delegate.toArray(a);
    }

    @Override
    public boolean add(T t) {
        return delegate.add(t);
    }

    @Override
    public boolean remove(Object o) {
        return delegate.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return delegate.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        return delegate.addAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return delegate.retainAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return delegate.removeAll(c);
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return delegate.equals(obj);
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    public boolean contains(String field, String value) {
        for (ValueTuple vt : this) {
            if (field == null || vt.getFieldName().equals(field)) {
                if (eq(String.valueOf(vt.getValue()), value) || eq(String.valueOf(vt.getNormalizedValue()), value)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns true if the specified arguments are equal, or both null.
     *
     * @param o1
     *            the first object
     * @param o2
     *            the second object
     * @return a boolean if the objects are equal
     */
    static boolean eq(Object o1, Object o2) {
        return o1 == null ? o2 == null : o1.equals(o2);
    }

    public static final <T extends ValueTuple> FunctionalSet<T> emptySet() {
        return (FunctionalSet<T>) EMPTY_SET;
    }

    public static <T extends ValueTuple> FunctionalSet<T> singleton(T value) {
        return new SingletonFunctionalSet<>(value);
    }

    public static <T extends ValueTuple> FunctionalSet<T> unmodifiableSet(FunctionalSet<T> set) {
        return new UnmodifiableSet<>(set);
    }

    public static <T extends ValueTuple> FunctionalSet<T> empty() {
        return EMPTY;
    }

    public T getValueForGroup(String group) {
        T ret = null;
        for (Iterator<T> iterator = iterator(); iterator.hasNext();) {
            T next = iterator.next();
            String field = next.first();
            if (field.endsWith("." + group)) {
                if (log.isTraceEnabled()) {
                    log.trace("field:" + field + " in:" + next + " is a match for group:" + group);
                }
                ret = next;
            }
        }
        return ret;
    }

    public T getValueForGroup(int group) {
        T ret = null;
        for (Iterator<T> iterator = iterator(); iterator.hasNext();) {
            T next = iterator.next();
            String field = next.first();
            if (field.endsWith("." + group)) {
                Object val = next.getValue();
                if (val != null) {
                    ret = next;
                    if (log.isTraceEnabled()) {
                        log.trace("field:" + field + " in:" + next + " is a match for group:" + group);
                    }
                }
                ret = next;
            }
        }
        return ret;
    }

    public Object max() {
        T max = null;
        for (T tuple : this) {
            try {
                if (max == null || ((Comparable) tuple.getNormalizedValue()).compareTo((Comparable) max.getNormalizedValue()) > 0) {
                    max = tuple;
                }
            } catch (Exception ex) {
                log.info("Ignoring " + ex + " while computing max");
            }
        }
        return max;
    }

    public Object min() {
        T min = null;
        for (T tuple : this) {
            try {
                if (min == null || ((Comparable) tuple.getNormalizedValue()).compareTo((Comparable) min.getNormalizedValue()) < 0) {
                    min = tuple;
                }
            } catch (Exception ex) {
                log.info("Ignoring " + ex + " while computing min");
            }
        }
        return min;
    }

    public Collection<T> getValuesForGroups(Object in) {

        Collection<T> values = new FunctionalSet<>();

        if (in instanceof State) {
            State state = (State) in;
            if (state.isIncomplete()) {
                throw new IllegalStateException("should not have gotten here");
            }
            in = ((State) in).getValue();
        }

        if (in instanceof String) {

            Object value = this.getValueForGroup((String) in);
            if (value != null) {
                return FunctionalSet.singleton(this.getValueForGroup((String) in));
            } else {
                return FunctionalSet.empty();
            }
        }
        if (in instanceof Collection) {
            for (Object group : (Collection) in) {
                T obj = this.getValueForGroup((String) group);
                if (obj != null) {
                    values.add(obj);
                }
            }
        }
        if (log.isTraceEnabled())
            log.trace("getValuesForGroups(" + in + ") returning " + values);
        return values;
    }

    public Collection<T> getValuesForGroups(Collection<String> groups) {
        Collection<T> values = new FunctionalSet<>();
        for (String group : groups) {
            T obj = this.getValueForGroup(group);
            if (obj != null) {
                values.add(obj);
            }
        }
        if (log.isTraceEnabled())
            log.trace("getValuesForGroups(" + groups + ") returning " + values);
        return values;
    }

    public Object getGroupsForValue(String value) {
        Set<String> groups = Sets.newHashSet();
        for (Iterator<T> iterator = iterator(); iterator.hasNext();) {
            ValueTuple next = iterator.next();
            String field = next.first();
            Object val = next.getValue();
            if (val instanceof String) {
                if (((String) val).equals(value)) {
                    groups.add(field.substring(field.indexOf('.') + 1));
                }
            } else if (val instanceof Number) {
                String longValue = ((Number) val).toString();
                if (longValue.equals(value)) {
                    groups.add(field.substring(field.indexOf('.') + 1));
                }
            } else if (val instanceof NumberType) {
                String longValue = "" + ((NumberType) val).getDelegate().longValue();
                if (longValue.equals(value)) {
                    groups.add(field.substring(field.indexOf('.') + 1));
                }
            }
        }
        return !groups.isEmpty() ? groups.iterator().next() : null;
    }

    public Object getGroupsForValue(int value) {
        Set<String> groups = Sets.newHashSet();
        for (Iterator<T> iterator = iterator(); iterator.hasNext();) {
            ValueTuple next = iterator.next();
            String field = next.first();
            Object val = next.getValue();
            if (val instanceof String) {
                if (((String) val).equals("" + value)) {
                    groups.add(field.substring(field.indexOf('.') + 1));
                }
            } else if (val instanceof Number) {
                String longValue = ((Number) val).toString();
                if (longValue.equals("" + value)) {
                    groups.add(field.substring(field.indexOf('.') + 1));
                }
            } else if (val instanceof NumberType) {
                String longValue = "" + ((NumberType) val).getDelegate().longValue();
                if (longValue.equals("" + value)) {
                    groups.add(field.substring(field.indexOf('.') + 1));
                }
            }
        }
        return groups;
    }

    /*
     * revisit this if the incoming min/max from 4.2.0 are insufficient. It has not been used before public ValueTuple min() { ValueTuple min = null; for
     * (Iterator<T> iterator = iterator(); iterator.hasNext();) { ValueTuple next = iterator.next(); if (min == null) { min = next; } else if (compare(min,
     * next) >= 0) { min = next; } } return min; }
     *
     * public ValueTuple max() { ValueTuple max = null; for (Iterator<T> iterator = iterator(); iterator.hasNext();) { ValueTuple next = iterator.next(); if
     * (max == null) { max = next; } else if (compare(max, next) <= 0) { max = next; } } return max; }
     *
     * private int compare(ValueTuple left, ValueTuple right) { Object leftValue = left.getValue(); Object rightValue = right.getValue(); if (leftValue
     * instanceof String && rightValue instanceof String) { return ((String) leftValue).compareTo((String) rightValue); } if (leftValue instanceof Number &&
     * rightValue instanceof Number) { return Double.compare(((Number) leftValue).doubleValue(), ((Number) rightValue).doubleValue()); } if (leftValue
     * instanceof Date && rightValue instanceof Date) { return Long.compare(((Date) leftValue).getTime(), ((Date) rightValue).getTime()); } if (leftValue
     * instanceof Type && rightValue instanceof Type) { return ((Type) leftValue).getDelegate().compareTo(((Type) rightValue).getDelegate()); } return 0; }
     */
    public Object compareWith(Object reference, String operatorString) {
        Collection<Object> values = new HashSet<>();
        for (Iterator<T> iterator = iterator(); iterator.hasNext();) {
            ValueTuple next = iterator.next();
            Object nextValue = next.getValue();
            if (nextValue instanceof Type) {
                Type nextType = (Type) nextValue;
                Class typeClass = nextType.getClass();
                Type referenceType = Type.Factory.createType(typeClass.getName());
                referenceType.setDelegateFromString(reference.toString());
                boolean keep = OperationEvaluator.compare(nextType.normalize(), referenceType.normalize(), operatorString);
                if (keep) {
                    values.add(next);
                }
            }
        }
        log.debug("returning:" + values);
        return values;
    }

    public Object lessThan(Object reference) {
        return compareWith(reference, "<");
    }

    public Object greaterThan(Object reference) {
        return compareWith(reference, ">");
    }

    /**
     * @serial include
     */
    private static class SingletonFunctionalSet<E extends ValueTuple> extends FunctionalSet<E> implements Serializable {
        private static final long serialVersionUID = 3193687207550431679L;

        SingletonFunctionalSet(E e) {
            super();
            delegate = Collections.singleton(e);
        }

    }

    /**
     * @serial include
     */
    private static class EmptySet<E extends ValueTuple> extends FunctionalSet<E> implements Serializable {
        private static final long serialVersionUID = 3193687207550431678L;

        EmptySet() {
            super();
            delegate = Collections.emptySet();
        }
    }

    /**
     * @serial include
     */
    private static class UnmodifiableSet<E extends ValueTuple> extends FunctionalSet<E> implements Serializable {
        private static final long serialVersionUID = 3193687207550431677L;

        UnmodifiableSet(FunctionalSet<? extends E> set) {
            super();
            delegate = Collections.unmodifiableSet(set.delegate);
        }
    }

}
