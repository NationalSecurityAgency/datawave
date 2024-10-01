package datawave.ingest.mapreduce.job;

import java.util.List;

import com.google.common.collect.Ordering;

/**
 * Wraps a list that is immutable and verified as sorted.
 */
public class SortedList<T> {

    private final List<T> list;

    private SortedList(List<T> list) {
        this.list = list;
    }

    public List<T> get() {
        return list;
    }

    private static final SortedList<?> EMPTY = new SortedList<>(List.of());

    @SuppressWarnings("unchecked")
    public static <T2> SortedList<T2> empty() {
        return (SortedList<T2>) EMPTY;
    }

    /**
     * For a list that is expected to be sorted this will verify it is sorted and if so return an immutable copy of it.
     *
     * @throws IllegalArgumentException
     *             when the input list is not sorted.
     */
    public static <T2> SortedList<T2> fromSorted(List<T2> list) {
        if (list.isEmpty()) {
            return empty();
        }

        var copy = List.copyOf(list);

        // verify after copying because nothing can change at this point
        @SuppressWarnings("unchecked")
        boolean isSorted = Ordering.natural().isOrdered((Iterable<? extends Comparable>) copy);

        if (isSorted) {
            return new SortedList<>(copy);
        } else {
            throw new IllegalArgumentException("Input list of size " + copy.size() + " was expected to be sorted but was not");
        }
    }
}
