package datawave.ingest.mapreduce.job;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * Wraps a list that is immutable and verified as sorted.
 */
public class SortedList<T> {

    private static final Logger log = Logger.getLogger(SortedList.class);

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
     * For a list that is expected to be sorted this will verify it is sorted and if so return an immutable copy of it. If this list is not sorted it will log a
     * warning, copy it, sort the copy, and return an immutable version of the copy.
     */
    public static <T2> SortedList<T2> fromSorted(List<T2> list) {
        if (list.isEmpty()) {
            return empty();
        }

        var copy = List.copyOf(list);

        // verify after copying because nothing can change at this point
        boolean isSorted = true;
        for (int i = 1; i < copy.size(); i++) {
            @SuppressWarnings("unchecked")
            var prev = (Comparable<? super T2>) copy.get(i - 1);
            if (prev.compareTo(copy.get(i)) > 0) {
                isSorted = false;
            }
        }

        if (isSorted) {
            return new SortedList<>(copy);
        } else {
            log.warn("Input list of size " + copy.size() + " was expected to be sorted but was not", new IllegalArgumentException());
            return fromUnsorted(copy);
        }
    }

    /**
     * Copies a list and sorts the copy returning an immutable version of the copy.
     */
    public static <T2> SortedList<T2> fromUnsorted(List<T2> list) {
        if (list.isEmpty()) {
            return empty();
        }

        var copy = new ArrayList<>(list);
        @SuppressWarnings("unchecked")
        var compartor = (Comparator<? super T2>) Comparator.naturalOrder();
        copy.sort(compartor);
        return new SortedList<>(Collections.unmodifiableList(copy));
    }
}
