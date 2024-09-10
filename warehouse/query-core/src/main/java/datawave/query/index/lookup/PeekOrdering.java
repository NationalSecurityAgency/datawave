package datawave.query.index.lookup;

import java.util.Comparator;

import com.google.common.collect.Ordering;
import com.google.common.collect.PeekingIterator;

public class PeekOrdering<T> extends Ordering<PeekingIterator<T>> {
    private final Ordering<T> ordering;

    private PeekOrdering(Ordering<T> ordering) {
        this.ordering = ordering;
    }

    @Override
    public int compare(PeekingIterator<T> o1, PeekingIterator<T> o2) {
        return ordering.compare(o1.peek(), o2.peek());
    }

    public static <T extends Comparable<T>> PeekOrdering<T> make() {
        return new PeekOrdering<>(Ordering.<T> natural());
    }

    public static <T> PeekOrdering<T> make(Comparator<T> comp) {
        return make(Ordering.from(comp));
    }

    public static <T> PeekOrdering<T> make(Ordering<T> ordering) {
        return new PeekOrdering<>(ordering);
    }
}
