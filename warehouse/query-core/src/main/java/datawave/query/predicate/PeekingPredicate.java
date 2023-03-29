package datawave.query.predicate;

import com.google.common.base.Predicate;
import org.apache.accumulo.core.data.Key;

import javax.annotation.Nullable;

public interface PeekingPredicate<T> extends Predicate<T> {
    /**
     * Peek at the result of applying an input without actually applying it. This method is equivalent to {@link #apply} without making any state updates that
     * {@code apply()} may have made. This is necessary for cases where a predicate may be evaluated multiple times for different reasons in the lifecycle.
     * Calling this method multiple times with the same value should always yield the same result, whereas calling {@code apply()} has no such guarantee
     *
     * @see datawave.query.predicate.TLDEventDataFilter#keep(Key)
     *
     * @param input
     *            an input
     * @return the potential result of applying the input
     */
    boolean peek(@Nullable T input);
}
