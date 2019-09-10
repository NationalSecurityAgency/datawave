package datawave.query.predicate;

import com.google.common.base.Predicate;

import javax.annotation.Nullable;

public interface PeekingPredicate<T> extends Predicate<T> {
    /**
     * peek at the result of applying an input without actually applying it
     * 
     * @param input
     * @return
     */
    boolean peek(@Nullable T input);
}
