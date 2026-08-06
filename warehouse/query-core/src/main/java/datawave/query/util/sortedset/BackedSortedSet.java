package datawave.query.util.sortedset;

import java.io.IOException;
import java.util.SortedSet;

public interface BackedSortedSet<E> extends SortedSet<E> {
    default boolean isPersisted() {
        return false;
    }

    default void persist() throws IOException {
        // no-op
    }
}
