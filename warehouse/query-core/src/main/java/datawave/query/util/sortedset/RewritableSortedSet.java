package datawave.query.util.sortedset;

import java.util.SortedSet;

/**
 * A sorted set that can use a replacement strategy to determine when adding an element that is already in the set should result in replacing that element. This
 * will support null contained in the underlying sets iff a comparator is supplied that can handle null values.
 *
 * @param <E>
 *            type of set
 */
public interface RewritableSortedSet<E> extends SortedSet<E> {

    interface RewriteStrategy<E> {
        /**
         * Determine if the object should be rewritten
         *
         * @param original
         * @param update
         * @return true of the original should be replaced with the update
         */
        boolean rewrite(E original, E update);
    }

    RewritableSortedSetImpl.RewriteStrategy getRewriteStrategy();

    E get(E e);

    RewritableSortedSet<E> subSet(E fromElement, E toElement);

    RewritableSortedSet<E> headSet(E toElement);

    RewritableSortedSet<E> tailSet(E fromElement);

}
