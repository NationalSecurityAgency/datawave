package datawave.test.framework.util;

import java.util.Iterator;
import java.util.List;

import com.google.common.base.Preconditions;

/**
 * An iterator that cycles through the list of elements forever
 *
 * @param <E>
 *            the type
 */
public class InfiniteIterator<E> implements Iterator<E> {

    private final List<E> elements;

    private int index = 0;

    /**
     * @param elements
     *            the elements to cycle through, which must not be null or empty
     */
    public InfiniteIterator(List<E> elements) {
        Preconditions.checkArgument(elements != null && !elements.isEmpty(), "elements must not be null or empty");
        this.elements = elements;
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public E next() {
        E element = elements.get(index);
        index = (index + 1) % elements.size();
        return element;
    }
}
