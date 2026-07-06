package datawave.test.framework.util;

import java.util.Iterator;
import java.util.List;

/**
 * An iterator that cycles through the list of elements forever
 *
 * @param <E>
 *            the type
 */
public class InfiniteIterator<E> implements Iterator<E> {

    private final List<E> elements;

    private int index = 0;

    public InfiniteIterator(List<E> elements) {
        this.elements = elements;
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public E next() {
        int i = index++ % elements.size();
        return elements.get(i);
    }
}
