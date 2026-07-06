package datawave.test.framework.util;

import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An iterator that returns a random element
 *
 * @param <E>
 *            the type
 */
public class RandomIterator<E> implements Iterator<E> {

    private static final Logger log = LoggerFactory.getLogger(RandomIterator.class);

    private final Random random;
    private final List<E> elements;

    public RandomIterator(List<E> elements) {
        this(System.nanoTime(), elements);
    }

    public RandomIterator(long seed, List<E> elements) {
        log.trace("Creating RandomIterator with seed: {}", seed);
        this.random = new Random(seed);
        this.elements = elements;
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public E next() {
        return elements.get(random.nextInt(elements.size()));
    }
}
