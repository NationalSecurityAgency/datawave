package datawave.test.framework.util;

import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An iterator that returns a random element
 * <p>
 * There is deliberately no unseeded constructor: a caller that cannot name its seed cannot reproduce a failing run.
 *
 * @param <E>
 *            the type
 */
public class RandomIterator<E> implements Iterator<E> {

    private static final Logger log = LoggerFactory.getLogger(RandomIterator.class);

    private final Random random;
    private final List<E> elements;

    /**
     * Constructor that derives its randomness from the given seed
     *
     * @param seed
     *            the seed
     * @param elements
     *            the elements to iterate
     */
    public RandomIterator(long seed, List<E> elements) {
        log.trace("Creating RandomIterator with seed: {}", seed);
        this.random = new Random(seed);
        this.elements = elements;
    }

    /**
     * Constructor that shares an existing source of randomness, so the caller keeps a single reproducible stream
     *
     * @param random
     *            the source of randomness
     * @param elements
     *            the elements to iterate
     */
    public RandomIterator(Random random, List<E> elements) {
        this.random = random;
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
