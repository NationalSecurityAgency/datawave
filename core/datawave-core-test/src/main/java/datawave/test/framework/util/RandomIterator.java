package datawave.test.framework.util;

import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

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
     *            the elements to iterate, which must not be null or empty
     */
    public RandomIterator(long seed, List<E> elements) {
        this(new Random(seed), elements);
        log.trace("Creating RandomIterator with seed: {}", seed);
    }

    /**
     * Constructor that shares an existing source of randomness, so the caller keeps a single reproducible stream
     *
     * @param random
     *            the source of randomness
     * @param elements
     *            the elements to iterate, which must not be null or empty
     */
    public RandomIterator(Random random, List<E> elements) {
        Preconditions.checkArgument(elements != null && !elements.isEmpty(), "elements must not be null or empty");
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
