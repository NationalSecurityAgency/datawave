package datawave.test.framework.generators.id;

import java.util.List;

/**
 * Abstract implementation of {@link EventIdGenerator}
 */
public abstract class AbstractEventIdGenerator implements EventIdGenerator {

    protected static final int DEFAULT_OFFSET = 0;
    protected int offset;

    /**
     * Constructor that uses the default offset of zero
     */
    public AbstractEventIdGenerator() {
        this(DEFAULT_OFFSET);
    }

    /**
     * Constructor that accepts a user-provided offset
     *
     * @param offset
     *            the starting event id offset
     */
    public AbstractEventIdGenerator(int offset) {
        this.offset = offset;
    }

    /**
     * Configure the offset
     *
     * @param offset
     *            the offset
     */
    @Override
    public void setOffset(int offset) {
        this.offset = offset;
    }

    @Override
    public List<Integer> generateCount(int count) {
        return generateCountWithinBound(count, Integer.MAX_VALUE);
    }

    @Override
    public List<Integer> generateWithinBound(int bound) {
        return generateCountWithinBound(Integer.MAX_VALUE, bound);
    }

    /**
     * Guards against integer overflow when a generator computes an event id using {@code long} arithmetic before narrowing it to an {@code int}
     *
     * @param value
     *            the computed event id
     * @return true if the value can be safely narrowed to an int
     */
    protected static boolean fitsInInt(long value) {
        return value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE;
    }
}
