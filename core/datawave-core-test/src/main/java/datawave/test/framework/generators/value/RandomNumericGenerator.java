package datawave.test.framework.generators.value;

import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;

import com.google.common.base.Preconditions;

/**
 * Generates random numeric values.
 * <p>
 * The {@link Random} is supplied by the caller rather than created here so that an entire ingest can be reproduced from a single seed. See
 * {@code IngestMetadataBuilder.setSeed(long)}.
 */
public class RandomNumericGenerator implements ValueGenerator<Integer> {

    private static final int DEFAULT_LENGTH = 4;

    private final int length;
    private final Random random;

    /**
     * Create a generator of default-length values
     *
     * @param random
     *            the source of randomness
     * @return the generator
     */
    public static ValueGenerator<Integer> create(Random random) {
        return new RandomNumericGenerator(DEFAULT_LENGTH, random);
    }

    /**
     * Create a generator of fixed-length values
     *
     * @param length
     *            the number of digits
     * @param random
     *            the source of randomness
     * @return the generator
     */
    public static ValueGenerator<Integer> create(int length, Random random) {
        return new RandomNumericGenerator(length, random);
    }

    private RandomNumericGenerator(int length, Random random) {
        // an int cannot hold more than ten digits, and the tenth overflows for values above Integer.MAX_VALUE
        Preconditions.checkArgument(length > 0 && length < 10, "length must be between 1 and 9");
        Preconditions.checkNotNull(random, "random cannot be null");
        this.length = length;
        this.random = random;
    }

    @Override
    public Integer next() {
        return Integer.valueOf(RandomStringUtils.random(length, 0, 0, false, true, null, random));
    }
}
