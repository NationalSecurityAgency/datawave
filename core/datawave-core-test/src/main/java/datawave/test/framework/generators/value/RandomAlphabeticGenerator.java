package datawave.test.framework.generators.value;

import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;

import com.google.common.base.Preconditions;

/**
 * Generates random alphabetic values.
 * <p>
 * The {@link Random} is supplied by the caller rather than created here so that an entire ingest can be reproduced from a single seed. See
 * {@code IngestMetadataBuilder.setSeed(long)}.
 */
public class RandomAlphabeticGenerator implements ValueGenerator<String> {

    private static final int DEFAULT_LENGTH = 5;

    private final int length;
    private final Random random;

    /**
     * Create a generator of default-length values
     *
     * @param random
     *            the source of randomness
     * @return the generator
     */
    public static ValueGenerator<String> create(Random random) {
        return new RandomAlphabeticGenerator(DEFAULT_LENGTH, random);
    }

    /**
     * Create a generator of fixed-length values
     *
     * @param length
     *            the value length
     * @param random
     *            the source of randomness
     * @return the generator
     */
    public static ValueGenerator<String> create(int length, Random random) {
        return new RandomAlphabeticGenerator(length, random);
    }

    private RandomAlphabeticGenerator(int length, Random random) {
        Preconditions.checkArgument(length > 0, "length must be greater than 0");
        Preconditions.checkNotNull(random, "random cannot be null");
        this.length = length;
        this.random = random;
    }

    @Override
    public String next() {
        return RandomStringUtils.random(length, 0, 0, true, false, null, random);
    }
}
