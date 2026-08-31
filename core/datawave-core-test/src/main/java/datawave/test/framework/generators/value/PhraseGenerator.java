package datawave.test.framework.generators.value;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;

import com.google.common.base.Preconditions;

/**
 * Generates space-delimited phrases of random alphabetic words, suitable for a tokenized (term frequency) field.
 * <p>
 * The {@link Random} is supplied by the caller rather than created here so that an entire ingest can be reproduced from a single seed. See
 * {@code IngestMetadataBuilder.setSeed(long)}.
 */
public class PhraseGenerator implements ValueGenerator<String> {

    private static final int DEFAULT_WORD_COUNT = 5;
    private static final int DEFAULT_WORD_LENGTH = 5;

    private final int wordCount;
    private final int wordLength;
    private final Random random;

    /**
     * Create a generator of default-shaped phrases
     *
     * @param random
     *            the source of randomness
     * @return the generator
     */
    public static ValueGenerator<String> create(Random random) {
        return new PhraseGenerator(DEFAULT_WORD_COUNT, DEFAULT_WORD_LENGTH, random);
    }

    /**
     * Create a generator of phrases with the given shape
     *
     * @param wordCount
     *            the number of words per phrase
     * @param wordLength
     *            the length of each word
     * @param random
     *            the source of randomness
     * @return the generator
     */
    public static ValueGenerator<String> create(int wordCount, int wordLength, Random random) {
        return new PhraseGenerator(wordCount, wordLength, random);
    }

    private PhraseGenerator(int wordCount, int wordLength, Random random) {
        Preconditions.checkArgument(wordCount > 0, "wordCount must be greater than 0");
        Preconditions.checkArgument(wordLength > 0, "wordLength must be greater than 0");
        Preconditions.checkNotNull(random, "random cannot be null");
        this.wordCount = wordCount;
        this.wordLength = wordLength;
        this.random = random;
    }

    @Override
    public String next() {
        List<String> words = new ArrayList<>();
        for (int i = 0; i < wordCount; i++) {
            words.add(RandomStringUtils.random(wordLength, 0, 0, true, false, null, random));
        }
        return String.join(" ", words);
    }
}
