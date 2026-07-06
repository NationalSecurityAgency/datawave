package datawave.test.framework.generators.value;

import org.apache.commons.lang3.RandomStringUtils;

public class RandomAlphabeticGenerator implements ValueGenerator<String> {

    private static final int DEFAULT_LENGTH = 5;
    private final int length;

    public static ValueGenerator<String> create() {
        return new RandomAlphabeticGenerator();
    }

    public static ValueGenerator<String> create(int length) {
        return new RandomAlphabeticGenerator(length);
    }

    private RandomAlphabeticGenerator() {
        this(DEFAULT_LENGTH);
    }

    private RandomAlphabeticGenerator(int length) {
        this.length = length;
    }

    @Override
    public String next() {
        return RandomStringUtils.secure().nextAlphabetic(length);
    }
}
