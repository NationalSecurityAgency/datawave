package datawave.test.framework.generators.value;

import org.apache.commons.lang3.RandomStringUtils;

public class RandomAlphaNumericGenerator implements ValueGenerator<String> {

    private static final int DEFAULT_LENGTH = 5;
    private final int length;

    public static ValueGenerator<String> create() {
        return new RandomAlphaNumericGenerator();
    }

    public static ValueGenerator<String> create(int length) {
        return new RandomAlphaNumericGenerator(length);
    }

    private RandomAlphaNumericGenerator() {
        this(DEFAULT_LENGTH);
    }

    private RandomAlphaNumericGenerator(int length) {
        this.length = length;
    }

    @Override
    public String next() {
        return RandomStringUtils.secure().nextAlphanumeric(length);
    }
}
