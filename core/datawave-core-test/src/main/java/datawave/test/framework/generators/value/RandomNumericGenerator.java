package datawave.test.framework.generators.value;

import org.apache.commons.lang3.RandomStringUtils;

public class RandomNumericGenerator implements ValueGenerator<Integer> {

    private static final int DEFAULT_LENGTH = 4;
    private final int length;

    public static ValueGenerator<Integer> create() {
        return new RandomNumericGenerator();
    }

    public static ValueGenerator<Integer> create(int length) {
        return new RandomNumericGenerator(length);
    }

    private RandomNumericGenerator() {
        this(DEFAULT_LENGTH);
    }

    private RandomNumericGenerator(int length) {
        this.length = length;
    }

    @Override
    public Integer next() {
        return Integer.valueOf(RandomStringUtils.secure().nextNumeric(length));
    }
}
