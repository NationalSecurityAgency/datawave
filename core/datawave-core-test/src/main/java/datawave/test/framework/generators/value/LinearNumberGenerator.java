package datawave.test.framework.generators.value;

public class LinearNumberGenerator implements ValueGenerator<Integer> {

    private int value = 1;

    public static ValueGenerator<Integer> create() {
        return new LinearNumberGenerator();
    }

    private LinearNumberGenerator() {
        // no-op
    }

    @Override
    public Integer next() {
        int next = value;
        value++;
        return next;
    }
}
