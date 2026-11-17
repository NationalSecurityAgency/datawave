package datawave.test.framework.generators.id;

import java.util.ArrayList;
import java.util.List;

/**
 * This generator produces the Fibonacci sequence of all positive integers (excludes zero)
 * <p>
 * The offset is added to each generated id, so an offset of 1 produces <code>2, 3, 4, 6, 9, ...</code>
 */
public class FibonacciEventIdGenerator extends AbstractEventIdGenerator {

    public static EventIdGenerator create() {
        return new FibonacciEventIdGenerator();
    }

    public static EventIdGenerator create(int offset) {
        return new FibonacciEventIdGenerator(offset);
    }

    private FibonacciEventIdGenerator() {
        super();
    }

    private FibonacciEventIdGenerator(int offset) {
        super(offset);
    }

    @Override
    public List<Integer> generateCountWithinBound(int count, int bound) {
        long first = 0;
        long second = 1;
        List<Integer> frequencies = new ArrayList<>();
        while (frequencies.size() < count) {
            long sum = first + second;
            long eventId = sum + offset;
            if (!fitsInInt(eventId) || eventId > bound) {
                break;
            }

            frequencies.add((int) eventId);
            first = second;
            second = sum;
        }

        return frequencies;
    }
}
