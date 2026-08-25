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
        long sum;
        List<Integer> frequencies = new ArrayList<>();
        do {
            sum = first + second;
            long eventId = sum + offset;
            if (eventId <= bound && fitsInInt(eventId)) {
                frequencies.add((int) eventId);
                first = second;
                second = sum;
            } else {
                break;
            }
        } while (frequencies.size() < count);

        return frequencies;
    }
}
