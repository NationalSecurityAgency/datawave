package datawave.test.framework.generators.id;

import java.util.ArrayList;
import java.util.List;

/**
 * This generator produces the Fibonacci sequence of all positive integers (excludes zero)
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
