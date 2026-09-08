package datawave.test.framework.frequency;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import datawave.test.framework.generators.id.EventIdGenerator;
import datawave.test.framework.generators.id.FibonacciEventIdGenerator;

public class FibonacciEventIdGeneratorTest implements FrequencyGeneratorTest {

    @Override
    public EventIdGenerator getGenerator() {
        return FibonacciEventIdGenerator.create();
    }

    @Test
    @Override
    public void testGenerateCount() {
        EventIdGenerator generator = getGenerator();
        List<Integer> frequencies = generator.generateCount(5);
        List<Integer> expected = List.of(1, 2, 3, 5, 8);
        assertEquals(expected, frequencies);
    }

    @Test
    @Override
    public void testGenerateWithinBound() {
        EventIdGenerator generator = getGenerator();
        List<Integer> frequencies = generator.generateWithinBound(5);
        List<Integer> expected = List.of(1, 2, 3, 5);
        assertEquals(expected, frequencies);
    }

    @Test
    @Override
    public void testGenerateCountWithinBound() {
        EventIdGenerator generator = getGenerator();
        List<Integer> frequencies = generator.generateCountWithinBound(7, 4);
        List<Integer> expected = List.of(1, 2, 3);
        assertEquals(expected, frequencies);
    }

    @Test
    public void testGenerateCountStopsBeforeIntOverflow() {
        // fib(46) = 1836311903 fits in an int, fib(47) = 2971215073 does not, so a count beyond that must stop cleanly rather than emit corrupted ids
        EventIdGenerator generator = getGenerator();
        List<Integer> frequencies = generator.generateCount(1000);

        assertEquals(45, frequencies.size());
        assertTrue(frequencies.stream().allMatch(id -> id > 0));
        assertEquals(1_836_311_903, frequencies.get(frequencies.size() - 1));
    }
}
