package datawave.test.framework.frequency;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import datawave.test.framework.generators.id.EventIdGenerator;
import datawave.test.framework.generators.id.SquaresEventIdGenerator;

public class SquaresEventIdGeneratorTest implements FrequencyGeneratorTest {

    @Override
    public EventIdGenerator getGenerator() {
        return SquaresEventIdGenerator.create();
    }

    @Test
    @Override
    public void testGenerateCount() {
        EventIdGenerator generator = getGenerator();
        List<Integer> frequencies = generator.generateCount(5);
        List<Integer> expected = List.of(1, 4, 9, 16, 25);
        assertEquals(expected, frequencies);
    }

    @Test
    @Override
    public void testGenerateWithinBound() {
        EventIdGenerator generator = getGenerator();
        List<Integer> frequencies = generator.generateWithinBound(20);
        List<Integer> expected = List.of(1, 4, 9, 16);
        assertEquals(expected, frequencies);
    }

    @Test
    @Override
    public void testGenerateCountWithinBound() {
        EventIdGenerator generator = getGenerator();
        List<Integer> frequencies = generator.generateCountWithinBound(5, 10);
        List<Integer> expected = List.of(1, 4, 9);
        assertEquals(expected, frequencies);
    }

    @Test
    public void testGenerateCountStopsBeforeIntOverflow() {
        // 46340^2 fits in an int, 46341^2 does not, so a count beyond that must stop cleanly rather than emit corrupted ids
        EventIdGenerator generator = getGenerator();
        List<Integer> frequencies = generator.generateCount(50_000);

        assertEquals(46_340, frequencies.size());
        assertTrue(frequencies.stream().allMatch(id -> id > 0));
        assertEquals(46_340 * 46_340, frequencies.get(frequencies.size() - 1));
    }
}
