package datawave.test.framework.frequency;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

import datawave.test.framework.generators.id.EventIdGenerator;
import datawave.test.framework.generators.id.PrimeEventIdGenerator;

public class PrimeEventIdGeneratorTest implements FrequencyGeneratorTest {

    @Override
    public EventIdGenerator getGenerator() {
        return PrimeEventIdGenerator.create();
    }

    @Test
    @Override
    public void testGenerateCount() {
        EventIdGenerator generator = getGenerator();
        List<Integer> frequencies = generator.generateCount(5);
        List<Integer> expected = List.of(2, 3, 5, 7, 11);
        assertEquals(expected, frequencies);
    }

    @Test
    @Override
    public void testGenerateWithinBound() {
        EventIdGenerator generator = getGenerator();
        List<Integer> frequencies = generator.generateWithinBound(5);
        List<Integer> expected = List.of(2, 3, 5);
        assertEquals(expected, frequencies);
    }

    @Test
    @Override
    public void testGenerateCountWithinBound() {
        EventIdGenerator generator = getGenerator();
        List<Integer> frequencies = generator.generateCountWithinBound(10, 25);
        List<Integer> expected = List.of(2, 3, 5, 7, 11, 13, 17, 19, 23);
        assertEquals(expected, frequencies);
    }
}
