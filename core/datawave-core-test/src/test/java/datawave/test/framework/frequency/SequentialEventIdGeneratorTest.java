package datawave.test.framework.frequency;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

import datawave.test.framework.generators.id.EventIdGenerator;
import datawave.test.framework.generators.id.SequentialEventIdGenerator;

public class SequentialEventIdGeneratorTest implements FrequencyGeneratorTest {

    @Override
    public EventIdGenerator getGenerator() {
        return SequentialEventIdGenerator.create();
    }

    @Test
    @Override
    public void testGenerateCount() {
        EventIdGenerator generator = getGenerator();
        List<Integer> frequencies = generator.generateCount(7);
        List<Integer> expected = List.of(1, 2, 3, 4, 5, 6, 7);
        assertEquals(expected, frequencies);
    }

    @Test
    @Override
    public void testGenerateWithinBound() {
        EventIdGenerator generator = getGenerator();
        List<Integer> frequencies = generator.generateWithinBound(5);
        List<Integer> expected = List.of(1, 2, 3, 4, 5);
        assertEquals(expected, frequencies);
    }

    @Test
    @Override
    public void testGenerateCountWithinBound() {
        EventIdGenerator generator = getGenerator();
        List<Integer> frequencies = generator.generateCountWithinBound(7, 4);
        List<Integer> expected = List.of(1, 2, 3, 4);
        assertEquals(expected, frequencies);
    }
}
