package datawave.test.framework.frequency;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

import datawave.test.framework.generators.id.EventIdGenerator;
import datawave.test.framework.generators.id.ModuloEventIdGenerator;

public class ModuloEventIdGeneratorTest implements FrequencyGeneratorTest {

    @Override
    public EventIdGenerator getGenerator() {
        return ModuloEventIdGenerator.create(3);
    }

    @Test
    @Override
    public void testGenerateCount() {
        EventIdGenerator generator = getGenerator();
        List<Integer> frequencies = generator.generateCount(5);
        List<Integer> expected = List.of(3, 6, 9, 12, 15);
        assertEquals(expected, frequencies);
    }

    @Test
    @Override
    public void testGenerateWithinBound() {
        EventIdGenerator generator = getGenerator();
        List<Integer> frequencies = generator.generateWithinBound(5);
        List<Integer> expected = List.of(3);
        assertEquals(expected, frequencies);
    }

    @Test
    @Override
    public void testGenerateCountWithinBound() {
        EventIdGenerator generator = getGenerator();
        List<Integer> frequencies = generator.generateCountWithinBound(10, 25);
        List<Integer> expected = List.of(3, 6, 9, 12, 15, 18, 21, 24);
        assertEquals(expected, frequencies);
    }
}
