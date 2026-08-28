package datawave.test.framework.frequency;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

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

    @Test
    public void testOffsetSelectsResidueClass() {
        EventIdGenerator generator = ModuloEventIdGenerator.create(1, 2);
        List<Integer> frequencies = generator.generateCount(5);
        List<Integer> expected = List.of(1, 3, 5, 7, 9);
        assertEquals(expected, frequencies);
    }

    @Test
    public void testOffsetsPartitionTheIdSpace() {
        List<Integer> evens = ModuloEventIdGenerator.create(0, 2).generateWithinBound(10);
        List<Integer> odds = ModuloEventIdGenerator.create(1, 2).generateWithinBound(10);

        assertEquals(List.of(2, 4, 6, 8, 10), evens);
        assertEquals(List.of(1, 3, 5, 7, 9), odds);

        Set<Integer> combined = new TreeSet<>(evens);
        combined.addAll(odds);
        assertEquals(Set.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), combined);
        assertEquals(evens.size() + odds.size(), combined.size(), "The residue classes must not overlap");
    }

    @Test
    public void testOffsetMustBeLessThanModulo() {
        assertThrows(IllegalArgumentException.class, () -> ModuloEventIdGenerator.create(2, 2));
        assertThrows(IllegalArgumentException.class, () -> ModuloEventIdGenerator.create(-1, 2));
    }
}
