package datawave.test.framework.generators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import datawave.test.framework.generators.field.AlphabeticFieldNameGenerator;

public class AlphabeticFieldNameGeneratorTest {

    private static final long SEED = 42L;

    @Test
    public void testGenerate() {
        AlphabeticFieldNameGenerator generator = AlphabeticFieldNameGenerator.create(new Random(SEED));
        generator.generate(5);

        List<String> fieldNames = generator.getFieldNames();
        List<String> expected = List.of("A", "B", "C", "D", "E");
        assertEquals(expected, fieldNames);
    }

    @Test
    public void testRepeatedGenerationAppends() {
        AlphabeticFieldNameGenerator generator = AlphabeticFieldNameGenerator.create(new Random(SEED));
        generator.generate(3);
        generator.generate(2);

        List<String> fieldNames = generator.getFieldNames();
        List<String> expected = List.of("A", "B", "C", "D", "E");
        assertEquals(expected, fieldNames);
    }

    @Test
    public void testGetRandomFields() {
        AlphabeticFieldNameGenerator generator = AlphabeticFieldNameGenerator.create(new Random(SEED));
        generator.generate(7);

        List<String> fieldNames = generator.getFieldNames();
        List<String> randomized = generator.getRandomizedFieldNames();
        assertNotEquals(fieldNames, randomized);
        assertEquals(fieldNames.size(), randomized.size());
        assertTrue(randomized.containsAll(fieldNames));
    }

    @Test
    public void testRandomFieldsReproduceFromSeed() {
        AlphabeticFieldNameGenerator first = AlphabeticFieldNameGenerator.create(new Random(SEED));
        first.generate(7);

        AlphabeticFieldNameGenerator second = AlphabeticFieldNameGenerator.create(new Random(SEED));
        second.generate(7);

        assertEquals(first.getRandomizedFieldNames(), second.getRandomizedFieldNames());
    }

    @Test
    public void testRollover() {
        AlphabeticFieldNameGenerator generator = AlphabeticFieldNameGenerator.create(new Random(SEED));
        generator.generate(35);

        List<String> fieldNames = generator.getFieldNames();
        List<String> expected = List.of("AA", "AB", "AC", "AD", "AE");
        assertEquals(35, fieldNames.size());
        assertTrue(fieldNames.containsAll(expected));
    }

    @Test
    public void testSecondLevelRollover() {
        AlphabeticFieldNameGenerator generator = AlphabeticFieldNameGenerator.create(new Random(SEED));
        generator.generate(53);

        List<String> fieldNames = generator.getFieldNames();
        assertEquals(53, fieldNames.size());
        assertEquals("AZ", fieldNames.get(51));
        assertEquals("BA", fieldNames.get(52));
    }
}
