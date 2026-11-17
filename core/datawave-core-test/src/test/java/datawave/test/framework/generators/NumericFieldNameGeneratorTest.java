package datawave.test.framework.generators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import datawave.test.framework.generators.field.NumericFieldNameGenerator;

public class NumericFieldNameGeneratorTest {

    private static final long SEED = 42L;

    @Test
    public void testFieldGeneration() {
        NumericFieldNameGenerator generator = NumericFieldNameGenerator.create(new Random(SEED));
        generator.generate(4);

        List<String> expected = List.of("1_1", "2_1", "3_1", "4_1");
        List<String> fieldNames = generator.getFieldNames();
        assertEquals(expected, fieldNames);
    }

    @Test
    public void testRepeatedGenerationAppends() {
        NumericFieldNameGenerator generator = NumericFieldNameGenerator.create(new Random(SEED));
        generator.generate(2);
        generator.generate(2);

        List<String> expected = List.of("1_1", "2_1", "3_1", "4_1");
        List<String> fieldNames = generator.getFieldNames();
        assertEquals(expected, fieldNames);
    }

    @Test
    public void testGetRandomFields() {
        NumericFieldNameGenerator generator = NumericFieldNameGenerator.create(new Random(SEED));
        generator.generate(7);

        List<String> fieldNames = generator.getFieldNames();
        List<String> randomized = generator.getRandomizedFieldNames();
        assertNotEquals(fieldNames, randomized);
        assertEquals(fieldNames.size(), randomized.size());
        assertTrue(randomized.containsAll(fieldNames));
    }

    @Test
    public void testRandomFieldsReproduceFromSeed() {
        NumericFieldNameGenerator first = NumericFieldNameGenerator.create(new Random(SEED));
        first.generate(7);

        NumericFieldNameGenerator second = NumericFieldNameGenerator.create(new Random(SEED));
        second.generate(7);

        assertEquals(first.getRandomizedFieldNames(), second.getRandomizedFieldNames());
    }

    @Test
    public void testFieldNameRollover() {
        NumericFieldNameGenerator generator = NumericFieldNameGenerator.create(new Random(SEED));
        generator.generate(15);

        List<String> expected = List.of("10_1", "11_1", "12_1", "13_1", "14_1", "15_1");
        List<String> fieldNames = generator.getFieldNames();
        assertEquals(15, fieldNames.size());
        assertTrue(fieldNames.containsAll(expected));
    }

}
