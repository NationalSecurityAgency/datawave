package datawave.test.framework.generators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import datawave.test.framework.generators.field.NumericFieldNameGenerator;

public class NumericFieldNameGeneratorTest {

    @Test
    public void testFieldGeneration() {
        NumericFieldNameGenerator generator = NumericFieldNameGenerator.create();
        generator.generate(4);

        List<String> expected = List.of("1_1", "2_1", "3_1", "4_1");
        List<String> fieldNames = generator.getFieldNames();
        assertEquals(expected, fieldNames);
    }

    @Test
    public void testGetRandomFields() {
        NumericFieldNameGenerator generator = NumericFieldNameGenerator.create();
        generator.generate(7);

        List<String> expected = List.of("1_1", "2_1", "3_1", "4_1", "5_1", "6_1", "7_1");
        List<String> fieldNames = generator.getRandomizedFieldNames();
        assertNotEquals(expected, fieldNames);
        assertTrue(fieldNames.containsAll(expected));
    }

    @Test
    public void testFieldNameRollover() {
        NumericFieldNameGenerator generator = NumericFieldNameGenerator.create();
        generator.generate(15);

        List<String> expected = List.of("10_1", "11_1", "12_1", "13_1", "14_1", "15_1");
        List<String> fieldNames = generator.getFieldNames();
        assertEquals(15, fieldNames.size());
        assertTrue(fieldNames.containsAll(expected));
    }

}
