package datawave.test.framework.generators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import datawave.test.framework.generators.field.AlphabeticFieldNameGenerator;

public class AlphabeticFieldNameGeneratorTest {

    @Test
    public void testGenerate() {
        AlphabeticFieldNameGenerator generator = AlphabeticFieldNameGenerator.create();
        generator.generate(5);

        List<String> fieldNames = generator.getFieldNames();
        List<String> expected = List.of("A", "B", "C", "D", "E");
        assertEquals(expected, fieldNames);
    }

    @Test
    public void testRollover() {
        AlphabeticFieldNameGenerator generator = AlphabeticFieldNameGenerator.create();
        generator.generate(35);

        List<String> fieldNames = generator.getFieldNames();
        List<String> expected = List.of("AA", "AB", "AC", "AD", "AE");
        assertEquals(35, fieldNames.size());
        assertTrue(fieldNames.containsAll(expected));
    }

    @Test
    public void testSecondLevelRollover() {
        AlphabeticFieldNameGenerator generator = AlphabeticFieldNameGenerator.create();
        generator.generate(53);

        List<String> fieldNames = generator.getFieldNames();
        assertEquals(53, fieldNames.size());
        assertEquals("AZ", fieldNames.get(51));
        assertEquals("BA", fieldNames.get(52));
    }
}
