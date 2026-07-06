package datawave.test.framework;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Test;

import datawave.test.framework.generators.id.ModuloEventIdGenerator;
import datawave.test.framework.generators.value.PresetGenerator;

public class FieldMetadataTest {

    @Test
    public void testNullFieldName() {
        Exception e = assertThrows(NullPointerException.class, () -> new FieldMetadata(null));
        assertEquals("FieldName cannot be null", e.getMessage());
    }

    @Test
    public void testPopulateModuloValues() {

        FieldMetadata metadata = new FieldMetadata("A");
        metadata.setValueGenerator(PresetGenerator.of(List.of("a", "b")));
        metadata.setEventIdGenerator(ModuloEventIdGenerator.create(2));

        // should generate {a, b, a, b, a}
        metadata.populateValues(9, 2);

        // verify frequencies
        List<Integer> frequencies = metadata.getEventIds();
        List<Integer> expectedFrequencies = List.of(2, 4, 6, 8);
        assertEquals(expectedFrequencies, frequencies);

        // verify values
        List<String> values = metadata.getValues();
        List<String> expectedValues = List.of("a", "b");
        assertEquals(expectedValues, values);

        // verify value via index
        for (int i = 0; i < frequencies.size(); i++) {
            int frequency = frequencies.get(i);
            String value = metadata.getValueForEventId(frequency);
            String expected = i % 2 == 0 ? "a" : "b";
            assertEquals(expected, value, "frequency " + frequency + " returned wrong value");
        }

        // verify frequencies per value
        List<Integer> frequenciesA = metadata.getEventIdsForValue("a");
        assertEquals(List.of(2, 6), frequenciesA);

        List<Integer> frequenciesB = metadata.getEventIdsForValue("b");
        assertEquals(List.of(4, 8), frequenciesB);
    }

    /**
     * valuesPerField is a fixed property of the field's configuration. Query generators count {@code values.size()} to determine how many queries to emit per
     * field, so that count must never shrink just because a small event count backs fewer of the field's values with an actual event.
     */
    @Test
    public void testValuesPerFieldIsConstantRegardlessOfEventCount() {
        FieldMetadata metadata = new FieldMetadata("A");
        metadata.setValueGenerator(PresetGenerator.of(List.of("a", "b")));
        metadata.setEventIdGenerator(ModuloEventIdGenerator.create(1));

        // only 1 event exists, so only one of the two values can ever map to an actual event; the value count must still be 2
        metadata.populateValues(1, 2);

        assertEquals(List.of("a", "b"), metadata.getValues());
    }
}
