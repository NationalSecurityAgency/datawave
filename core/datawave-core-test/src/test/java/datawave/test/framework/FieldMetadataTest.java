package datawave.test.framework;

import static datawave.test.framework.util.MetadataColumn.E;
import static datawave.test.framework.util.MetadataColumn.I;
import static datawave.test.framework.util.MetadataColumn.T;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import datawave.data.type.LcNoDiacriticsType;
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

    /**
     * The type column is implied by the presence of normalizers, so neither setter may depend on being called first.
     */
    @Test
    public void testTypeColumnAddedRegardlessOfSetterOrder() {
        FieldMetadata columnsFirst = new FieldMetadata("A");
        columnsFirst.setMetadataColumns(List.of(I, E));
        columnsFirst.setNormalizers(List.of(new LcNoDiacriticsType()));
        assertTrue(columnsFirst.getMetadataColumns().contains(T), "type column missing when columns were set first");

        FieldMetadata normalizersFirst = new FieldMetadata("B");
        normalizersFirst.setNormalizers(List.of(new LcNoDiacriticsType()));
        normalizersFirst.setMetadataColumns(List.of(I, E));
        assertTrue(normalizersFirst.getMetadataColumns().contains(T), "type column missing when normalizers were set first");

        assertEquals(columnsFirst.getMetadataColumns(), normalizersFirst.getMetadataColumns());
    }

    /**
     * An empty value list would make the value-to-event mapping divide by zero, so it is rejected where it enters.
     */
    @Test
    public void testEmptyValuesRejected() {
        FieldMetadata metadata = new FieldMetadata("A");

        Exception e1 = assertThrows(IllegalArgumentException.class, () -> metadata.setValues(Collections.emptyList()));
        assertEquals("values cannot be empty", e1.getMessage());

        metadata.setValueGenerator(PresetGenerator.of(List.of("a")));
        metadata.setEventIdGenerator(ModuloEventIdGenerator.create(1));
        Exception e2 = assertThrows(IllegalArgumentException.class, () -> metadata.populateValues(5, 0));
        assertEquals("valuesPerField must be greater than 0", e2.getMessage());
    }

    /**
     * The value to event id mapping is indexed on first use, so it has to be discarded when either side of the mapping is replaced.
     */
    @Test
    public void testEventIdsForValueReflectsLaterChanges() {
        FieldMetadata metadata = new FieldMetadata("A");
        metadata.setValues(List.of("a", "b"));
        metadata.setEventIds(List.of(1, 2, 3, 4));

        assertEquals(List.of(1, 3), metadata.getEventIdsForValue("a"));

        // replacing the values must not serve the previously indexed mapping
        metadata.setValues(List.of("a"));
        assertEquals(List.of(1, 2, 3, 4), metadata.getEventIdsForValue("a"));

        // and neither must replacing the event ids
        metadata.setEventIds(List.of(7, 8));
        assertEquals(List.of(7, 8), metadata.getEventIdsForValue("a"));

        // a value with no backing events resolves to empty rather than failing
        assertEquals(List.of(), metadata.getEventIdsForValue("absent"));
    }
}
