package datawave.test.framework;

import static datawave.test.framework.util.MetadataColumn.E;
import static datawave.test.framework.util.MetadataColumn.I;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.test.framework.generators.id.SequentialEventIdGenerator;
import datawave.test.framework.generators.value.PresetGenerator;

class QueryFieldMetadataTest {

    private QueryFieldMetadata queryFieldMetadata;

    @BeforeEach
    public void beforeEach() {
        FieldMetadata indexed = createIndexed();
        FieldMetadata indexOnly = createIndexOnly();
        FieldMetadata eventOnly = createEventOnly();
        FieldMetadata id = createId();

        List<FieldMetadata> fieldMetadata = List.of(indexed, indexOnly, eventOnly, id);
        queryFieldMetadata = QueryFieldMetadata.of(fieldMetadata);
    }

    @Test
    void testGetIndexed() {
        List<FieldMetadata> results = queryFieldMetadata.getIndexed();

        FieldMetadata indexed = createIndexed();
        FieldMetadata indexOnly = createIndexOnly();
        List<FieldMetadata> expected = List.of(indexed, indexOnly);
        assertEquals(expected, results);
    }

    /**
     * The ID field is indexed like a regular content field, but it is a synthetic per-event unique identifier, not a content field under test. It must be
     * excluded from the generic classification groups so its always-scales-with-event-count value count doesn't leak into the query counts computed from these
     * groups.
     */
    @Test
    void testGetIndexedExcludesIdField() {
        List<FieldMetadata> results = queryFieldMetadata.getIndexed();
        assertEquals(List.of(), results.stream().filter(f -> f.getFieldName().equals(IngestMetadata.ID_FIELD_NAME)).collect(Collectors.toList()));
    }

    @Test
    void testGetId() {
        List<FieldMetadata> results = queryFieldMetadata.getId();
        assertEquals(List.of(createId()), results);
    }

    @Test
    void getIndexOnly() {
        List<FieldMetadata> results = queryFieldMetadata.getIndexOnly();
        List<FieldMetadata> expected = List.of(createIndexOnly());
        assertEquals(expected, results);
    }

    @Test
    void testGetEventOnly() {
        List<FieldMetadata> results = queryFieldMetadata.getEventOnly();
        List<FieldMetadata> expected = List.of(createEventOnly());
        assertEquals(expected, results);
    }

    @Test
    void getValueCountForEntry() {
        int count = queryFieldMetadata.getValueCountForEntry(Map.entry("EVENT_ONLY", "e"));
        assertEquals(2, count);
    }

    private FieldMetadata createIndexed() {
        FieldMetadata indexed = new FieldMetadata("INDEXED");
        indexed.setMetadataColumns(List.of(I, E));
        indexed.setValueGenerator(PresetGenerator.of(List.of("a", "b")));
        indexed.setEventIdGenerator(SequentialEventIdGenerator.create());
        indexed.populateValues(2, 2);
        return indexed;
    }

    /**
     * The obvious usage passes {@code IngestMetadata#getFieldMetadata()} straight in, which hands over the live list. A view that shifted afterwards would
     * silently change the expected results a query was built against.
     */
    @Test
    void testOfCopiesTheSourceList() {
        List<FieldMetadata> source = new ArrayList<>(List.of(createIndexed()));
        QueryFieldMetadata view = QueryFieldMetadata.of(source);

        source.add(createEventOnly());

        assertEquals(List.of(createIndexed()), view.getIndexed());
    }

    private FieldMetadata createIndexOnly() {
        FieldMetadata indexOnly = new FieldMetadata("INDEX_ONLY");
        indexOnly.setMetadataColumns(List.of(I));
        indexOnly.setValueGenerator(PresetGenerator.of(List.of("c", "d")));
        indexOnly.setEventIdGenerator(SequentialEventIdGenerator.create());
        indexOnly.populateValues(2, 2);
        return indexOnly;
    }

    private FieldMetadata createEventOnly() {
        FieldMetadata eventOnly = new FieldMetadata("EVENT_ONLY");
        eventOnly.setMetadataColumns(List.of(E));
        eventOnly.setValueGenerator(PresetGenerator.of(List.of("e")));
        eventOnly.setEventIdGenerator(SequentialEventIdGenerator.create());
        eventOnly.populateValues(2, 2);
        return eventOnly;
    }

    private FieldMetadata createId() {
        FieldMetadata id = new FieldMetadata(IngestMetadata.ID_FIELD_NAME);
        id.setMetadataColumns(List.of(I, E));
        id.setValueGenerator(PresetGenerator.of(List.of("1", "2")));
        id.setEventIdGenerator(SequentialEventIdGenerator.create());
        id.populateValues(2, 2);
        return id;
    }
}
