package datawave.query.tables.keyword.extractor;

import static datawave.query.function.JexlEvaluation.HIT_TERM_FIELD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.query.attributes.Content;
import datawave.util.keyword.TagCloudPartition;

public class FieldedTagCloudInputExtractorTest {
    public static final Key KEY = new Key("row", "dataType" + '\u0000' + "123.234.345");
    private FieldedTagCloudInputExtractor extractor;

    @BeforeEach
    public void setup() {
        extractor = new FieldedTagCloudInputExtractor();
        extractor.setCategory("animals");
        extractor.setFields(List.of("A"));
    }

    @Test
    public void nullFieldsTest() {
        extractor.setFields(null);
        TagCloudPartition partition = extractor.get();

        assertNull(partition);
    }

    @Test
    public void noExtractionTest() {
        extractor.extract(KEY, Map.of("B", new Content("found", KEY, true)));
        TagCloudPartition partition = extractor.get();
        assertNull(partition);

        extractor.extract(KEY, Map.of());
        partition = extractor.get();
        assertNull(partition);
    }

    @Test
    public void nullSubTypeTest() {
        extractor.extract(KEY, Map.of("A", new Content("found", KEY, true)));
        TagCloudPartition partition = extractor.get();

        assertFalse(partition.getInputs().get(0).getMetadata().containsKey("subType"));
        assertTrue(partition.getInputs().get(0).getMetadata().containsKey("type"));
        assertEquals("animals", partition.getInputs().get(0).getMetadata().get("type"));
    }

    @Test
    public void withSubTypeTest() {
        extractor.setSubType("mammals");

        extractor.extract(KEY, Map.of("A", new Content("found", KEY, true)));
        TagCloudPartition partition = extractor.get();

        assertTrue(partition.getInputs().get(0).getMetadata().containsKey("subType"));
        assertEquals("mammals", partition.getInputs().get(0).getMetadata().get("subType"));
        assertTrue(partition.getInputs().get(0).getMetadata().containsKey("type"));
        assertEquals("animals", partition.getInputs().get(0).getMetadata().get("type"));
    }

    @Test
    public void noHitTermDocIdTest() {
        extractor.extract(KEY, Map.of("A", new Content("found", KEY, true)));
        TagCloudPartition partition = extractor.get();

        assertEquals("row/dataType/123.234.345", partition.getInputs().get(0).getSource());
    }

    @Test
    public void hitTermDocIdTest() {
        extractor.extract(KEY, Map.of(HIT_TERM_FIELD, new Content("UUID:ABC", KEY, true), "A", new Content("found", KEY, true)));
        TagCloudPartition partition = extractor.get();

        assertEquals("UUID:ABC", partition.getInputs().get(0).getSource());
    }
}
