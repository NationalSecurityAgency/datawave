package datawave.query.tables.keyword.extractor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.util.keyword.TagCloudPartition;

public class FieldedTagCloudInputExtractorTest {

    private FieldedTagCloudInputExtractor extractor;

    @BeforeEach
    public void setup() {
        extractor = new FieldedTagCloudInputExtractor();
        extractor.setCategory("animals");
    }

    @Test
    public void nullSubTypeTest() {
        extractor.extract(new Key("row", "dataType" + '\u0000' + "123.234.345"), Map.of());
        TagCloudPartition partition = extractor.get();

        assertFalse(partition.getInputs().get(0).getMetadata().containsKey("subType"));
        assertTrue(partition.getInputs().get(0).getMetadata().containsKey("type"));
        assertEquals("animals", partition.getInputs().get(0).getMetadata().get("type"));
    }

    @Test
    public void withSubTypeTest() {
        extractor.setSubType("mammals");

        extractor.extract(new Key("row", "dataType" + '\u0000' + "123.234.345"), Map.of());
        TagCloudPartition partition = extractor.get();

        assertTrue(partition.getInputs().get(0).getMetadata().containsKey("subType"));
        assertEquals("mammals", partition.getInputs().get(0).getMetadata().get("subType"));
        assertTrue(partition.getInputs().get(0).getMetadata().containsKey("type"));
        assertEquals("animals", partition.getInputs().get(0).getMetadata().get("type"));
    }
}
