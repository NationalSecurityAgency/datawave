package datawave.query.attributes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.accumulo.core.data.Key;
import org.junit.Test;

public class ContentTest extends AttributeTest {

    private final Key expectedMetadata = new Key("row", "datatype\0uid");

    @Test
    public void testMetadataFromTermFrequencyKey() {
        Key tfKey = new Key("row", "tf", "datatype\0uid\0value\0FIELD");
        Content content = new Content("value", tfKey, true);
        assertEquals(expectedMetadata, content.getMetadata());

        // value contains nulls
        tfKey = new Key("row", "tf", "datatype\0uid\0val\0ue\0FIELD");
        content = new Content("value", tfKey, true);
        assertEquals(expectedMetadata, content.getMetadata());
    }

    @Test
    public void validateSerializationOfToKeepFlag() {
        String value = "some random value";
        Key docKey = new Key("shard", "datatype\0uid");

        Content attr = new Content(value, docKey, false);
        testToKeep(attr, false);

        attr = new Content(value, docKey, true);
        testToKeep(attr, true);
    }

    @Test
    public void testSourceSerialization() {
        Content content = new Content("derivativeValue", new Key("derivativeValue"), false,
                        new Content("sourceValue", new Key("sourceKey", "sourceCf", "sourceCq", "sourceVis"), true));

        // test kryo
        Attribute<?> deserialized = serializeKryo(content);
        validateSourceSerialization(deserialized);

        // test regular
        deserialized = serialize(content);
        validateSourceSerialization(deserialized);
    }

    private void validateSourceSerialization(Attribute<?> deserialized) {
        assertTrue(deserialized instanceof Content);
        Content deserializedContent = (Content) deserialized;
        assertEquals("derivativeValue", deserializedContent.getContent());
        assertTrue(deserializedContent.getSource() != null);
        assertTrue(deserializedContent.getSource() instanceof Content);
        Content sourceContent = (Content) deserializedContent.getSource();
        assertEquals("sourceValue", sourceContent.getContent());
        assertTrue(sourceContent.isMetadataSet());
        // the only thing preserved is the visibility
        assertEquals("", sourceContent.getMetadata().getRow().toString());
        assertEquals("", sourceContent.getMetadata().getColumnFamily().toString());
        assertEquals("", sourceContent.getMetadata().getColumnQualifier().toString());
        assertEquals("sourceVis", sourceContent.getMetadata().getColumnVisibility().toString());
        assertEquals(true, sourceContent.isToKeep());
    }
}
