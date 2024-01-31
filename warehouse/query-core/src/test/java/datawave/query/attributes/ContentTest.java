package datawave.query.attributes;

import static org.junit.Assert.assertEquals;

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
}
