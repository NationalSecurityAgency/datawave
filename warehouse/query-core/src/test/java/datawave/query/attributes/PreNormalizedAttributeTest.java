package datawave.query.attributes;

import static org.junit.Assert.assertEquals;

import org.apache.accumulo.core.data.Key;
import org.junit.Test;

public class PreNormalizedAttributeTest extends AttributeTest {

    @Test
    public void testMetadataFromFieldIndexKey() {
        Key fiKey = new Key("row", "fi\0FIELD", "value\0datatype\0uid");
        PreNormalizedAttribute attribute = new PreNormalizedAttribute("value", fiKey, true);

        Key expectedMetadata = new Key("row", "datatype\0uid");
        assertEquals(expectedMetadata, attribute.getMetadata());

        // also test a value with nulls in it
        fiKey = new Key("row", "fi\0FIELD", "va\0lue\0datatype\0uid");
        attribute = new PreNormalizedAttribute("value", fiKey, true);
        assertEquals(expectedMetadata, attribute.getMetadata());
    }

    @Test
    public void validateSerializationOfToKeepFlag() {
        String value = "some random value";
        Key docKey = new Key("shard", "datatype\0uid");

        PreNormalizedAttribute attr = new PreNormalizedAttribute(value, docKey, false);
        testToKeep(attr, false);

        attr = new PreNormalizedAttribute(value, docKey, true);
        testToKeep(attr, true);
    }
}
