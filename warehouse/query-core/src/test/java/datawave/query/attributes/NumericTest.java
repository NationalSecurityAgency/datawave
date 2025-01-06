package datawave.query.attributes;

import static org.junit.Assert.assertEquals;

import org.apache.accumulo.core.data.Key;
import org.junit.Test;

public class NumericTest extends AttributeTest {

    private final Key expectedMetadata = new Key("row", "datatype\0uid");

    @Test
    public void testMetadataFromFieldIndexKey() {
        Key fiKey = new Key("row", "fi\0FIELD", "42\0datatype\0uid");
        Numeric numeric = new Numeric("42", fiKey, true);
        assertEquals(expectedMetadata, numeric.getMetadata());

        // value contains null
        fiKey = new Key("row", "fi\0FIELD", "4\u00002\0datatype\0uid");
        numeric = new Numeric("42", fiKey, true);
        assertEquals(expectedMetadata, numeric.getMetadata());
    }

    @Test
    public void testMetadataFromEventKey() {
        Key eventKey = new Key("row", "datatype\0uid", "FIELD\u000042");
        Numeric numeric = new Numeric("42", eventKey, true);
        assertEquals(expectedMetadata, numeric.getMetadata());

        // value contains null
        eventKey = new Key("row", "datatype\0uid", "FIELD\u00004\u00002");
        numeric = new Numeric("42", eventKey, true);
        assertEquals(expectedMetadata, numeric.getMetadata());
    }

    @Test
    public void testMetadataFromTermFrequencyKey() {
        Key tfKey = new Key("row", "tf", "datatype\0uid\u000042\0FIELD");
        Numeric numeric = new Numeric("42", tfKey, true);
        assertEquals(expectedMetadata, numeric.getMetadata());

        // value contains null
        tfKey = new Key("row", "tf", "datatype\0uid\u00004\u00002\0FIELD");
        numeric = new Numeric("42", tfKey, true);
        assertEquals(expectedMetadata, numeric.getMetadata());
    }

    @Test
    public void validateSerializationOfToKeepFlag() {
        String value = "42";
        Key docKey = new Key("shard", "datatype\0uid");

        Numeric attr = new Numeric(value, docKey, false);
        testToKeep(attr, false);

        attr = new Numeric(value, docKey, true);
        testToKeep(attr, true);
    }
}
