package datawave.query.attributes;

import org.apache.accumulo.core.data.Key;
import org.junit.Test;

public class PreNormalizedAttributeTest extends AttributeTest {

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
