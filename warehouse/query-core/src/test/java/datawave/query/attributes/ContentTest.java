package datawave.query.attributes;

import org.apache.accumulo.core.data.Key;
import org.junit.Test;

public class ContentTest extends AttributeTest {

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
