package datawave.query.attributes;

import org.apache.accumulo.core.data.Key;
import org.junit.Test;

public class DocumentKeyTest extends AttributeTest {

    @Test
    public void validateSerializationOfToKeepFlag() {
        Key docKey = new Key("shard", "datatype\0uid");

        DocumentKey attr = new DocumentKey(docKey, false);
        testToKeep(attr, false);

        attr = new DocumentKey(docKey, true);
        testToKeep(attr, true);
    }
}
