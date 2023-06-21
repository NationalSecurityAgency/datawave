package datawave.query.attributes;

import org.apache.accumulo.core.data.Key;
import org.junit.Test;

public class NumericTest extends AttributeTest {

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
