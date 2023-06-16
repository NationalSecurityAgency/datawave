package datawave.query.attributes;

import datawave.data.type.NoOpType;
import org.apache.accumulo.core.data.Key;
import org.junit.Test;

public class TypeAttributeTest extends AttributeTest {

    @Test
    public void validateSerializationOfToKeepFlag() {
        NoOpType type = new NoOpType("no op value");
        Key docKey = new Key("shard", "datatype\0uid");

        TypeAttribute<?> attr = new TypeAttribute<>(type, docKey, false);
        testToKeep(attr, false);

        attr = new TypeAttribute<>(type, docKey, true);
        testToKeep(attr, true);
    }
}
