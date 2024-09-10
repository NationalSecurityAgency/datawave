package datawave.query.attributes;

import org.apache.accumulo.core.data.Key;
import org.junit.Test;

public class IpAddressTest extends AttributeTest {

    @Test
    public void validateSerializationOfToKeepFlag() {
        String value = "127.0.0.1";
        Key docKey = new Key("shard", "datatype\0uid");

        IpAddress attr = new IpAddress(value, docKey, false);
        testToKeep(attr, false);

        attr = new IpAddress(value, docKey, true);
        testToKeep(attr, true);
    }
}
