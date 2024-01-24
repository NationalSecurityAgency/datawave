package datawave.query.attributes;

import org.apache.accumulo.core.data.Key;
import org.junit.Test;

public class DiacriticContentTest extends AttributeTest {

    @Test
    public void validateSerializationOfToKeepFlag() {
        String value = "diacritic content";
        Key docKey = new Key("shard", "datatype\0uid");

        DiacriticContent attr = new DiacriticContent(value, docKey, false);
        testToKeep(attr, false);

        attr = new DiacriticContent(value, docKey, true);
        testToKeep(attr, true);
    }
}
