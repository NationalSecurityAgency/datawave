package datawave.query.data.parsers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.Test;

public class ShardIndexKeyTest {

    private final Key shardIndexKey = new Key("value", "FIELD", "20241021_0\u0000datatype");

    private final KeyParser parser = new ShardIndexKey();

    @Test
    public void testShardIndexKeyParse() {
        parser.parse(shardIndexKey);

        assertEquals("FIELD", parser.getField());
        assertEquals("value", parser.getValue());
        assertEquals("datatype", parser.getDatatype());
    }

    @Test
    public void testUidException() {
        parser.parse(shardIndexKey);
        assertThrows(UnsupportedOperationException.class, parser::getUid);
    }

    @Test
    public void testRootUidException() {
        parser.parse(shardIndexKey);
        assertThrows(UnsupportedOperationException.class, parser::getRootUid);
    }

    @Test
    public void testKeyEquals() {
        parser.parse(shardIndexKey);
        assertEquals(shardIndexKey, parser.getKey());
    }
}
