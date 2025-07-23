package datawave.query.data.parsers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.Test;

public class ShardIndexKeyTest {

    private final Key siKey = new Key("value", "FIELD", "20241021_0\0datatype");
    private final Key siKeyWithNullsInValue = new Key("va\0lu\0e", "FIELD", "20241021_0\0datatype");

    private final KeyParser parser = new ShardIndexKey();

    @Test
    public void testNormalParse() {
        parser.parse(siKey);
        assertNormalParse();
    }

    @Test
    public void testParseWithNullsInValue() {
        parser.parse(siKeyWithNullsInValue);
        assertKeyWithNullsInValue();
    }

    @Test
    public void testRepeatedParse() {
        int max = 10;
        for (int i = 0; i < max; i++) {
            parser.parse(siKey);
            assertNormalParse();

            parser.parse(siKey);
            assertNormalParse();

            parser.parse(siKeyWithNullsInValue);
            assertKeyWithNullsInValue();
        }
    }

    private void assertNormalParse() {
        assertEquals("datatype", parser.getDatatype());
        assertEquals("value", parser.getValue());
        assertEquals("FIELD", parser.getField());
        assertInstanceOf(ShardIndexKey.class, parser);
        assertEquals("20241021_0", ((ShardIndexKey) parser).getShard());
        assertThrows(UnsupportedOperationException.class, parser::getUid);
        assertThrows(UnsupportedOperationException.class, parser::getRootUid);
        assertEquals(siKey, parser.getKey());
    }

    private void assertKeyWithNullsInValue() {
        assertEquals("datatype", parser.getDatatype());
        assertEquals("va\0lu\0e", parser.getValue());
        assertEquals("FIELD", parser.getField());
        assertInstanceOf(ShardIndexKey.class, parser);
        assertEquals("20241021_0", ((ShardIndexKey) parser).getShard());
        assertThrows(UnsupportedOperationException.class, parser::getUid);
        assertThrows(UnsupportedOperationException.class, parser::getRootUid);
        assertEquals(siKeyWithNullsInValue, parser.getKey());
    }
}
