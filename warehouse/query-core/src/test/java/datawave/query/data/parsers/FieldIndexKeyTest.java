package datawave.query.data.parsers;

import org.apache.accumulo.core.data.Key;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class FieldIndexKeyTest {

    private final Key fiKey = new Key("row", "fi\0FIELD", "value\0datatype\0d8zay2.-3pnndm.-anolok");
    private final Key fiKeyWithNullsInValue = new Key("row", "fi\0FIELD", "v\0al\0ue\0datatype\0d8zay2.-3pnndm.-anolok");
    private final Key fiKeyWithChildUid = new Key("row", "fi\0FIELD", "value\0datatype\0d8zay2.-3pnndm.-anolok.12.37");

    private final Key fiKeyNoCf = new Key("row", "", "value\0datatype\0d8zay2.-3pnndm.-anolok");
    private final Key fiKeyNoCq = new Key("row", "fi\0FIELD", "");
    private final Key fiKeyCqOneNull = new Key("row", "fi\0FIELD", "value\0datatype");
    private final Key fiKeyCqZeroNulls = new Key("row", "fi\0FIELD", "value");

    private final FieldIndexKey parser = new FieldIndexKey();

    @Test
    public void testNormalParse() {
        parser.parse(fiKey);
        assertNormalKey();
    }

    @Test
    public void testChildUidParse() {
        parser.parse(fiKeyWithNullsInValue);
        asserKeyWithNullsInValue();
    }

    @Test
    public void testNullsInValueParse() {
        parser.parse(fiKeyWithChildUid);
        assertKeyWithChildUid();
    }

    @Test
    public void testRepeatedParsing() {
        parser.parse(fiKey);
        assertNormalKey();

        parser.parse(fiKeyWithNullsInValue);
        asserKeyWithNullsInValue();

        parser.parse(fiKeyWithChildUid);
        assertKeyWithChildUid();

        parser.parse(fiKey);
        assertNormalKey();

        // this should skip actual processing, since it was already parsed up once.

        Key dupeKey = new Key("row", "fi\0FIELD", "value\0datatype\0d8zay2.-3pnndm.-anolok");
        parser.parse(dupeKey);
        assertNotNull(parser.getDatatype());
        assertNotNull(parser.getValue());
        assertNotNull(parser.getField());
        assertNormalKey();
    }

    private void assertNormalKey() {
        assertEquals("datatype", parser.getDatatype());
        assertEquals("d8zay2.-3pnndm.-anolok", parser.getUid());
        assertEquals("d8zay2.-3pnndm.-anolok", parser.getRootUid());
        assertEquals("FIELD", parser.getField());
        assertEquals("value", parser.getValue());
        assertEquals(fiKey, parser.getKey());
    }

    private void asserKeyWithNullsInValue() {
        assertEquals("datatype", parser.getDatatype());
        assertEquals("d8zay2.-3pnndm.-anolok", parser.getUid());
        assertEquals("d8zay2.-3pnndm.-anolok", parser.getRootUid());
        assertEquals("FIELD", parser.getField());
        assertEquals("v\0al\0ue", parser.getValue());
        assertEquals(fiKeyWithNullsInValue, parser.getKey());
    }

    private void assertKeyWithChildUid() {
        assertEquals("datatype", parser.getDatatype());
        assertEquals("d8zay2.-3pnndm.-anolok.12.37", parser.getUid());
        assertEquals("d8zay2.-3pnndm.-anolok", parser.getRootUid());
        assertEquals("FIELD", parser.getField());
        assertEquals("value", parser.getValue());
        assertEquals(fiKeyWithChildUid, parser.getKey());
    }

    @Test
    public void testParseNoCf() {
        parser.parse(fiKeyNoCf);

        // column qualifier parse works fine
        assertEquals("datatype", parser.getDatatype());
        assertEquals("d8zay2.-3pnndm.-anolok", parser.getUid());
        assertEquals("d8zay2.-3pnndm.-anolok", parser.getRootUid());
        assertEquals("value", parser.getValue());
        assertEquals(fiKeyNoCf, parser.getKey());
        assertTrue("made it this far", true);

        // invalid column family means the parser will throw an exception
        assertThrows(IllegalArgumentException.class, parser::getField);
    }

    @Test
    public void testParseNoCq() {
        parser.parse(fiKeyNoCq);

        // parse field works
        assertEquals("FIELD", parser.getField());
        assertEquals(fiKeyNoCq, parser.getKey());

        // invalid column qualifier means the parser throws an exception
        assertThrows(IllegalArgumentException.class, parser::getDatatype);
        assertThrows(IllegalArgumentException.class, parser::getUid);
        assertThrows(IllegalArgumentException.class, parser::getRootUid);
        assertThrows(IllegalArgumentException.class, parser::getValue);
    }

    @Test
    public void testParseCqOneNull() {
        parser.parse(fiKeyCqOneNull);

        // parse field works
        assertEquals("FIELD", parser.getField());
        assertEquals(fiKeyCqOneNull, parser.getKey());

        // invalid column qualifier means the parser throws an exception
        assertThrows(IllegalArgumentException.class, parser::getDatatype);
        assertThrows(IllegalArgumentException.class, parser::getUid);
        assertThrows(IllegalArgumentException.class, parser::getRootUid);
        assertThrows(IllegalArgumentException.class, parser::getValue);
    }

    @Test
    public void testParseCqZeroNulls() {
        parser.parse(fiKeyCqZeroNulls);

        // parse field works
        assertEquals("FIELD", parser.getField());
        assertEquals(fiKeyCqZeroNulls, parser.getKey());

        // invalid column qualifier means the parser throws an exception
        assertThrows(IllegalArgumentException.class, parser::getDatatype);
        assertThrows(IllegalArgumentException.class, parser::getUid);
        assertThrows(IllegalArgumentException.class, parser::getRootUid);
        assertThrows(IllegalArgumentException.class, parser::getValue);
    }

    @Test
    public void testParseNullKey() {
        parser.parse(null);
        assertThrows(IllegalArgumentException.class, parser::getDatatype);
        assertThrows(IllegalArgumentException.class, parser::getUid);
        assertThrows(IllegalArgumentException.class, parser::getRootUid);
        assertThrows(IllegalArgumentException.class, parser::getField);
        assertThrows(IllegalArgumentException.class, parser::getValue);
        assertNull(parser.getKey());
    }
}
