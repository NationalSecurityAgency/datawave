package datawave.query.data.parsers;

import org.apache.accumulo.core.data.Key;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class TermFrequencyKeyTest {

    private final Key tfKey = new Key("row", "tf", "datatype\0d8zay2.-3pnndm.-anolok\0value\0FIELD");
    private final Key tfKeyWithNullsInValue = new Key("row", "tf", "datatype\0d8zay2.-3pnndm.-anolok\0v\0a\0l\0u\0e\0FIELD");
    private final Key tfKeyWithChildUid = new Key("row", "tf", "datatype\0d8zay2.-3pnndm.-anolok.12.34\0value\0FIELD");
    private final Key tfKeyWithChildUidNoDashes = new Key("row", "tf", "datatype\0d8zay2.3pnndm.anolok.12.34\0value\0FIELD");

    private final Key tfKeyWithNoCf = new Key("row", "", "datatype\0d8zay2.-3pnndm.-anolok.12.34\0value\0FIELD");
    private final Key tfKeyWithNoCq = new Key("row", "tf", "");

    private final Key tfKeyWithMalformedCf = new Key("row", "t\0 f", "datatype\0d8zay2.-3pnndm.-anolok.12.34\0value\0FIELD");
    private final Key tfKeyWithMalformedCq = new Key("row", "tf", "datatype\0\0FIELD");

    private final TermFrequencyKey parser = new TermFrequencyKey();

    @Test
    public void testSimpleParse() {
        parser.parse(tfKey);
        assertNormalKey(parser);
    }

    @Test
    public void testParseValueWithNulls() {
        parser.parse(tfKeyWithNullsInValue);
        assertValueWithNulls(parser);
    }

    @Test
    public void testParseChildUid() {
        parser.parse(tfKeyWithChildUid);
        assertChildUid(parser);
    }

    @Test
    public void testParseChildUidWithNoDashes() {
        parser.parse(tfKeyWithChildUidNoDashes);
        assertChildUidNoDashes(parser);
    }

    @Test
    public void testKeyWithNoCf() {
        parser.parse(tfKeyWithNoCf);
        assertEquals("datatype", parser.getDatatype());
        assertEquals("d8zay2.-3pnndm.-anolok.12.34", parser.getUid());
        assertEquals("d8zay2.-3pnndm.-anolok", parser.getRootUid());
        assertEquals("FIELD", parser.getField());
        assertEquals("value", parser.getValue());
        assertEquals(tfKeyWithNoCf, parser.getKey());
    }

    @Test
    public void testKeyWithNoCq() {
        parser.parse(tfKeyWithNoCq);
        assertThrows(IllegalArgumentException.class, parser::getDatatype);
        assertThrows(IllegalArgumentException.class, parser::getUid);
        assertThrows(IllegalArgumentException.class, parser::getRootUid);
        assertThrows(IllegalArgumentException.class, parser::getField);
        assertThrows(IllegalArgumentException.class, parser::getValue);
        assertThrows(IllegalArgumentException.class, parser::getUidAndValue);
        assertEquals(tfKeyWithNoCq, parser.getKey());
    }

    @Test
    public void testKeyWithMalformedCf() {
        parser.parse(tfKeyWithMalformedCf);
        assertEquals("datatype", parser.getDatatype());
        assertEquals("d8zay2.-3pnndm.-anolok.12.34", parser.getUid());
        assertEquals("d8zay2.-3pnndm.-anolok", parser.getRootUid());
        assertEquals("FIELD", parser.getField());
        assertEquals("value", parser.getValue());
        assertEquals(tfKeyWithMalformedCf, parser.getKey());
    }

    @Test
    public void testKeyWithMalformedCq() {
        parser.parse(tfKeyWithMalformedCq);
        assertEquals("datatype", parser.getDatatype());
        assertEquals("", parser.getUid());
        assertEquals("", parser.getRootUid());
        assertEquals("FIELD", parser.getField());
        assertThrows(IllegalArgumentException.class, parser::getValue);
        assertThrows(IllegalArgumentException.class, parser::getUidAndValue);
        assertEquals(tfKeyWithMalformedCq, parser.getKey());
    }

    @Test
    public void testRepeatedParse() {
        parser.parse(tfKey);
        assertNormalKey(parser);

        parser.parse(tfKeyWithNullsInValue);
        assertValueWithNulls(parser);

        parser.parse(tfKeyWithChildUid);
        assertChildUid(parser);
    }

    @Test
    public void testParseNullKey() {
        parser.parse(null);
        assertNull(parser.getKey());
        assertThrows(IllegalArgumentException.class, parser::getDatatype);
        assertThrows(IllegalArgumentException.class, parser::getUid);
        assertThrows(IllegalArgumentException.class, parser::getRootUid);
        assertThrows(IllegalArgumentException.class, parser::getField);
        assertThrows(IllegalArgumentException.class, parser::getValue);
    }

    private void assertNormalKey(KeyParser parser) {
        assertEquals("datatype", parser.getDatatype());
        assertEquals("d8zay2.-3pnndm.-anolok", parser.getUid());
        assertEquals("d8zay2.-3pnndm.-anolok", parser.getRootUid());
        assertEquals("FIELD", parser.getField());
        assertEquals("value", parser.getValue());
        assertEquals(tfKey, parser.getKey());
    }

    private void assertValueWithNulls(KeyParser parser) {
        assertEquals("datatype", parser.getDatatype());
        assertEquals("d8zay2.-3pnndm.-anolok", parser.getUid());
        assertEquals("d8zay2.-3pnndm.-anolok", parser.getRootUid());
        assertEquals("FIELD", parser.getField());
        assertEquals("v\0a\0l\0u\0e", parser.getValue());
        assertEquals(tfKeyWithNullsInValue, parser.getKey());
    }

    private void assertChildUid(KeyParser parser) {
        assertEquals("datatype", parser.getDatatype());
        assertEquals("d8zay2.-3pnndm.-anolok.12.34", parser.getUid());
        assertEquals("d8zay2.-3pnndm.-anolok", parser.getRootUid());
        assertEquals("FIELD", parser.getField());
        assertEquals("value", parser.getValue());
        assertEquals(tfKeyWithChildUid, parser.getKey());
    }

    private void assertChildUidNoDashes(KeyParser parser) {
        assertEquals("datatype", parser.getDatatype());
        assertEquals("d8zay2.3pnndm.anolok.12.34", parser.getUid());
        assertEquals("d8zay2.3pnndm.anolok", parser.getRootUid());
        assertEquals("FIELD", parser.getField());
        assertEquals("value", parser.getValue());
        assertEquals(tfKeyWithChildUidNoDashes, parser.getKey());
    }

    @Test
    public void testParse_missingColumnQualifier() {
        Key k = new Key("row", "tf");
        parser.parse(k);

        assertThrows(IllegalArgumentException.class, parser::getDatatype);
        assertThrows(IllegalArgumentException.class, parser::getUid);
        assertThrows(IllegalArgumentException.class, parser::getRootUid);
        assertThrows(IllegalArgumentException.class, parser::getField);
        assertThrows(IllegalArgumentException.class, parser::getValue);
        assertEquals(k, parser.getKey());
    }
}
