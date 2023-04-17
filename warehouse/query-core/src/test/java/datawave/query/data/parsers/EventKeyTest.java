package datawave.query.data.parsers;

import org.apache.accumulo.core.data.Key;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class EventKeyTest {
    
    private final Key eventKey = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok", "FIELD\0value");
    private final Key eventKeyWithNullsInValue = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok", "FIELD\0v\0a\0l\0u\0e");
    private final Key eventKeyWithChildUid = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok.12.34", "FIELD\0value");
    private final Key eventKeyWithChildUidNoDashes = new Key("row", "datatype\0d8zay2.3pnndm.anolok.12.34", "FIELD\0value");
    
    private final Key eventKeyWithNoCf = new Key("row", "", "FIELD\0value");
    private final Key eventKeyWithNoCq = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok", "");
    
    private final Key eventKeyWithMalformedCf = new Key("row", "datatype", "FIELD\0value");
    private final Key eventKeyWithMalformedCq = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok", "FIELD");
    
    private final EventKey parser = new EventKey();
    
    @Test
    public void testEventKeyParse() {
        parser.parse(eventKey);
        assertNormalKey(parser);
    }
    
    @Test
    public void testEventKeyWithNullsInValue() {
        parser.parse(eventKeyWithNullsInValue);
        assertKeyWithNullsInValue(parser);
    }
    
    @Test
    public void testEventKeyWithChildUid() {
        parser.parse(eventKeyWithChildUid);
        assertKeyWithChildUid(parser);
    }
    
    @Test
    public void testEventKeyWithChildUidNoDashes() {
        parser.parse(eventKeyWithChildUidNoDashes);
        assertKeyWithChildUidNoDashes(parser);
    }
    
    @Test
    public void testRepeatedParsing() {
        parser.parse(eventKey);
        assertNormalKey(parser);
        
        parser.parse(eventKeyWithNullsInValue);
        assertKeyWithNullsInValue(parser);
        
        parser.parse(eventKeyWithChildUid);
        assertKeyWithChildUid(parser);
        
        parser.parse(eventKeyWithChildUidNoDashes);
        assertKeyWithChildUidNoDashes(parser);
    }
    
    private void assertNormalKey(KeyParser parser) {
        assertEquals("datatype", parser.getDatatype());
        assertEquals("d8zay2.-3pnndm.-anolok", parser.getUid());
        assertEquals("d8zay2.-3pnndm.-anolok", parser.getRootUid());
        assertEquals("FIELD", parser.getField());
        assertEquals("value", parser.getValue());
    }
    
    private void assertKeyWithNullsInValue(KeyParser parser) {
        assertEquals("datatype", parser.getDatatype());
        assertEquals("d8zay2.-3pnndm.-anolok", parser.getUid());
        assertEquals("d8zay2.-3pnndm.-anolok", parser.getRootUid());
        assertEquals("FIELD", parser.getField());
        assertEquals("v\0a\0l\0u\0e", parser.getValue());
    }
    
    private void assertKeyWithChildUid(KeyParser parser) {
        assertEquals("datatype", parser.getDatatype());
        assertEquals("d8zay2.-3pnndm.-anolok.12.34", parser.getUid());
        assertEquals("d8zay2.-3pnndm.-anolok", parser.getRootUid());
        assertEquals("FIELD", parser.getField());
        assertEquals("value", parser.getValue());
    }
    
    private void assertKeyWithChildUidNoDashes(KeyParser parser) {
        assertEquals("datatype", parser.getDatatype());
        assertEquals("d8zay2.3pnndm.anolok.12.34", parser.getUid());
        assertEquals("d8zay2.3pnndm.anolok", parser.getRootUid());
        assertEquals("FIELD", parser.getField());
        assertEquals("value", parser.getValue());
    }
    
    @Test
    public void testKeyWithNoCf() {
        parser.parse(eventKeyWithNoCf);
        
        // cq parses cleanly
        assertEquals("FIELD", parser.getField());
        assertEquals("value", parser.getValue());
        
        // cf is invalid, exceptions are thrown
        assertThrows(IllegalArgumentException.class, parser::getDatatype);
        assertThrows(IllegalArgumentException.class, parser::getUid);
        assertThrows(IllegalArgumentException.class, parser::getRootUid);
    }
    
    @Test
    public void testKeyWithNoCq() {
        parser.parse(eventKeyWithNoCq);
        
        assertEquals("datatype", parser.getDatatype());
        assertEquals("d8zay2.-3pnndm.-anolok", parser.getUid());
        assertEquals("d8zay2.-3pnndm.-anolok", parser.getRootUid());
        
        assertThrows(IllegalArgumentException.class, parser::getField);
        assertThrows(IllegalArgumentException.class, parser::getValue);
    }
    
    @Test
    public void testKeyWithMalformedCf() {
        parser.parse(eventKeyWithMalformedCf);
        
        // cq parses cleanly
        assertEquals("FIELD", parser.getField());
        assertEquals("value", parser.getValue());
        
        // cf is invalid, exceptions will be thrown
        assertThrows(IllegalArgumentException.class, parser::getDatatype);
        assertThrows(IllegalArgumentException.class, parser::getUid);
        assertThrows(IllegalArgumentException.class, parser::getRootUid);
        
    }
    
    @Test
    public void testKeyWithMalformedCq() {
        parser.parse(eventKeyWithMalformedCq);
        
        assertEquals("datatype", parser.getDatatype());
        assertEquals("d8zay2.-3pnndm.-anolok", parser.getUid());
        assertEquals("d8zay2.-3pnndm.-anolok", parser.getRootUid());
        
        assertThrows(IllegalArgumentException.class, parser::getField);
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
    }
}
