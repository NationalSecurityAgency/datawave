package datawave.query.data.parsers;

import org.apache.accumulo.core.data.Key;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class FieldIndexKeyTest {
    
    private final Key fiKey = new Key("row", "fi\0FIELD", "value\0datatype\0uid");
    private final Key fiKeyWithNullsInValue = new Key("row", "fi\0FIELD", "v\0al\0ue\0datatype\0uid");
    private final Key fiKeyWithChildUid = new Key("row", "fi\0FIELD", "value\0datatype\0uid.12.37");
    
    private final Key fiKeyNoCf = new Key("row", "", "value\0datatype\0uid");
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
    }
    
    private void assertNormalKey() {
        assertEquals("datatype", parser.getDatatype());
        assertEquals("uid", parser.getUid());
        assertEquals("FIELD", parser.getField());
        assertEquals("value", parser.getValue());
    }
    
    private void asserKeyWithNullsInValue() {
        assertEquals("datatype", parser.getDatatype());
        assertEquals("uid", parser.getUid());
        assertEquals("FIELD", parser.getField());
        assertEquals("v\0al\0ue", parser.getValue());
    }
    
    private void assertKeyWithChildUid() {
        assertEquals("datatype", parser.getDatatype());
        assertEquals("uid.12.37", parser.getUid());
        assertEquals("FIELD", parser.getField());
        assertEquals("value", parser.getValue());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testParseNoCf() {
        parser.parse(fiKeyNoCf);
        
        // column qualifier parse works fine
        assertEquals("datatype", parser.getDatatype());
        assertEquals("uid", parser.getUid());
        assertEquals("value", parser.getValue());
        assertTrue("made it this far", true);
        
        // invalid column family means the parser returns null values
        assertNull(parser.getField());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testParseNoCq() {
        parser.parse(fiKeyNoCq);
        
        // parse field works
        assertEquals("FIELD", parser.getField());
        
        // invalid column qualifier means the parser returns null values
        assertNull(parser.getDatatype());
        assertNull(parser.getUid());
        assertNull(parser.getValue());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testParseCqOneNull() {
        parser.parse(fiKeyCqOneNull);
        
        // parse field works
        assertEquals("FIELD", parser.getField());
        
        // invalid column qualifier means the parser returns null values
        assertNull(parser.getDatatype());
        assertNull(parser.getUid());
        assertNull(parser.getValue());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testParseCqZeroNulls() {
        parser.parse(fiKeyCqZeroNulls);
        
        // parse field works
        assertEquals("FIELD", parser.getField());
        
        // invalid column qualifier means the parser returns null values
        assertNull(parser.getDatatype());
        assertNull(parser.getUid());
        assertNull(parser.getValue());
    }
}
