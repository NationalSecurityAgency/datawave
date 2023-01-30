package datawave.query.data.parsers;

import org.apache.accumulo.core.data.Key;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FieldIndexKeyTest {
    
    private final Key fiKey = new Key("row", "fi\0FIELD", "value\0datatype\0uid");
    private final Key fiKeyWithNullsInValue = new Key("row", "fi\0FIELD", "v\0al\0ue\0datatype\0uid");
    private final Key fiKeyWithChildUid = new Key("row", "fi\0FIELD", "value\0datatype\0uid.12.37");
    
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
}
