package datawave.core.iterators.key.util;

import org.apache.accumulo.core.data.Key;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FiKeyUtilTest {
    
    private final Key fikey = new Key("row", "fi\0FIELD", "value\0datatype\0uid");
    private final Key fikeyWithNulls = new Key("row", "fi\0FIELD", "v\0a\0l\0u\0e\0datatype\0uid");
    private final Key fikeyWithChildUid = new Key("row", "fi\0FIELD", "value\0datatype\0uid.12.37");
    
    @Test
    public void testSimpleParse() {
        assertTrue(FiKeyUtil.instanceOf(fikey));
        assertEquals("FIELD", FiKeyUtil.getFieldString(fikey));
        assertEquals("value", FiKeyUtil.getValueString(fikey));
        assertEquals("datatype", FiKeyUtil.getDatatypeString(fikey));
        assertEquals("uid", FiKeyUtil.getUidString(fikey));
    }
    
    @Test
    public void testParsingValueWithNulls() {
        assertTrue(FiKeyUtil.instanceOf(fikey));
        assertEquals("FIELD", FiKeyUtil.getFieldString(fikeyWithNulls));
        assertEquals("v\0a\0l\0u\0e", FiKeyUtil.getValueString(fikeyWithNulls));
        assertEquals("datatype", FiKeyUtil.getDatatypeString(fikeyWithNulls));
        assertEquals("uid", FiKeyUtil.getUidString(fikeyWithNulls));
    }
    
    @Test
    public void testParseChildUid() {
        assertTrue(FiKeyUtil.instanceOf(fikey));
        assertEquals("FIELD", FiKeyUtil.getFieldString(fikeyWithChildUid));
        assertEquals("value", FiKeyUtil.getValueString(fikeyWithChildUid));
        assertEquals("datatype", FiKeyUtil.getDatatypeString(fikeyWithChildUid));
        assertEquals("uid.12.37", FiKeyUtil.getUidString(fikeyWithChildUid));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testParseNoValue() {
        Key key = new Key("row", "fi\0FIELD", "datatype\0uid");
        assertEquals("value", FiKeyUtil.getValueString(key));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testParseNoDatatype() {
        Key key = new Key("row", "fi\0FIELD", "value\0uid");
        assertEquals("datatype", FiKeyUtil.getDatatypeString(key));
    }
    
    // getUidString simply returns everything after the last null byte. If the column qualifier is wrong
    // then the method will return the wrong value
    @Test
    public void testParseNoUid() {
        Key key = new Key("row", "fi\0FIELD", "value\0datatype");
        assertEquals("datatype", FiKeyUtil.getUidString(key));
    }
    
}
