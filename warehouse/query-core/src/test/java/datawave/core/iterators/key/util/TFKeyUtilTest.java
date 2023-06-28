package datawave.core.iterators.key.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.accumulo.core.data.Key;
import org.junit.Test;

public class TFKeyUtilTest {

    private final Key tfKey = new Key("row", "tf", "datatype\0uid\0value\0FIELD");
    private final Key tfKeyWithNulls = new Key("row", "tf", "datatype\0uid\0v\0a\0l\0u\0e\0FIELD");
    private final Key tfKeyWithChildUid = new Key("row", "tf", "datatype\0uid.11.22\0value\0FIELD");

    @Test
    public void testSimpleParse() {
        assertTrue(TFKeyUtil.instanceOf(tfKey));
        assertEquals("FIELD", TFKeyUtil.getFieldString(tfKey));
        assertEquals("value", TFKeyUtil.getValueString(tfKey));
        assertEquals("datatype", TFKeyUtil.getDatatypeString(tfKey));
        assertEquals("uid", TFKeyUtil.getUidString(tfKey));
    }

    @Test
    public void testParseValueWithNulls() {
        assertTrue(TFKeyUtil.instanceOf(tfKeyWithNulls));
        assertEquals("FIELD", TFKeyUtil.getFieldString(tfKeyWithNulls));
        assertEquals("v\0a\0l\0u\0e", TFKeyUtil.getValueString(tfKeyWithNulls));
        assertEquals("datatype", TFKeyUtil.getDatatypeString(tfKeyWithNulls));
        assertEquals("uid", TFKeyUtil.getUidString(tfKeyWithNulls));
    }

    @Test
    public void testParseChildUid() {
        assertTrue(TFKeyUtil.instanceOf(tfKeyWithChildUid));
        assertEquals("FIELD", TFKeyUtil.getFieldString(tfKeyWithChildUid));
        assertEquals("value", TFKeyUtil.getValueString(tfKeyWithChildUid));
        assertEquals("datatype", TFKeyUtil.getDatatypeString(tfKeyWithChildUid));
        assertEquals("uid.11.22", TFKeyUtil.getUidString(tfKeyWithChildUid));
    }

    // malformed keys will still parse

    @Test
    public void testParseNoField() {
        Key k = new Key("row", "tf", "datatype\0uid\0value");
        assertEquals("value", TFKeyUtil.getFieldString(k));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseNoValue() {
        Key k = new Key("row", "tf", "datatype\0uid\0FIELD");
        assertEquals("", TFKeyUtil.getValueString(k));
    }

    @Test
    public void testParseNoDatatype() {
        Key k = new Key("row", "tf", "uid\0value\0FIELD");
        assertEquals("uid", TFKeyUtil.getDatatypeString(k));
    }

    @Test
    public void testParseNoUid() {
        Key k = new Key("row", "tf", "datatype\0value\0FIELD");
        assertEquals("value", TFKeyUtil.getUidString(k));
    }
}
