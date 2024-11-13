package datawave.core.iterators.key.util;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.Test;

class TFKeyUtilTest {

    private final Key tfKey = new Key("row", "tf", "datatype\0uid\0value\0FIELD");
    private final Key tfKeyWithNulls = new Key("row", "tf", "datatype\0uid\0v\0a\0l\0u\0e\0FIELD");
    private final Key tfKeyWithChildUid = new Key("row", "tf", "datatype\0uid.11.22\0value\0FIELD");

    @Test
    void testSimpleParse() {
        assertTrue(TFKeyUtil.instanceOf(tfKey));
        //  @formatter:off
        assertAll(
                        () -> assertEquals("FIELD", TFKeyUtil.getFieldString(tfKey)),
                        () -> assertEquals("value", TFKeyUtil.getValueString(tfKey)),
                        () -> assertEquals("datatype", TFKeyUtil.getDatatypeString(tfKey)),
                        () -> assertEquals("uid", TFKeyUtil.getUidString(tfKey))
        );
        //  @formatter:on
    }

    @Test
    void testParseValueWithNulls() {
        assertTrue(TFKeyUtil.instanceOf(tfKeyWithNulls));
        //  @formatter:off
        assertAll(
                        () -> assertEquals("FIELD", TFKeyUtil.getFieldString(tfKeyWithNulls)),
                        () -> assertEquals("v\0a\0l\0u\0e", TFKeyUtil.getValueString(tfKeyWithNulls)),
                        () -> assertEquals("datatype", TFKeyUtil.getDatatypeString(tfKeyWithNulls)),
                        () -> assertEquals("uid", TFKeyUtil.getUidString(tfKeyWithNulls))
        );
        //  @formatter:on
    }

    @Test
    void testParseChildUid() {
        assertTrue(TFKeyUtil.instanceOf(tfKeyWithChildUid));
        //  @formatter:off
        assertAll(
                        () -> assertEquals("FIELD", TFKeyUtil.getFieldString(tfKeyWithChildUid)),
                        () -> assertEquals("value", TFKeyUtil.getValueString(tfKeyWithChildUid)),
                        () -> assertEquals("datatype", TFKeyUtil.getDatatypeString(tfKeyWithChildUid)),
                        () -> assertEquals("uid.11.22", TFKeyUtil.getUidString(tfKeyWithChildUid))
        );
        //  @formatter:on
    }

    // malformed keys will still parse

    @Test
    void testParseNoField() {
        Key k = new Key("row", "tf", "datatype\0uid\0value");
        assertEquals("value", TFKeyUtil.getFieldString(k));
    }

    @Test
    void testParseNoValue() {
        assertThrows(IllegalArgumentException.class, () -> {
            Key k = new Key("row", "tf", "datatype\0uid\0FIELD");
            assertEquals(".getValueString() should have thrown an IllegalArgumentException error: ", "", TFKeyUtil.getValueString(k));
        });
    }

    @Test
    void testParseNoDatatype() {
        Key k = new Key("row", "tf", "uid\0value\0FIELD");
        assertEquals("uid", TFKeyUtil.getDatatypeString(k));
    }

    @Test
    void testParseNoUid() {
        Key k = new Key("row", "tf", "datatype\0value\0FIELD");
        assertEquals("value", TFKeyUtil.getUidString(k));
    }
}
