package datawave.core.iterators.key.util;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.Test;

class FiKeyUtilTest {

    private final Key fikey = new Key("row", "fi\0FIELD", "value\0datatype\0uid");
    private final Key fikeyWithNulls = new Key("row", "fi\0FIELD", "v\0a\0l\0u\0e\0datatype\0uid");
    private final Key fikeyWithChildUid = new Key("row", "fi\0FIELD", "value\0datatype\0uid.12.37");

    @Test
    void testSimpleParse() {
        assertTrue(FiKeyUtil.instanceOf(fikey));
        //  @formatter:off
        assertAll(
                        () -> assertEquals("FIELD", FiKeyUtil.getFieldString(fikey)),
                        () -> assertEquals("value", FiKeyUtil.getValueString(fikey)),
                        () -> assertEquals("datatype", FiKeyUtil.getDatatypeString(fikey)),
                        () -> assertEquals("uid", FiKeyUtil.getUidString(fikey))
        );
        //  @formatter:on
    }

    @Test
    void testParsingValueWithNulls() {
        assertTrue(FiKeyUtil.instanceOf(fikeyWithNulls));
        //  @formatter:off
        assertAll(
                        () -> assertEquals("FIELD", FiKeyUtil.getFieldString(fikeyWithNulls)),
                        () -> assertEquals("v\0a\0l\0u\0e", FiKeyUtil.getValueString(fikeyWithNulls)),
                        () -> assertEquals("datatype", FiKeyUtil.getDatatypeString(fikeyWithNulls)),
                        () -> assertEquals("uid", FiKeyUtil.getUidString(fikeyWithNulls))
        );
        //  @formatter:on
    }

    @Test
    void testParseChildUid() {
        assertTrue(FiKeyUtil.instanceOf(fikeyWithChildUid));
        //  @formatter:off
        assertAll(
                        () -> assertEquals("FIELD", FiKeyUtil.getFieldString(fikeyWithChildUid)),
                        () -> assertEquals("value", FiKeyUtil.getValueString(fikeyWithChildUid)),
                        () -> assertEquals("datatype", FiKeyUtil.getDatatypeString(fikeyWithChildUid)),
                        () -> assertEquals("uid.12.37", FiKeyUtil.getUidString(fikeyWithChildUid))
        );
        //  @formatter:on
    }

    @Test
    void testParseNoValue() {
        Key key = new Key("row", "fi\0FIELD", "datatype\0uid");
        assertThrows(IllegalArgumentException.class, () -> {
            assertEquals(".getValueString() should have thrown an IllegalArgumentException error: ", "", FiKeyUtil.getValueString(key));
        });
    }

    @Test
    void testParseNoDatatype() {
        Key key = new Key("row", "fi\0FIELD", "value\0uid");
        assertThrows(IllegalArgumentException.class, () -> {
            assertEquals(".getDatatypeString() should have thrown an IllegalArgumentException error: ", "", FiKeyUtil.getDatatypeString(key));
        });
    }

    // getUidString simply returns everything after the last null byte. If the column qualifier is wrong
    // then the method will return the wrong value
    @Test
    void testParseNoUid() {
        Key key = new Key("row", "fi\0FIELD", "value\0datatype");
        assertEquals("datatype", FiKeyUtil.getUidString(key));
    }

}
