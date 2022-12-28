package datawave.core.iterators.key;

import org.apache.accumulo.core.data.Key;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TFKeyTest {
    
    @Test
    public void testParse() {
        Key k = new Key("row", "tf", "datatype\0uid\0fieldValue\0fieldName");
        TFKey tfKey = new TFKey();
        tfKey.parse(k);
        
        assertEquals("datatype", tfKey.getDatatype());
        assertEquals("uid", tfKey.getUid());
        assertEquals("fieldValue", tfKey.getValue());
        assertEquals("fieldName", tfKey.getField());
        assertEquals("uid\0fieldValue", tfKey.getUidAndValue());
        
        // Ensure it works when multiple nulls are in the value
        k = new Key("row", "tf", "datatype\0uid\0fi\0eld\0Va\0lue\0fieldName");
        tfKey.parse(k);
        assertEquals("datatype", tfKey.getDatatype());
        assertEquals("uid", tfKey.getUid());
        assertEquals("fi\0eld\0Va\0lue", tfKey.getValue());
        assertEquals("fieldName", tfKey.getField());
        assertEquals("uid\0fi\0eld\0Va\0lue", tfKey.getUidAndValue());
        assertTrue(tfKey.isValid());
    }
    
    @Test
    public void testParse_missingColumnQualifier() {
        Key k = new Key("row", "tf");
        TFKey tfKey = new TFKey();
        tfKey.parse(k);
        
        assertNull(tfKey.getDatatype());
        assertNull(tfKey.getUid());
        assertNull(tfKey.getValue());
        assertNull(tfKey.getField());
        assertNull(tfKey.getUidAndValue());
        assertFalse(tfKey.isValid());
    }
}
