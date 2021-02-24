package datawave.core.iterators.key;

import org.apache.accumulo.core.data.Key;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

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
    }
}
