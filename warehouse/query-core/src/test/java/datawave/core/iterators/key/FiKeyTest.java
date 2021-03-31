package datawave.core.iterators.key;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class FiKeyTest {
    
    @Test
    public void testParseValue() {
        Text columnQualifier = new Text("value\0datatype\0uid");
        FiKey fiKey = new FiKey();
        fiKey.parse(columnQualifier);
        assertEquals("value", fiKey.getValue());
        
        // validate the fiKey returns the same object once cached
        assertSame(fiKey.getValue(), fiKey.getValue());
    }
    
    @Test
    public void testParseDatatype() {
        Text columnQualifier = new Text("value\0datatype\0uid");
        FiKey fiKey = new FiKey();
        fiKey.parse(columnQualifier);
        assertEquals("datatype", fiKey.getDatatype());
        
        // validate the fiKey returns the same object once cached
        assertSame(fiKey.getDatatype(), fiKey.getDatatype());
    }
    
    @Test
    public void testParseUid() {
        Text columnQualifier = new Text("value\0datatype\0uid");
        FiKey fiKey = new FiKey();
        fiKey.parse(columnQualifier);
        assertEquals("uid", fiKey.getUid());
        
        // validate the fiKey returns the same object once cached
        assertSame(fiKey.getUid(), fiKey.getUid());
    }
    
    @Test
    public void testUidStartsWith() {
        Text columnQualifier = new Text("value\0datatype\0uid.1.2");
        FiKey fiKey = new FiKey();
        fiKey.parse(columnQualifier);
        assertEquals("uid.1.2", fiKey.getUid());
        assertTrue(fiKey.uidStartsWith("uid"));
        assertTrue(fiKey.uidStartsWith("uid.1"));
        assertTrue(fiKey.uidStartsWith("uid.1.2"));
        assertFalse(fiKey.uidStartsWith("uid.9"));
    }
}
