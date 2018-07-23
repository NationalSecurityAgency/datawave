package datawave.util;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.Date;

public class DateSchemaTest {
    
    private DateSchema dateSchema = new DateSchema();
    
    @Test
    public void testDateSchemaConfig() {
        Assert.assertNull(dateSchema.getFieldName(0));
        Assert.assertEquals("dateMillis", dateSchema.getFieldName(1));
        
        Assert.assertEquals(0, dateSchema.getFieldNumber("bogusField"));
        Assert.assertEquals(1, dateSchema.getFieldNumber("dateMillis"));
        
        Assert.assertTrue(dateSchema.isInitialized(null));
        
        Assert.assertEquals("Date", dateSchema.messageName());
        Assert.assertEquals("java.util.Date", dateSchema.messageFullName());
        Assert.assertEquals(Date.class, dateSchema.typeClass());
    }
    
    @Test
    public void testProtobufReadWrite() {
        Date sourceDate = new Date(0);
        
        byte[] multimapBytes = ProtobufIOUtil.toByteArray(sourceDate, dateSchema, LinkedBuffer.allocate());
        
        Assert.assertNotNull(multimapBytes);
        Assert.assertTrue(multimapBytes.length > 0);
        
        Date destDate = dateSchema.newMessage();
        ProtobufIOUtil.mergeFrom(multimapBytes, destDate, dateSchema);
        
        Assert.assertEquals(sourceDate, destDate);
    }
    
    @Test(expected = RuntimeException.class)
    public void testProtobufReadError() {
        byte[] someBytes = new byte[] {30, 0};
        
        Date destDate = dateSchema.newMessage();
        ProtobufIOUtil.mergeFrom(someBytes, destDate, dateSchema);
    }
}
