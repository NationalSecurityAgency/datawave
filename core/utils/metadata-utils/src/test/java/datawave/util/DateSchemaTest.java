package datawave.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Date;

import org.junit.jupiter.api.Test;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;

public class DateSchemaTest {
    
    private DateSchema dateSchema = new DateSchema();
    
    @Test
    public void testDateSchemaConfig() {
        assertNull(dateSchema.getFieldName(0));
        assertEquals("dateMillis", dateSchema.getFieldName(1));
        
        assertEquals(0, dateSchema.getFieldNumber("bogusField"));
        assertEquals(1, dateSchema.getFieldNumber("dateMillis"));
        
        assertTrue(dateSchema.isInitialized(null));
        
        assertEquals("Date", dateSchema.messageName());
        assertEquals("java.util.Date", dateSchema.messageFullName());
        assertEquals(Date.class, dateSchema.typeClass());
    }
    
    @Test
    public void testProtobufReadWrite() {
        Date sourceDate = new Date(0);
        
        byte[] multimapBytes = ProtobufIOUtil.toByteArray(sourceDate, dateSchema, LinkedBuffer.allocate());
        
        assertNotNull(multimapBytes);
        assertTrue(multimapBytes.length > 0);
        
        Date destDate = dateSchema.newMessage();
        ProtobufIOUtil.mergeFrom(multimapBytes, destDate, dateSchema);
        
        assertEquals(sourceDate, destDate);
    }
    
    @Test
    public void testProtobufReadError() {
        byte[] someBytes = new byte[] {30, 0};
        
        Date destDate = dateSchema.newMessage();
        assertThrows(RuntimeException.class, () -> ProtobufIOUtil.mergeFrom(someBytes, destDate, dateSchema));
    }
}
