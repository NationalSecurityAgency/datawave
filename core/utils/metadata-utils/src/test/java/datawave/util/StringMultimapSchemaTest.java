package datawave.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;

public class StringMultimapSchemaTest {
    
    private StringMultimapSchema stringMultimapSchema = new StringMultimapSchema();
    
    @Test
    public void testMultimapSchemaConfig() {
        assertNull(stringMultimapSchema.getFieldName(0));
        assertEquals("e", stringMultimapSchema.getFieldName(1));
        
        assertEquals(0, stringMultimapSchema.getFieldNumber("bogusField"));
        assertEquals(1, stringMultimapSchema.getFieldNumber("e"));
        
        assertTrue(stringMultimapSchema.isInitialized(null));
        
        assertEquals("Multimap", stringMultimapSchema.messageName());
        assertEquals("com.google.common.collect.Multimap", stringMultimapSchema.messageFullName());
        assertEquals(Multimap.class, stringMultimapSchema.typeClass());
    }
    
    @Test
    public void testProtobufReadWrite() {
        Multimap<String,String> sourceMultimap = ArrayListMultimap.create();
        sourceMultimap.put("this", "that");
        sourceMultimap.putAll("something", Arrays.asList("this", "that", "the other"));
        
        byte[] multimapBytes = ProtobufIOUtil.toByteArray(sourceMultimap, stringMultimapSchema, LinkedBuffer.allocate());
        
        assertNotNull(multimapBytes);
        assertTrue(multimapBytes.length > 0);
        
        Multimap<String,String> destMultimap = stringMultimapSchema.newMessage();
        ProtobufIOUtil.mergeFrom(multimapBytes, destMultimap, stringMultimapSchema);
        
        assertEquals(sourceMultimap, destMultimap);
    }
    
    @Test
    public void testProtobufReadError() {
        byte[] someBytes = new byte[] {30, 0};
        
        Multimap<String,String> destMultimap = stringMultimapSchema.newMessage();
        assertThrows(RuntimeException.class, () -> ProtobufIOUtil.mergeFrom(someBytes, destMultimap, stringMultimapSchema));
    }
}
