package datawave.util;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class StringMultimapSchemaTest {
    
    private StringMultimapSchema stringMultimapSchema = new StringMultimapSchema();
    
    @Test
    public void testMultimapSchemaConfig() {
        Assert.assertNull(stringMultimapSchema.getFieldName(0));
        Assert.assertEquals("e", stringMultimapSchema.getFieldName(1));
        
        Assert.assertEquals(0, stringMultimapSchema.getFieldNumber("bogusField"));
        Assert.assertEquals(1, stringMultimapSchema.getFieldNumber("e"));
        
        Assert.assertTrue(stringMultimapSchema.isInitialized(null));
        
        Assert.assertEquals("Multimap", stringMultimapSchema.messageName());
        Assert.assertEquals("com.google.common.collect.Multimap", stringMultimapSchema.messageFullName());
        Assert.assertEquals(Multimap.class, stringMultimapSchema.typeClass());
    }
    
    @Test
    public void testProtobufReadWrite() {
        Multimap<String,String> sourceMultimap = ArrayListMultimap.create();
        sourceMultimap.put("this", "that");
        sourceMultimap.putAll("something", Arrays.asList("this", "that", "the other"));
        
        byte[] multimapBytes = ProtobufIOUtil.toByteArray(sourceMultimap, stringMultimapSchema, LinkedBuffer.allocate());
        
        Assert.assertNotNull(multimapBytes);
        Assert.assertTrue(multimapBytes.length > 0);
        
        Multimap<String,String> destMultimap = stringMultimapSchema.newMessage();
        ProtobufIOUtil.mergeFrom(multimapBytes, destMultimap, stringMultimapSchema);
        
        Assert.assertEquals(sourceMultimap, destMultimap);
    }
    
    @Test(expected = RuntimeException.class)
    public void testProtobufReadError() {
        byte[] someBytes = new byte[] {30, 0};
        
        Multimap<String,String> destMultimap = stringMultimapSchema.newMessage();
        ProtobufIOUtil.mergeFrom(someBytes, destMultimap, stringMultimapSchema);
    }
}
