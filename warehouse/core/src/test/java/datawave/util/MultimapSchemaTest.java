package datawave.util;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class MultimapSchemaTest {
    
    private MultimapSchema multimapSchema = new MultimapSchema();
    
    @Test
    public void testMultimapSchemaConfig() {
        Assert.assertNull(multimapSchema.getFieldName(0));
        Assert.assertEquals("e", multimapSchema.getFieldName(1));
        
        Assert.assertEquals(0, multimapSchema.getFieldNumber("bogusField"));
        Assert.assertEquals(1, multimapSchema.getFieldNumber("e"));
        
        Assert.assertTrue(multimapSchema.isInitialized(null));
        
        Assert.assertEquals("Multimap", multimapSchema.messageName());
        Assert.assertEquals("com.google.common.collect.Multimap", multimapSchema.messageFullName());
        Assert.assertEquals(Multimap.class, multimapSchema.typeClass());
    }
    
    @Test
    public void testProtobufReadWrite() {
        Multimap<String,String> sourceMultimap = HashMultimap.create();
        sourceMultimap.put("this", "that");
        sourceMultimap.putAll("something", Arrays.asList("this", "that", "the other"));
        
        byte[] multimapBytes = ProtobufIOUtil.toByteArray(sourceMultimap, multimapSchema, LinkedBuffer.allocate());
        
        Assert.assertNotNull(multimapBytes);
        Assert.assertTrue(multimapBytes.length > 0);
        
        Multimap<String,String> destMultimap = multimapSchema.newMessage();
        ProtobufIOUtil.mergeFrom(multimapBytes, destMultimap, multimapSchema);
        
        Assert.assertEquals(sourceMultimap, destMultimap);
    }
    
    @Test(expected = RuntimeException.class)
    public void testProtobufReadError() {
        byte[] someBytes = new byte[] {30, 0};
        
        Multimap<String,String> destMultimap = multimapSchema.newMessage();
        ProtobufIOUtil.mergeFrom(someBytes, destMultimap, multimapSchema);
    }
}
