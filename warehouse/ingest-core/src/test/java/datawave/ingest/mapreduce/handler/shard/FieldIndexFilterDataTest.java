package datawave.ingest.mapreduce.handler.shard;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import io.protostuff.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class FieldIndexFilterDataTest {
    
    @Test
    public void testFieldIndexFilterDataSchemaConfig() {
        Schema schema = FieldIndexFilterData.SCHEMA;
        
        Assert.assertNull(schema.getFieldName(0));
        Assert.assertEquals("fieldValueMapping", schema.getFieldName(1));
        
        Assert.assertEquals(0, schema.getFieldNumber("bogusField"));
        Assert.assertEquals(1, schema.getFieldNumber("fieldValueMapping"));
        
        Assert.assertTrue(schema.isInitialized(null));
        
        Assert.assertEquals("FieldIndexFilterData", schema.messageName());
        Assert.assertEquals("datawave.ingest.mapreduce.handler.shard.FieldIndexFilterData", schema.messageFullName());
        Assert.assertEquals(FieldIndexFilterData.class, schema.typeClass());
    }
    
    @Test
    public void testProtobufReadWrite() {
        Multimap<String,String> multimapData = HashMultimap.create();
        multimapData.put("GEO_FIELD", "WKT_BYTE_LENGTH");
        multimapData.putAll("TEXT_FIELD", Arrays.asList("NUMBER_FIELD", "DATE_FIELD"));
        
        FieldIndexFilterData sourceFieldIndexFilterData = new FieldIndexFilterData(multimapData);
        byte[] fieldIndexFilterBytes = ProtobufIOUtil.toByteArray(sourceFieldIndexFilterData, sourceFieldIndexFilterData.cachedSchema(),
                        LinkedBuffer.allocate());
        
        Assert.assertNotNull(fieldIndexFilterBytes);
        Assert.assertTrue(fieldIndexFilterBytes.length > 0);
        
        FieldIndexFilterData destFieldIndexFilterData = FieldIndexFilterData.SCHEMA.newMessage();
        ProtobufIOUtil.mergeFrom(fieldIndexFilterBytes, destFieldIndexFilterData, destFieldIndexFilterData.cachedSchema());
        
        Assert.assertEquals(sourceFieldIndexFilterData.getFieldValueMapping(), destFieldIndexFilterData.getFieldValueMapping());
    }
    
    @Test(expected = RuntimeException.class)
    public void testProtobufReadError() {
        byte[] someBytes = new byte[] {30, 0};
        
        FieldIndexFilterData fieldIndexFilterData = FieldIndexFilterData.SCHEMA.newMessage();
        ProtobufIOUtil.mergeFrom(someBytes, fieldIndexFilterData, FieldIndexFilterData.SCHEMA);
    }
}
