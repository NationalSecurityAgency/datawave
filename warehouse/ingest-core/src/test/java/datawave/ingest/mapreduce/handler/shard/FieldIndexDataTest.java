package datawave.ingest.mapreduce.handler.shard;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import io.protostuff.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class FieldIndexDataTest {
    
    @Test
    public void testFieldIndexDataSchemaConfig() {
        Schema schema = FieldIndexData.SCHEMA;
        
        Assert.assertNull(schema.getFieldName(0));
        Assert.assertEquals("filterData", schema.getFieldName(1));
        Assert.assertEquals("bloomFilterBytes", schema.getFieldName(2));
        
        Assert.assertEquals(0, schema.getFieldNumber("bogusField"));
        Assert.assertEquals(1, schema.getFieldNumber("filterData"));
        Assert.assertEquals(2, schema.getFieldNumber("bloomFilterBytes"));
        
        Assert.assertTrue(schema.isInitialized(null));
        
        Assert.assertEquals("FieldIndexData", schema.messageName());
        Assert.assertEquals("datawave.ingest.mapreduce.handler.shard.FieldIndexData", schema.messageFullName());
        Assert.assertEquals(FieldIndexData.class, schema.typeClass());
    }
    
    @Test
    public void testProtobufReadWrite() {
        byte[] bloomFilterBytes = new String("bloom filter bytes").getBytes();
        
        Multimap<String,String> multimapData = HashMultimap.create();
        multimapData.put("GEO_FIELD", "WKT_BYTE_LENGTH");
        multimapData.putAll("TEXT_FIELD", Arrays.asList("NUMBER_FIELD", "DATE_FIELD"));
        
        FieldIndexData sourceFieldIndexData = new FieldIndexData(new FieldIndexFilterData(multimapData), bloomFilterBytes);
        byte[] fieldIndexFilterBytes = ProtobufIOUtil.toByteArray(sourceFieldIndexData, sourceFieldIndexData.cachedSchema(), LinkedBuffer.allocate());
        
        Assert.assertNotNull(fieldIndexFilterBytes);
        Assert.assertTrue(fieldIndexFilterBytes.length > 0);
        
        FieldIndexData destFieldIndexFilterData = FieldIndexData.SCHEMA.newMessage();
        ProtobufIOUtil.mergeFrom(fieldIndexFilterBytes, destFieldIndexFilterData, destFieldIndexFilterData.cachedSchema());
        
        Assert.assertEquals(sourceFieldIndexData.getFilterData().getFieldValueMapping(), destFieldIndexFilterData.getFilterData().getFieldValueMapping());
        Assert.assertTrue(Arrays.equals(sourceFieldIndexData.getBloomFilterBytes(), destFieldIndexFilterData.getBloomFilterBytes()));
    }
    
    @Test(expected = RuntimeException.class)
    public void testProtobufReadError() {
        byte[] someBytes = new byte[] {30, 0};
        
        FieldIndexData fieldIndexData = FieldIndexData.SCHEMA.newMessage();
        ProtobufIOUtil.mergeFrom(someBytes, fieldIndexData, FieldIndexData.SCHEMA);
    }
}
