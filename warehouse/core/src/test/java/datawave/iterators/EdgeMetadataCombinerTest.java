package datawave.iterators;

import com.google.protobuf.InvalidProtocolBufferException;
import datawave.edge.protobuf.EdgeData;
import org.apache.accumulo.core.data.Value;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class EdgeMetadataCombinerTest {
    
    @Test
    public void reduceTest() throws InvalidProtocolBufferException {
        
        EdgeMetadataCombiner combiner = new EdgeMetadataCombiner();
        
        EdgeData.MetadataValue.Metadata.Builder builder = EdgeData.MetadataValue.Metadata.newBuilder();
        EdgeData.MetadataValue.Builder mBuilder = EdgeData.MetadataValue.newBuilder();
        List<Value> testValues = new ArrayList();
        List<EdgeData.MetadataValue.Metadata> expectedMetadata = new ArrayList();
        
        builder.setSource("field_1");
        builder.setSink("field_3");
        builder.setDate("20100101");
        expectedMetadata.add(builder.build());
        mBuilder.addMetadata(builder.build());
        builder.clear();
        
        builder.setSource("field_2");
        builder.setSink("field_4");
        builder.setDate("20100101");
        expectedMetadata.add(builder.build());
        mBuilder.addMetadata(builder.build());
        builder.clear();
        
        builder.setSource("field_3");
        builder.setSink("field_1");
        builder.setDate("20100101");
        expectedMetadata.add(builder.build());
        mBuilder.addMetadata(builder.build());
        builder.clear();
        testValues.add(new Value(mBuilder.build().toByteArray()));
        mBuilder.clear();
        
        builder.setSource("field_1");
        builder.setSink("field_3");
        builder.setDate("20150101");
        
        mBuilder.addMetadata(builder.build());
        builder.clear();
        testValues.add(new Value(mBuilder.build().toByteArray()));
        
        Value reducedValue = combiner.reduce(null, testValues.iterator());
        
        EdgeData.MetadataValue metadataVal = datawave.edge.protobuf.EdgeData.MetadataValue.parseFrom(reducedValue.get());
        
        Assert.assertEquals(3, metadataVal.getMetadataCount());
        Assert.assertTrue(expectedMetadata.containsAll(metadataVal.getMetadataList()));
        
    }
}
