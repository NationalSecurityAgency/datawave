package datawave.iterators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.Value;
import org.junit.jupiter.api.Test;

import com.google.protobuf.InvalidProtocolBufferException;

import datawave.metadata.protobuf.EdgeMetadata.MetadataValue;
import datawave.metadata.protobuf.EdgeMetadata.MetadataValue.Metadata;

public class EdgeMetadataCombinerTest {
    
    @Test
    public void reduceTest() throws InvalidProtocolBufferException {
        
        EdgeMetadataCombiner combiner = new EdgeMetadataCombiner();
        
        Metadata.Builder builder = Metadata.newBuilder();
        MetadataValue.Builder mBuilder = MetadataValue.newBuilder();
        List<Value> testValues = new ArrayList();
        List<Metadata> expectedMetadata = new ArrayList();
        
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
        
        MetadataValue metadataVal = MetadataValue.parseFrom(reducedValue.get());
        
        assertEquals(3, metadataVal.getMetadataCount());
        assertTrue(expectedMetadata.containsAll(metadataVal.getMetadataList()));
        
    }
}
