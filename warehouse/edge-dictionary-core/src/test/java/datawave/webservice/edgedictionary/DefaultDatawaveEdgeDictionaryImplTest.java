package datawave.webservice.edgedictionary;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import datawave.edge.protobuf.EdgeData;
import datawave.edge.util.EdgeKey;
import datawave.webservice.results.edgedictionary.DefaultEdgeDictionary;
import datawave.webservice.results.edgedictionary.EventField;
import datawave.webservice.results.edgedictionary.DefaultMetadata;
import datawave.webservice.results.edgedictionary.MetadataBase;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

import static org.junit.Assert.fail;

public class DefaultDatawaveEdgeDictionaryImplTest {
    
    public static final String EDGE_TYPE = "TYPE";
    public static final String SOURCE_RELATIONSHIP = "OWNER";
    public static final String SINK_RELATIONSHIP = "PET";
    public static final String ATTRIBUTE3 = "abc123";
    public static final String SINK_ATTRIBUTE1 = "EYES";
    public static final String[] SOURCE_ATTRIBUTE1 = new String[] {"P", "Q", "R", "S"};
    public static final String ATTRIBUTE2 = "blue";
    public static final String SOURCE_FIELD = "M_E_123";
    public static final String SINK_FIELD = "M_E_123";
    public static final String EARLY_DATE_FIELD = "20130318";
    
    private static final List<Key> EDGE_KEYS = createEdgeKeys();
    private static final Value EDGE_VALUE = createEdgeValue();
    private static final Collection<DefaultMetadata> METADATA = createMetadata();
    
    SetMultimap<Key,Value> edgeMetadataRows;
    DefaultDatawaveEdgeDictionaryImpl impl;
    Method transformResultsMethod;
    
    @Before
    public void setUp() {
        edgeMetadataRows = HashMultimap.create();
        for (Map.Entry<Key,Value> entry : permuteKeysAndEdges(EDGE_KEYS, EDGE_VALUE)) {
            edgeMetadataRows.put(entry.getKey(), entry.getValue());
        }
        impl = new DefaultDatawaveEdgeDictionaryImpl();
        transformResultsMethod = getPrivateMethod("transformResults");
    }
    
    private static List<Map.Entry<Key,Value>> permuteKeysAndEdges(List<Key> edgeKeys, Value... edgeValue) {
        ArrayList<Map.Entry<Key,Value>> list = new ArrayList<>();
        for (Key key : edgeKeys) {
            for (Value value : edgeValue) {
                list.add(new HashMap.SimpleEntry<>(key, value));
            }
        }
        return list;
    }
    
    @Test
    public void testNoOp() throws InvocationTargetException, IllegalAccessException {
        if (null == transformResultsMethod)
            fail();
        DefaultEdgeDictionary dictionary = (DefaultEdgeDictionary) transformResultsMethod.invoke(impl, HashMultimap.create());
        Assert.assertEquals("Should be empty", dictionary.getTotalResults(), 0L);
    }
    
    @Test
    public void testWorked() throws InvocationTargetException, IllegalAccessException {
        if (null == transformResultsMethod)
            fail();
        Assert.assertEquals("data to be inserted contains as many rows as keys", edgeMetadataRows.keySet().size(), EDGE_KEYS.size());
        DefaultEdgeDictionary dictionary = (DefaultEdgeDictionary) transformResultsMethod.invoke(impl, edgeMetadataRows);
        Assert.assertEquals("Dictionary should now have some entries", dictionary.getTotalResults(), EDGE_KEYS.size());
        Assert.assertTrue("METADATA not in list.  returned list: " + dictionary.getMetadataList().toString() + " expected: " + METADATA, dictionary
                        .getMetadataList().containsAll(METADATA));
    }
    
    @Test
    public void earliestDateFound() throws InvocationTargetException, IllegalAccessException {
        if (null == transformResultsMethod)
            fail();
        
        DefaultEdgeDictionary dictionary = (DefaultEdgeDictionary) transformResultsMethod.invoke(impl, edgeMetadataRows);
        
        List<? extends MetadataBase<DefaultMetadata>> metadata = dictionary.getMetadataList();
        
        // Make sure that all Metadata in EdgeDictionary have the start date set to the EARLY_DATE_FIELD
        for (MetadataBase<DefaultMetadata> meta : metadata) {
            Assert.assertTrue("Incorrect start date. Expected: " + EARLY_DATE_FIELD + " Found: " + meta.getStartDate(),
                            EARLY_DATE_FIELD.equals(meta.getStartDate()));
            
        }
    }
    
    private Method getPrivateMethod(String methodName) {
        Class clas = DefaultDatawaveEdgeDictionaryImpl.class;
        for (Method method : clas.getDeclaredMethods()) {
            if (method.getName().equals(methodName)) {
                method.setAccessible(true);
                return method;
            }
        }
        return null;
    }
    
    private static Value createEdgeValue() {
        // Extra Metadata values with different dates to ensure that the earliest date is chosen to be the start date
        // of collection. Early Date = 20130318
        EdgeData.MetadataValue.Metadata value1 = EdgeData.MetadataValue.Metadata.newBuilder().setSource(SOURCE_FIELD).setSink(SINK_FIELD).setDate("20130415")
                        .build();
        EdgeData.MetadataValue.Metadata value2 = EdgeData.MetadataValue.Metadata.newBuilder().setSource(SOURCE_FIELD).setSink(SINK_FIELD).setDate("20130320")
                        .build();
        EdgeData.MetadataValue.Metadata value3 = EdgeData.MetadataValue.Metadata.newBuilder().setSource(SOURCE_FIELD).setSink(SINK_FIELD)
                        .setDate(EARLY_DATE_FIELD).build();
        EdgeData.MetadataValue.Metadata value4 = EdgeData.MetadataValue.Metadata.newBuilder().setSource(SOURCE_FIELD).setSink(SINK_FIELD).setDate("20140314")
                        .build();
        EdgeData.MetadataValue.Metadata value5 = EdgeData.MetadataValue.Metadata.newBuilder().setSource(SOURCE_FIELD).setSink(SINK_FIELD).setDate("20130319")
                        .build();
        
        EdgeData.MetadataValue.Builder valueBuilder = datawave.edge.protobuf.EdgeData.MetadataValue.newBuilder();
        valueBuilder.addAllMetadata(Collections.singletonList(value1));
        valueBuilder.addAllMetadata(Collections.singletonList(value2));
        valueBuilder.addAllMetadata(Collections.singletonList(value3));
        valueBuilder.addAllMetadata(Collections.singletonList(value4));
        valueBuilder.addAllMetadata(Collections.singletonList(value5));
        
        return new Value(valueBuilder.build().toByteArray());
    }
    
    private static ArrayList<Key> createEdgeKeys() {
        ArrayList<Key> result = new ArrayList<>();
        for (String currAttribute1 : SOURCE_ATTRIBUTE1) {
            result.add(generateKeyForEdgeMetadata(currAttribute1));
        }
        return result;
    }
    
    private static Key generateKeyForEdgeMetadata(String source_attribute1) {
        EdgeKey edgeKey = EdgeKey.newBuilder(EdgeKey.EDGE_FORMAT.STANDARD).escape().setType(EDGE_TYPE).setSourceRelationship(SOURCE_RELATIONSHIP)
                        .setSinkRelationship(SINK_RELATIONSHIP).setSourceAttribute1(source_attribute1).setSinkAttribute1(SINK_ATTRIBUTE1)
                        .setAttribute2(ATTRIBUTE2).setAttribute3(ATTRIBUTE3).build();
        return edgeKey.getMetadataKey();
    }
    
    private static Collection<DefaultMetadata> createMetadata() {
        Collection<DefaultMetadata> metadataList = new LinkedList<>();
        for (String source_attribute1 : SOURCE_ATTRIBUTE1) {
            DefaultMetadata metadata = new DefaultMetadata();
            metadata.setEdgeType(EDGE_TYPE);
            metadata.setEdgeRelationship(SOURCE_RELATIONSHIP + EdgeKey.COL_SUB_SEPARATOR + SINK_RELATIONSHIP);
            metadata.setEdgeAttribute1Source(source_attribute1 + EdgeKey.COL_SUB_SEPARATOR + SINK_ATTRIBUTE1);
            EventField field = new EventField();
            field.setSourceField(SOURCE_FIELD);
            field.setSinkField(SINK_FIELD);
            
            metadata.setEventFields(Collections.singletonList(field));
            metadata.setStartDate(EARLY_DATE_FIELD);
            metadataList.add(metadata);
        }
        return metadataList;
    }
}
