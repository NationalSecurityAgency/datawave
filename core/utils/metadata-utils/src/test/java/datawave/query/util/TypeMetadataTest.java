package datawave.query.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Set;

import org.junit.Test;

import com.google.common.collect.Sets;

public class TypeMetadataTest {
    
    @Test
    public void testGetNormalizers() {
        TypeMetadata typeMetadata = new TypeMetadata();
        typeMetadata.put("FIELD2", "datatypeA", "IntegerType");
        typeMetadata.put("FIELD2", "datatypeB", "LcType");
        typeMetadata.put("FIELD2", "datatypeC", "NumberType");
        
        Set<String> norms1 = typeMetadata.getNormalizerNamesForField("FIELD2");
        assertEquals(3, norms1.size());
        assertTrue(norms1.contains("IntegerType"));
        assertTrue(norms1.contains("LcType"));
        assertTrue(norms1.contains("NumberType"));
        
        Set<String> norms2 = typeMetadata.getNormalizerNamesForField("FIELD42");
        assertEquals(0, norms2.size());
        
        // empty request should return empty set
        Set<String> norms3 = typeMetadata.getNormalizerNamesForField("");
        assertEquals(0, norms3.size());
    }
    
    @Test
    public void testMultipleDataTypes() {
        TypeMetadata typeMetadata = new TypeMetadata();
        typeMetadata.put("FIELD1", "ingestA", "LcNoDiacriticsType");
        typeMetadata.put("FIELD1", "ingestB", "LcNoDiacriticsType");
        typeMetadata.put("FIELD1", "ingestC", "LcNoDiacriticsType");
        typeMetadata.put("FIELD2", "ingestA", "NumberType");
        typeMetadata.put("FIELD2", "ingestB", "NumberType");
        typeMetadata.put("FIELD2", "ingestC", "NumberType");
        typeMetadata.put("FIELD2", "ingestA", "LcNoDiacriticsType");
        typeMetadata.put("FIELD2", "ingestB", "LcNoDiacriticsType");
        typeMetadata.put("FIELD2", "ingestC", "LcNoDiacriticsType");
        
        assertEquals("dts:[0:ingestA,1:ingestB,2:ingestC];types:[0:LcNoDiacriticsType,1:NumberType];FIELD1:[0:0,1:0,2:0];FIELD2:[0:0,0:1,1:0,1:1,2:0,2:1]",
                        typeMetadata.toString());
    }
    
    @Test
    public void testMultipleIngestTypes() {
        TypeMetadata typeMetadata = new TypeMetadata();
        typeMetadata.put("FIELD1", "ingestA", "LcNoDiacriticsType");
        typeMetadata.put("FIELD1", "ingestA", "NumberType");
        typeMetadata.put("FIELD1", "ingestB", "DateType");
        
        assertEquals("dts:[0:ingestA,1:ingestB];types:[0:DateType,1:LcNoDiacriticsType,2:NumberType];FIELD1:[0:1,0:2,1:0]", typeMetadata.toString());
    }
    
    @Test
    public void testGetDataTypes() {
        TypeMetadata typeMetadata = new TypeMetadata();
        typeMetadata.put("FIELD1", "datatypeA", "LcType");
        typeMetadata.put("FIELD1", "datatypeB", "DateType");
        typeMetadata.put("FIELD3", "datatypeC", "LcType");
        
        Set<String> types1 = typeMetadata.getDataTypesForField("FIELD1");
        assertEquals(2, types1.size());
        assertTrue(types1.contains("datatypeA"));
        assertTrue(types1.contains("datatypeB"));
        
        Set<String> types2 = typeMetadata.getDataTypesForField("FIELD3");
        assertEquals(1, types2.size());
        assertTrue(types2.contains("datatypeC"));
        
        // empty request should return empty set
        Set<String> types3 = typeMetadata.getDataTypesForField("");
        assertEquals(0, types3.size());
    }
    
    @Test
    public void testReadSerializedFormatSingleFieldAndType() {
        TypeMetadata fromString = new TypeMetadata("dts:[0:ingest1];types:[0:DateType];FIELD1:[0:0]");
        
        Set<String> types1 = fromString.getDataTypesForField("FIELD1");
        assertEquals(1, types1.size());
        assertTrue(types1.contains("ingest1"));
        
        Set<String> normalizers = fromString.getNormalizerNamesForField("FIELD1");
        assertEquals(1, normalizers.size());
        assertTrue(normalizers.contains("DateType"));
    }
    
    @Test
    public void testReadSerializedFormatSingleFieldAndType2() {
        TypeMetadata fromString = new TypeMetadata("dts:[0:ingest1,1:ingest2];types:[0:DateType,1:NumberType];FIELD1:[1:1]");
        
        Set<String> types1 = fromString.getDataTypesForField("FIELD1");
        assertEquals(1, types1.size());
        assertTrue(types1.contains("ingest2"));
        
        Set<String> normalizers = fromString.getNormalizerNamesForField("FIELD1");
        assertEquals(1, normalizers.size());
        assertTrue(normalizers.contains("NumberType"));
    }
    
    @Test
    public void testReadSerializedFormatMultipleFieldsAndTypes() {
        TypeMetadata fromString = new TypeMetadata(
                        "dts:[0:ingest1,1:ingest2];types:[0:DateType,1:IntegerType,2:LcType];FIELD1:[0:2,1:0];FIELD2:[0:1,1:2];FIELD3:[0:0,1:0]");
        
        Set<String> types1 = fromString.getDataTypesForField("FIELD1");
        assertEquals(2, types1.size());
        assertTrue(types1.contains("ingest1"));
        assertTrue(types1.contains("ingest2"));
        
        Set<String> normalizers = fromString.getNormalizerNamesForField("FIELD1");
        assertEquals(2, normalizers.size());
        assertTrue(normalizers.contains("DateType"));
        assertTrue(normalizers.contains("LcType"));
    }
    
    @Test
    public void testReadNewSerializedFormatMultipleFieldsAndTypes2() {
        TypeMetadata fromString = new TypeMetadata(
                        "dts:[0:generic,1:null];types:[0:datawave.data.type.GeoType,1:datawave.data.type.LcNoDiacriticsType,2:datawave.data.type.NumberType];CITY:[0:1,1:1];CITY_STATE:[0:1];CODE:[0:1,1:1];CONTINENT:[0:1,1:1];GEO:[0:0,1:0];NUM:[0:2,1:2];STATE:[0:1]");
        
        Set<String> types1 = fromString.getDataTypesForField("CITY");
        assertEquals(2, types1.size());
        assertTrue(types1.contains("generic"));
        assertTrue(types1.contains("null"));
        
        Set<String> normalizers1 = fromString.getNormalizerNamesForField("CITY");
        assertEquals(1, normalizers1.size());
        assertTrue(normalizers1.contains("datawave.data.type.LcNoDiacriticsType"));
        
        Set<String> types2 = fromString.getDataTypesForField("CITY_STATE");
        assertEquals(1, types2.size());
        assertTrue(types2.contains("generic"));
        
        Set<String> normalizers2 = fromString.getNormalizerNamesForField("CITY_STATE");
        assertEquals(1, normalizers2.size());
        assertTrue(normalizers2.contains("datawave.data.type.LcNoDiacriticsType"));
    }
    
    @Test
    public void testReadSerializedFormatSingleFieldMultipleTypes1() {
        TypeMetadata fromString = new TypeMetadata("dts:[0:ingest1,1:ingest2];types:[0:DateType];FIELD1:[0:0,1:0]");
        
        Set<String> types1 = fromString.getDataTypesForField("FIELD1");
        assertEquals(2, types1.size());
        assertTrue(types1.contains("ingest1"));
        assertTrue(types1.contains("ingest2"));
        
        Set<String> normalizers = fromString.getNormalizerNamesForField("FIELD1");
        assertEquals(1, normalizers.size());
        assertTrue(normalizers.contains("DateType"));
    }
    
    @Test
    public void testReadSerializedFormatSingleFieldMultipleTypes2() {
        TypeMetadata fromString = new TypeMetadata("dts:[0:ingest1];types:[0:DateType,1:NumberType];FIELD1:[0:0,0:1]");
        
        Set<String> types1 = fromString.getDataTypesForField("FIELD1");
        assertEquals(1, types1.size());
        assertTrue(types1.contains("ingest1"));
        
        Set<String> normalizers = fromString.getNormalizerNamesForField("FIELD1");
        assertEquals(2, normalizers.size());
        assertTrue(normalizers.contains("DateType"));
        assertTrue(normalizers.contains("NumberType"));
    }
    
    @Test
    public void testWriteSerializedFormat() {
        TypeMetadata typeMetadata = new TypeMetadata();
        typeMetadata.put("FIELD1", "ingest1", "LcType");
        typeMetadata.put("FIELD1", "ingest2", "DateType");
        typeMetadata.put("FIELD2", "ingest1", "IntegerType");
        typeMetadata.put("FIELD2", "ingest2", "LcType");
        
        String newString = typeMetadata.toString();
        String expectedString = "dts:[0:ingest1,1:ingest2];types:[0:DateType,1:IntegerType,2:LcType];FIELD1:[0:2,1:0];FIELD2:[0:1,1:2]";
        
        assertEquals(expectedString, newString);
    }
    
    @Test
    public void testReduceEmptyTypeMetadata() {
        TypeMetadata reduced = new TypeMetadata().reduce(Collections.emptySet());
        assertTrue(reduced.typeMetadata.isEmpty());
    }
    
    @Test
    public void testReductionNoFieldsMatch() {
        TypeMetadata reduced = new TypeMetadata().reduce(Collections.singleton("FIELD4"));
        assertTrue(reduced.typeMetadata.isEmpty());
    }
    
    @Test
    public void testReductionSomeFieldsMatch() {
        TypeMetadata typeMetadata = new TypeMetadata();
        typeMetadata.put("FIELD1", "datatypeA", "LcType");
        typeMetadata.put("FIELD2", "datatypeB", "IntegerType");
        typeMetadata.put("FIELD3", "datatypeC", "LcType");
        
        TypeMetadata reduced = typeMetadata.reduce(Sets.newHashSet("FIELD1", "FIELD2"));
        assertTrue(reduced.keySet().contains("FIELD1"));
        assertTrue(reduced.keySet().contains("FIELD2"));
        assertFalse(reduced.keySet().contains("FIELD3"));
    }
    
    @Test
    public void testReductionAllFieldsMatch() {
        TypeMetadata typeMetadata = new TypeMetadata();
        typeMetadata.put("FIELD1", "datatypeA", "LcType");
        typeMetadata.put("FIELD2", "datatypeB", "LcType");
        typeMetadata.put("FIELD3", "datatypeC", "LcType");
        
        TypeMetadata reduced = typeMetadata.reduce(Sets.newHashSet("FIELD1", "FIELD2", "FIELD3"));
        assertTrue(reduced.keySet().contains("FIELD1"));
        assertTrue(reduced.keySet().contains("FIELD2"));
        assertTrue(reduced.keySet().contains("FIELD3"));
        assertEquals(reduced, typeMetadata);
    }
}
