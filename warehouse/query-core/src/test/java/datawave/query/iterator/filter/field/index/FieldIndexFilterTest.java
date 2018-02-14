package datawave.query.iterator.filter.field.index;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.data.type.GeometryType;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NoOpType;
import datawave.data.type.NumberType;
import datawave.ingest.mapreduce.handler.shard.FieldIndexData;
import datawave.ingest.mapreduce.handler.shard.FieldIndexFilterData;
import datawave.query.jexl.DefaultArithmetic;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.TypeMetadata;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.jexl2.JexlArithmetic;
import org.apache.commons.jexl2.parser.JexlNode;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class FieldIndexFilterTest {
    
    private static final String INGEST_TYPE = "WKT";
    private static final TypeMetadata TYPE_METADATA = new TypeMetadata();
    private static final JexlArithmetic ARITHMETIC = new DefaultArithmetic();
    
    @BeforeClass
    public static void setup() {
        TYPE_METADATA.put("GEO_FIELD", INGEST_TYPE, GeometryType.class.getName());
        TYPE_METADATA.put("WKT_BYTE_LENGTH", INGEST_TYPE, NumberType.class.getName());
    }
    
    @Test
    public void testAccept() throws Exception {
        Map<String,Multimap<String,String>> fieldIndexFilterMapByType = new HashMap<>();
        Multimap<String,String> multimap = HashMultimap.create();
        multimap.put("GEO_FIELD", "WKT_BYTE_LENGTH");
        fieldIndexFilterMapByType.put(INGEST_TYPE, multimap);
        
        FieldIndexFilter fieldIndexFilter = new FieldIndexFilter(fieldIndexFilterMapByType, TYPE_METADATA, ARITHMETIC);
        
        Multimap<String,JexlNode> filterNodes = HashMultimap.create();
        filterNodes.put("WKT_BYTE_LENGTH", JexlASTHelper.parseJexlQuery("WKT_BYTE_LENGTH < 45"));
        filterNodes.put("WKT_BYTE_LENGTH", JexlASTHelper.parseJexlQuery("WKT_BYTE_LENGTH > 20"));
        fieldIndexFilter.addFieldIndexFilterNodes(filterNodes);
        
        Multimap<String,String> fieldValueMapping = HashMultimap.create();
        fieldValueMapping.put("WKT_BYTE_LENGTH", "30");
        FieldIndexData fieldIndexData = new FieldIndexData(new FieldIndexFilterData(fieldValueMapping), null);
        
        Value value = new Value(ProtobufIOUtil.toByteArray(fieldIndexData, FieldIndexData.SCHEMA, LinkedBuffer.allocate()));
        boolean result = fieldIndexFilter.keep(INGEST_TYPE, "GEO_FIELD", value);
        
        Assert.assertTrue(result);
    }
    
    @Test
    public void testReject() throws Exception {
        Map<String,Multimap<String,String>> fieldIndexFilterMapByType = new HashMap<>();
        Multimap<String,String> multimap = HashMultimap.create();
        multimap.put("GEO_FIELD", "WKT_BYTE_LENGTH");
        fieldIndexFilterMapByType.put(INGEST_TYPE, multimap);
        
        FieldIndexFilter fieldIndexFilter = new FieldIndexFilter(fieldIndexFilterMapByType, TYPE_METADATA, ARITHMETIC);
        
        Multimap<String,JexlNode> filterNodes = HashMultimap.create();
        filterNodes.put("WKT_BYTE_LENGTH", JexlASTHelper.parseJexlQuery("WKT_BYTE_LENGTH < 45"));
        filterNodes.put("WKT_BYTE_LENGTH", JexlASTHelper.parseJexlQuery("WKT_BYTE_LENGTH > 20"));
        fieldIndexFilter.addFieldIndexFilterNodes(filterNodes);
        
        Multimap<String,String> fieldValueMapping = HashMultimap.create();
        fieldValueMapping.put("WKT_BYTE_LENGTH", "10");
        FieldIndexData fieldIndexData = new FieldIndexData(new FieldIndexFilterData(fieldValueMapping), null);
        
        Value value = new Value(ProtobufIOUtil.toByteArray(fieldIndexData, FieldIndexData.SCHEMA, LinkedBuffer.allocate()));
        boolean result = fieldIndexFilter.keep(INGEST_TYPE, "GEO_FIELD", value);
        
        Assert.assertFalse(result);
    }
    
    @Test
    public void testUnmatchedTypeMetadata() throws Exception {
        Map<String,Multimap<String,String>> fieldIndexFilterMapByType = new HashMap<>();
        Multimap<String,String> multimap = HashMultimap.create();
        multimap.put("GEO_FIELD", "WKT_BYTE_LENGTH");
        fieldIndexFilterMapByType.put(INGEST_TYPE, multimap);
        
        TypeMetadata typeMetadata = new TypeMetadata();
        typeMetadata.put("GEO_FIELD", "bogusType", GeometryType.class.getName());
        typeMetadata.put("WKT_BYTE_LENGTH", "bogusType", NumberType.class.getName());
        
        FieldIndexFilter fieldIndexFilter = new FieldIndexFilter(fieldIndexFilterMapByType, typeMetadata, ARITHMETIC);
        
        Multimap<String,JexlNode> filterNodes = HashMultimap.create();
        filterNodes.put("WKT_BYTE_LENGTH", JexlASTHelper.parseJexlQuery("WKT_BYTE_LENGTH < 45"));
        filterNodes.put("WKT_BYTE_LENGTH", JexlASTHelper.parseJexlQuery("WKT_BYTE_LENGTH > 20"));
        fieldIndexFilter.addFieldIndexFilterNodes(filterNodes);
        
        Multimap<String,String> fieldValueMapping = HashMultimap.create();
        fieldValueMapping.put("WKT_BYTE_LENGTH", "10");
        FieldIndexData fieldIndexData = new FieldIndexData(new FieldIndexFilterData(fieldValueMapping), null);
        
        Value value = new Value(ProtobufIOUtil.toByteArray(fieldIndexData, FieldIndexData.SCHEMA, LinkedBuffer.allocate()));
        boolean result = fieldIndexFilter.keep(INGEST_TYPE, "GEO_FIELD", value);
        
        Assert.assertTrue(result);
    }
    
    @Test
    public void testUnmatchedIngestType() throws Exception {
        Map<String,Multimap<String,String>> fieldIndexFilterMapByType = new HashMap<>();
        Multimap<String,String> multimap = HashMultimap.create();
        multimap.put("GEO_FIELD", "WKT_BYTE_LENGTH");
        fieldIndexFilterMapByType.put(INGEST_TYPE, multimap);
        
        FieldIndexFilter fieldIndexFilter = new FieldIndexFilter(fieldIndexFilterMapByType, TYPE_METADATA, ARITHMETIC);
        
        Multimap<String,JexlNode> filterNodes = HashMultimap.create();
        filterNodes.put("WKT_BYTE_LENGTH", JexlASTHelper.parseJexlQuery("WKT_BYTE_LENGTH < 45"));
        filterNodes.put("WKT_BYTE_LENGTH", JexlASTHelper.parseJexlQuery("WKT_BYTE_LENGTH > 20"));
        fieldIndexFilter.addFieldIndexFilterNodes(filterNodes);
        
        Multimap<String,String> fieldValueMapping = HashMultimap.create();
        fieldValueMapping.put("WKT_BYTE_LENGTH", "10");
        FieldIndexData fieldIndexData = new FieldIndexData(new FieldIndexFilterData(fieldValueMapping), null);
        
        Value value = new Value(ProtobufIOUtil.toByteArray(fieldIndexData, FieldIndexData.SCHEMA, LinkedBuffer.allocate()));
        boolean result = fieldIndexFilter.keep("bogusType", "GEO_FIELD", value);
        
        Assert.assertTrue(result);
    }
    
    @Test
    public void testUnmatchedFieldFilter() throws Exception {
        Map<String,Multimap<String,String>> fieldIndexFilterMapByType = new HashMap<>();
        Multimap<String,String> multimap = HashMultimap.create();
        multimap.put("GEO_FIELD", "WKT_BYTE_LENGTH");
        fieldIndexFilterMapByType.put(INGEST_TYPE, multimap);
        
        FieldIndexFilter fieldIndexFilter = new FieldIndexFilter(fieldIndexFilterMapByType, TYPE_METADATA, ARITHMETIC);
        
        Multimap<String,JexlNode> filterNodes = HashMultimap.create();
        filterNodes.put("FOO_FIELD", JexlASTHelper.parseJexlQuery("FOO_FIELD < 45"));
        filterNodes.put("FOO_FIELD", JexlASTHelper.parseJexlQuery("FOO_FIELD > 20"));
        fieldIndexFilter.addFieldIndexFilterNodes(filterNodes);
        
        Multimap<String,String> fieldValueMapping = HashMultimap.create();
        fieldValueMapping.put("WKT_BYTE_LENGTH", "10");
        FieldIndexData fieldIndexData = new FieldIndexData(new FieldIndexFilterData(fieldValueMapping), null);
        
        Value value = new Value(ProtobufIOUtil.toByteArray(fieldIndexData, FieldIndexData.SCHEMA, LinkedBuffer.allocate()));
        boolean result = fieldIndexFilter.keep(INGEST_TYPE, "GEO_FIELD", value);
        
        Assert.assertTrue(result);
    }
    
    @Test
    public void testMultipleDataTypes() throws Exception {
        Map<String,Multimap<String,String>> fieldIndexFilterMapByType = new HashMap<>();
        Multimap<String,String> multimap = HashMultimap.create();
        multimap.put("GEO_FIELD", "WKT_DATA");
        fieldIndexFilterMapByType.put(INGEST_TYPE, multimap);
        
        TypeMetadata typeMetadata = new TypeMetadata();
        typeMetadata.put("GEO_FIELD", INGEST_TYPE, GeometryType.class.getName());
        typeMetadata.put("WKT_DATA", INGEST_TYPE, LcNoDiacriticsType.class.getName());
        
        FieldIndexFilter fieldIndexFilter = new FieldIndexFilter(fieldIndexFilterMapByType, typeMetadata, ARITHMETIC);
        
        Multimap<String,JexlNode> filterNodes = HashMultimap.create();
        filterNodes.put("WKT_DATA", JexlASTHelper.parseJexlQuery("WKT_DATA == 'POINT(10 20)'"));
        fieldIndexFilter.addFieldIndexFilterNodes(filterNodes);
        
        Multimap<String,String> fieldValueMapping = HashMultimap.create();
        fieldValueMapping.put("WKT_DATA", "POINT(10 20)");
        FieldIndexData fieldIndexData = new FieldIndexData(new FieldIndexFilterData(fieldValueMapping), null);
        
        Value value = new Value(ProtobufIOUtil.toByteArray(fieldIndexData, FieldIndexData.SCHEMA, LinkedBuffer.allocate()));
        boolean result = fieldIndexFilter.keep(INGEST_TYPE, "GEO_FIELD", value);
        
        // Test without NoOpType added
        Assert.assertFalse(result);
        
        // Test with NoOpType added
        fieldIndexFilter.typeMetadata.put("WKT_DATA", INGEST_TYPE, NoOpType.class.getName());
        result = fieldIndexFilter.keep(INGEST_TYPE, "GEO_FIELD", value);
        Assert.assertTrue(result);
    }
    
    @Test
    public void testQueryOperators() throws Exception {
        // ==
        Assert.assertTrue(testBasicQueryOperators("WKT_BYTE_LENGTH == 30", "30"));
        Assert.assertFalse(testBasicQueryOperators("WKT_BYTE_LENGTH == 31", "30"));
        
        // !=
        Assert.assertTrue(testBasicQueryOperators("WKT_BYTE_LENGTH != 29", "30"));
        Assert.assertFalse(testBasicQueryOperators("WKT_BYTE_LENGTH != 30", "30"));
        
        // <
        Assert.assertTrue(testBasicQueryOperators("WKT_BYTE_LENGTH < 31", "30"));
        Assert.assertTrue(testBasicQueryOperators("WKT_BYTE_LENGTH < 40", "30"));
        Assert.assertFalse(testBasicQueryOperators("WKT_BYTE_LENGTH < 30", "30"));
        
        // <=
        Assert.assertTrue(testBasicQueryOperators("WKT_BYTE_LENGTH <= 31", "30"));
        Assert.assertTrue(testBasicQueryOperators("WKT_BYTE_LENGTH <= 40", "30"));
        Assert.assertTrue(testBasicQueryOperators("WKT_BYTE_LENGTH <= 30", "30"));
        Assert.assertFalse(testBasicQueryOperators("WKT_BYTE_LENGTH <= 29", "30"));
        
        // >
        Assert.assertTrue(testBasicQueryOperators("WKT_BYTE_LENGTH > 29", "30"));
        Assert.assertTrue(testBasicQueryOperators("WKT_BYTE_LENGTH > 20", "30"));
        Assert.assertFalse(testBasicQueryOperators("WKT_BYTE_LENGTH > 30", "30"));
        
        // >=
        Assert.assertTrue(testBasicQueryOperators("WKT_BYTE_LENGTH >= 29", "30"));
        Assert.assertTrue(testBasicQueryOperators("WKT_BYTE_LENGTH >= 20", "30"));
        Assert.assertTrue(testBasicQueryOperators("WKT_BYTE_LENGTH >= 30", "30"));
        Assert.assertFalse(testBasicQueryOperators("WKT_BYTE_LENGTH >= 31", "30"));
        
        // =~
        Assert.assertTrue(testRegexQueryOperators("STRING_FIELD =~ 'some .*'", "some string"));
        Assert.assertTrue(testRegexQueryOperators("STRING_FIELD =~ '.* string'", "some string"));
        Assert.assertTrue(testRegexQueryOperators("STRING_FIELD =~ 'some string'", "some string"));
        Assert.assertFalse(testRegexQueryOperators("STRING_FIELD =~ 'bogus'", "some string"));
        
        // !~
        Assert.assertFalse(testRegexQueryOperators("STRING_FIELD !~ 'some .*'", "some string"));
        Assert.assertFalse(testRegexQueryOperators("STRING_FIELD !~ '.* string'", "some string"));
        Assert.assertFalse(testRegexQueryOperators("STRING_FIELD !~ 'some string'", "some string"));
        Assert.assertTrue(testRegexQueryOperators("STRING_FIELD !~ 'bogus'", "some string"));
    }
    
    public boolean testBasicQueryOperators(String acceptQuery, String fieldValue) throws Exception {
        Map<String,Multimap<String,String>> fieldIndexFilterMapByType = new HashMap<>();
        Multimap<String,String> multimap = HashMultimap.create();
        multimap.put("GEO_FIELD", "WKT_BYTE_LENGTH");
        fieldIndexFilterMapByType.put(INGEST_TYPE, multimap);
        
        FieldIndexFilter fieldIndexFilter = new FieldIndexFilter(fieldIndexFilterMapByType, TYPE_METADATA, ARITHMETIC);
        
        Multimap<String,JexlNode> filterNodes = HashMultimap.create();
        filterNodes.put("WKT_BYTE_LENGTH", JexlASTHelper.parseJexlQuery(acceptQuery));
        fieldIndexFilter.addFieldIndexFilterNodes(filterNodes);
        
        Multimap<String,String> fieldValueMapping = HashMultimap.create();
        fieldValueMapping.put("WKT_BYTE_LENGTH", fieldValue);
        FieldIndexData fieldIndexData = new FieldIndexData(new FieldIndexFilterData(fieldValueMapping), null);
        
        Value value = new Value(ProtobufIOUtil.toByteArray(fieldIndexData, FieldIndexData.SCHEMA, LinkedBuffer.allocate()));
        return fieldIndexFilter.keep(INGEST_TYPE, "GEO_FIELD", value);
    }
    
    public boolean testRegexQueryOperators(String acceptQuery, String fieldValue) throws Exception {
        Map<String,Multimap<String,String>> fieldIndexFilterMapByType = new HashMap<>();
        Multimap<String,String> multimap = HashMultimap.create();
        multimap.put("GEO_FIELD", "STRING_FIELD");
        fieldIndexFilterMapByType.put(INGEST_TYPE, multimap);
        
        TypeMetadata typeMetadata = new TypeMetadata();
        typeMetadata.put("GEO_FIELD", INGEST_TYPE, GeometryType.class.getName());
        typeMetadata.put("STRING_FIELD", INGEST_TYPE, LcNoDiacriticsType.class.getName());
        typeMetadata.put("STRING_FIELD", INGEST_TYPE, NoOpType.class.getName());
        
        FieldIndexFilter fieldIndexFilter = new FieldIndexFilter(fieldIndexFilterMapByType, typeMetadata, ARITHMETIC);
        
        Multimap<String,JexlNode> filterNodes = HashMultimap.create();
        filterNodes.put("STRING_FIELD", JexlASTHelper.parseJexlQuery(acceptQuery));
        fieldIndexFilter.addFieldIndexFilterNodes(filterNodes);
        
        Multimap<String,String> fieldValueMapping = HashMultimap.create();
        fieldValueMapping.put("STRING_FIELD", fieldValue);
        FieldIndexData fieldIndexData = new FieldIndexData(new FieldIndexFilterData(fieldValueMapping), null);
        
        Value value = new Value(ProtobufIOUtil.toByteArray(fieldIndexData, FieldIndexData.SCHEMA, LinkedBuffer.allocate()));
        return fieldIndexFilter.keep(INGEST_TYPE, "GEO_FIELD", value);
    }
    
    // NOTE: This test is here to show that it is possible to perform field index filtering using custom functions
    // (i.e. geowave functions), but please note that support would need to be added to the IteratorBuildingVisitor in
    // order for ASTFunctionNodes to be considered for FieldIndexFilter evaluation. This can be reevaluated if/when a
    // need for such functionality presents itself.
    @Test
    public void testGeoWaveFunction() throws Exception {
        Map<String,Multimap<String,String>> fieldIndexFilterMapByType = new HashMap<>();
        Multimap<String,String> multimap = HashMultimap.create();
        multimap.put("WKT_BYTE_LENGTH", "GEO_FIELD");
        fieldIndexFilterMapByType.put(INGEST_TYPE, multimap);
        
        FieldIndexFilter fieldIndexFilter = new FieldIndexFilter(fieldIndexFilterMapByType, TYPE_METADATA, ARITHMETIC);
        
        Multimap<String,JexlNode> filterNodes = HashMultimap.create();
        filterNodes.put("GEO_FIELD", JexlASTHelper.parseJexlQuery("geowave:intersects(GEO_FIELD, 'POLYGON((-20 -20, 20 -20, 20 20, -20 20, -20 -20))')"));
        fieldIndexFilter.addFieldIndexFilterNodes(filterNodes);
        
        Multimap<String,String> fieldValueMapping = HashMultimap.create();
        fieldValueMapping.put("GEO_FIELD", "POINT(0 0)");
        FieldIndexData fieldIndexData = new FieldIndexData(new FieldIndexFilterData(fieldValueMapping), null);
        
        Value value = new Value(ProtobufIOUtil.toByteArray(fieldIndexData, FieldIndexData.SCHEMA, LinkedBuffer.allocate()));
        boolean result = fieldIndexFilter.keep(INGEST_TYPE, "WKT_BYTE_LENGTH", value);
        
        Assert.assertTrue(result);
        
        // Test with non-intersecting bounding box
        fieldIndexFilter.getFieldIndexFilterNodes().clear();
        fieldIndexFilter.getFieldIndexFilterNodes().put("GEO_FIELD",
                        JexlASTHelper.parseJexlQuery("geowave:intersects(GEO_FIELD, 'POLYGON((10 -20, 50 -20, 50 20, 30 20, 30 -20))')"));
        result = fieldIndexFilter.keep(INGEST_TYPE, "WKT_BYTE_LENGTH", value);
        Assert.assertFalse(result);
    }
}
