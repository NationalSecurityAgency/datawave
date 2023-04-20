package datawave.query.iterator;

import com.google.common.collect.Sets;
import datawave.query.function.Equality;
import datawave.query.function.PrefixEquality;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static datawave.query.iterator.QueryOptions.EVENT_FIELD_SEEK;
import static datawave.query.iterator.QueryOptions.EVENT_NEXT_SEEK;
import static datawave.query.iterator.QueryOptions.FI_FIELD_SEEK;
import static datawave.query.iterator.QueryOptions.FI_NEXT_SEEK;
import static datawave.query.iterator.QueryOptions.QUERY;
import static datawave.query.iterator.QueryOptions.TF_FIELD_SEEK;
import static datawave.query.iterator.QueryOptions.TF_NEXT_SEEK;
import static org.junit.Assert.assertEquals;

public class QueryOptionsTest {
    
    @BeforeClass
    public static void setupClass() {
        Logger.getLogger(QueryOptions.class).setLevel(Level.TRACE);
    }
    
    @Test
    public void testBuildFieldDataTypeMapFromSingleValueString() {
        String singleValueData = "k:v;";
        Map<String,Set<String>> expectedDataTypeMap = new HashMap<>();
        expectedDataTypeMap.put("k", Sets.newHashSet("v"));
        Map<String,Set<String>> fieldDataTypeMap = QueryOptions.buildFieldDataTypeMap(singleValueData);
        assertEquals("Failed to parse single value option string", expectedDataTypeMap, fieldDataTypeMap);
    }
    
    @Test
    public void testBuildFieldDataTypeMapFromMultiValueString() {
        String multiValueData = "k:v;key:value";
        Map<String,Set<String>> expectedDataTypeMap = new HashMap<>();
        expectedDataTypeMap.put("k", Sets.newHashSet("v"));
        expectedDataTypeMap.put("key", Sets.newHashSet("value"));
        Map<String,Set<String>> fieldDataTypeMap = QueryOptions.buildFieldDataTypeMap(multiValueData);
        assertEquals("Failed to parse multi-value option string", expectedDataTypeMap, fieldDataTypeMap);
    }
    
    @Test
    public void testBuildFieldDataTypeMapFromEmptyString() {
        String emptyData = "";
        Map<String,Set<String>> expectedDataTypeMap = new HashMap<>();
        Map<String,Set<String>> fieldDataTypeMap = QueryOptions.buildFieldDataTypeMap(emptyData);
        assertEquals("Failed to parse empty option string", expectedDataTypeMap, fieldDataTypeMap);
    }
    
    @Test
    public void testBuildFieldDataTypeMapFromNullString() {
        String nulldata = null;
        Map<String,Set<String>> expectedDataTypeMap = new HashMap<>();
        Map<String,Set<String>> fieldDataTypeMap = QueryOptions.buildFieldDataTypeMap(nulldata);
        assertEquals("Failed to parse null option string", expectedDataTypeMap, fieldDataTypeMap);
    }
    
    @Test
    public void testBuildFieldDataTypeMapFromBadString() {
        String badData = "k:k2:k3:v;";
        Map<String,Set<String>> expectedDataTypeMap = new HashMap<>();
        Map<String,Set<String>> fieldDataTypeMap = QueryOptions.buildFieldDataTypeMap(badData);
        assertEquals("Failed to parse bad option string", expectedDataTypeMap, fieldDataTypeMap);
    }
    
    @Test
    public void testFetchDataTypeKeysFromSingleValueString() {
        String data = "k:v;";
        Set<String> expectedDataTypeKeys = Sets.newHashSet("k");
        Set<String> dataTypeKeys = QueryOptions.fetchDataTypeKeys(data);
        assertEquals("Failed to parse single value option string", expectedDataTypeKeys, dataTypeKeys);
    }
    
    @Test
    public void testFetchDataTypeKeysFromMultiValueString() {
        String data = "k:v;key:value";
        Set<String> expectedDataTypeKeys = Sets.newHashSet("k", "key");
        Set<String> dataTypeKeys = QueryOptions.fetchDataTypeKeys(data);
        assertEquals("Failed to parse multi-value option string", expectedDataTypeKeys, dataTypeKeys);
    }
    
    @Test
    public void testFetchDataTypeKeysFromEmptyString() {
        String data = "";
        Set<String> expectedDataTypeKeys = Sets.newHashSet();
        Set<String> dataTypeKeys = QueryOptions.fetchDataTypeKeys(data);
        assertEquals("Failed to parse empty option string", expectedDataTypeKeys, dataTypeKeys);
    }
    
    @Test
    public void testFetchDataTypeKeysFromNullString() {
        String data = null;
        Set<String> expectedDataTypeKeys = Sets.newHashSet();
        Set<String> dataTypeKeys = QueryOptions.fetchDataTypeKeys(data);
        assertEquals("Failed to parse null option string", expectedDataTypeKeys, dataTypeKeys);
    }
    
    @Test
    public void testSeekingConfiguration() {
        Map<String,String> optionsMap = new HashMap<>();
        optionsMap.put(QUERY, "set to avoid early return");
        optionsMap.put(FI_FIELD_SEEK, "10");
        optionsMap.put(FI_NEXT_SEEK, "11");
        optionsMap.put(EVENT_FIELD_SEEK, "12");
        optionsMap.put(EVENT_NEXT_SEEK, "13");
        optionsMap.put(TF_FIELD_SEEK, "14");
        optionsMap.put(TF_NEXT_SEEK, "15");
        
        QueryOptions options = new QueryOptions();
        
        // initial state
        assertEquals(-1, options.getFiFieldSeek());
        assertEquals(-1, options.getFiNextSeek());
        assertEquals(-1, options.getEventFieldSeek());
        assertEquals(-1, options.getEventNextSeek());
        assertEquals(-1, options.getTfFieldSeek());
        assertEquals(-1, options.getTfNextSeek());
        
        options.validateOptions(optionsMap);
        
        // expected state
        assertEquals(10, options.getFiFieldSeek());
        assertEquals(11, options.getFiNextSeek());
        assertEquals(12, options.getEventFieldSeek());
        assertEquals(13, options.getEventNextSeek());
        assertEquals(14, options.getTfFieldSeek());
        assertEquals(15, options.getTfNextSeek());
    }
    
    @Test
    public void testGetEquality() {
        QueryOptions options = new QueryOptions();
        Equality equality = options.getEquality();
        assertEquals(PrefixEquality.class.getSimpleName(), equality.getClass().getSimpleName());
    }
}
