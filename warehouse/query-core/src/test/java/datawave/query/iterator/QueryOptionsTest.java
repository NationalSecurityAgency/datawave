package datawave.query.iterator;

import com.google.common.collect.Sets;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
}
