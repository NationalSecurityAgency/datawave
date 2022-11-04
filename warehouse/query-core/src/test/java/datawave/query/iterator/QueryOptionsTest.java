package datawave.query.iterator;

import com.google.common.collect.Sets;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class QueryOptionsTest {
    
    @BeforeAll
    public static void setupClass() {
        Logger.getLogger(QueryOptions.class).setLevel(Level.TRACE);
    }
    
    @Test
    public void testBuildFieldDataTypeMapFromSingleValueString() {
        String singleValueData = "k:v;";
        Map<String,Set<String>> expectedDataTypeMap = new HashMap<>();
        expectedDataTypeMap.put("k", Sets.newHashSet("v"));
        Map<String,Set<String>> fieldDataTypeMap = QueryOptions.buildFieldDataTypeMap(singleValueData);
        assertEquals(expectedDataTypeMap, fieldDataTypeMap, "Failed to parse single value option string");
    }
    
    @Test
    public void testBuildFieldDataTypeMapFromMultiValueString() {
        String multiValueData = "k:v;key:value";
        Map<String,Set<String>> expectedDataTypeMap = new HashMap<>();
        expectedDataTypeMap.put("k", Sets.newHashSet("v"));
        expectedDataTypeMap.put("key", Sets.newHashSet("value"));
        Map<String,Set<String>> fieldDataTypeMap = QueryOptions.buildFieldDataTypeMap(multiValueData);
        assertEquals(expectedDataTypeMap, fieldDataTypeMap, "Failed to parse multi-value option string");
    }
    
    @Test
    public void testBuildFieldDataTypeMapFromEmptyString() {
        String emptyData = "";
        Map<String,Set<String>> expectedDataTypeMap = new HashMap<>();
        Map<String,Set<String>> fieldDataTypeMap = QueryOptions.buildFieldDataTypeMap(emptyData);
        assertEquals(expectedDataTypeMap, fieldDataTypeMap, "Failed to parse empty option string");
    }
    
    @Test
    public void testBuildFieldDataTypeMapFromNullString() {
        Map<String,Set<String>> expectedDataTypeMap = new HashMap<>();
        Map<String,Set<String>> fieldDataTypeMap = QueryOptions.buildFieldDataTypeMap(null);
        assertEquals(expectedDataTypeMap, fieldDataTypeMap, "Failed to parse null option string");
    }
    
    @Test
    public void testBuildFieldDataTypeMapFromBadString() {
        String badData = "k:k2:k3:v;";
        Map<String,Set<String>> expectedDataTypeMap = new HashMap<>();
        Map<String,Set<String>> fieldDataTypeMap = QueryOptions.buildFieldDataTypeMap(badData);
        assertEquals(expectedDataTypeMap, fieldDataTypeMap, "Failed to parse bad option string");
    }
    
    @Test
    public void testFetchDataTypeKeysFromSingleValueString() {
        String data = "k:v;";
        Set<String> expectedDataTypeKeys = Sets.newHashSet("k");
        Set<String> dataTypeKeys = QueryOptions.fetchDataTypeKeys(data);
        assertEquals(expectedDataTypeKeys, dataTypeKeys, "Failed to parse single value option string");
    }
    
    @Test
    public void testFetchDataTypeKeysFromMultiValueString() {
        String data = "k:v;key:value";
        Set<String> expectedDataTypeKeys = Sets.newHashSet("k", "key");
        Set<String> dataTypeKeys = QueryOptions.fetchDataTypeKeys(data);
        assertEquals(expectedDataTypeKeys, dataTypeKeys, "Failed to parse multi-value option string");
    }
    
    @Test
    public void testFetchDataTypeKeysFromEmptyString() {
        String data = "";
        Set<String> expectedDataTypeKeys = Sets.newHashSet();
        Set<String> dataTypeKeys = QueryOptions.fetchDataTypeKeys(data);
        assertEquals(expectedDataTypeKeys, dataTypeKeys, "Failed to parse empty option string");
    }
    
    @Test
    public void testFetchDataTypeKeysFromNullString() {
        Set<String> expectedDataTypeKeys = Sets.newHashSet();
        Set<String> dataTypeKeys = QueryOptions.fetchDataTypeKeys(null);
        assertEquals(expectedDataTypeKeys, dataTypeKeys, "Failed to parse null option string");
    }
}
