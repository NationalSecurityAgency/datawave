package datawave.ingest.json.util;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import datawave.ingest.json.util.JsonObjectFlattener.FlattenMode;

public class JsonObjectFlattenerImplTest {
    
    protected static String jsonFile = "/input/flattener-test.json";
    protected static String json;
    
    protected JsonObjectFlattener.MapKeyValueNormalizer toLowerCaseNormalizer = new JsonObjectFlattener.MapKeyValueNormalizer() {
        @Override
        public String normalizeMapKey(String key, String value) throws IllegalStateException {
            return key.toLowerCase();
        }
        
        @Override
        public String normalizeMapValue(String value, String key) throws IllegalStateException {
            return value;
        }
    };
    
    protected JsonObjectFlattener.MapKeyValueNormalizer toUpperCaseNormalizer = new JsonObjectFlattener.MapKeyValueNormalizer() {
        @Override
        public String normalizeMapKey(String key, String value) throws IllegalStateException {
            return key.toUpperCase();
        }
        
        @Override
        public String normalizeMapValue(String value, String key) throws IllegalStateException {
            return value;
        }
    };
    
    protected JsonObjectFlattener.MapKeyValueNormalizer noOpNormalizer = new JsonObjectFlattener.MapKeyValueNormalizer.NoOp();
    
    @BeforeClass
    public static void setup() throws URISyntaxException, IOException {
        URL data = JsonObjectFlattenerImplTest.class.getResource(jsonFile);
        Assert.assertNotNull(data);
        json = new String(Files.readAllBytes(Paths.get(data.toURI())));
    }
    
    @Test
    public void testFlattenAndForceUpperCaseKeys() throws Exception {
        JsonObjectFlattener flattener = new JsonObjectFlattenerImpl.Builder().pathDelimiter(".").mapKeyValueNormalizer(toUpperCaseNormalizer).build();
        
        JsonParser parser = new JsonParser();
        JsonElement jsonElement = parser.parse(json);
        Multimap<String,String> fieldMap = flattener.flatten(jsonElement.getAsJsonObject());
        
        // printJson(json);
        // printMap(fieldMap);
        
        Assert.assertEquals(25, fieldMap.keySet().size());
        Assert.assertEquals(29, fieldMap.values().size());
        Assert.assertEquals(3, fieldMap.get("ROOTOBJECT.DATE").size());
    }
    
    @Test
    public void testFlattenAndForceLowerCaseKeys() throws Exception {
        
        JsonObjectFlattener flattener = new JsonObjectFlattenerImpl.Builder().pathDelimiter(".").mapKeyValueNormalizer(toLowerCaseNormalizer).build();
        
        JsonParser parser = new JsonParser();
        JsonElement jsonElement = parser.parse(json);
        Multimap<String,String> fieldMap = flattener.flatten(jsonElement.getAsJsonObject());
        
        // printJson(json);
        // printMap(fieldMap);
        
        Assert.assertEquals(25, fieldMap.keySet().size());
        Assert.assertEquals(29, fieldMap.values().size());
        Assert.assertEquals(3, fieldMap.get("rootobject.date").size());
        Assert.assertEquals(1, fieldMap.get("rootobject.string1").size());
    }
    
    @Test
    public void testFlattenWithBlacklist() throws Exception {
        JsonObjectFlattener flattener = new JsonObjectFlattenerImpl.Builder()
                        .mapKeyBlacklist(new HashSet<>(Arrays.asList("ROOTOBJECT.NUMBER2", "ROOTOBJECT.STRING2"))).pathDelimiter(".")
                        .mapKeyValueNormalizer(toUpperCaseNormalizer).build();
        
        JsonParser parser = new JsonParser();
        JsonElement jsonElement = parser.parse(json);
        Multimap<String,String> fieldMap = flattener.flatten(jsonElement.getAsJsonObject());
        
        // printJson(json);
        // printMap(fieldMap);
        
        Assert.assertEquals(23, fieldMap.keySet().size());
        Assert.assertEquals(27, fieldMap.values().size());
        Assert.assertEquals(3, fieldMap.get("ROOTOBJECT.DATE").size());
        Assert.assertFalse(fieldMap.containsKey("ROOTOBJECT.NUMBER2") || fieldMap.containsKey("ROOTOBJECT.STRING22"));
    }
    
    @Test
    public void testFlattenWithWhitelist() throws Exception {
        JsonObjectFlattener flattener = new JsonObjectFlattenerImpl.Builder()
                        .mapKeyWhitelist(new HashSet<>(Arrays.asList("ROOTOBJECT.NUMBER2", "ROOTOBJECT.STRING2"))).pathDelimiter(".")
                        .mapKeyValueNormalizer(toUpperCaseNormalizer).build();
        
        JsonParser parser = new JsonParser();
        JsonElement jsonElement = parser.parse(json);
        Multimap<String,String> fieldMap = flattener.flatten(jsonElement.getAsJsonObject());
        
        // printJson(json);
        // printMap(fieldMap);
        
        Assert.assertEquals(2, fieldMap.keySet().size());
        Assert.assertEquals(2, fieldMap.values().size());
        Assert.assertTrue(fieldMap.containsKey("ROOTOBJECT.NUMBER2") && fieldMap.containsKey("ROOTOBJECT.STRING2"));
    }
    
    @Test
    public void testFlattenWithWhitelistBlacklistConflict() throws Exception {
        JsonObjectFlattener flattener = new JsonObjectFlattenerImpl.Builder()
                        .mapKeyWhitelist(new HashSet<>(Arrays.asList("ROOTOBJECT.NUMBER2", "ROOTOBJECT.STRING2")))
                        .mapKeyBlacklist(new HashSet<>(Arrays.asList("ROOTOBJECT.NUMBER2"))).pathDelimiter(".").mapKeyValueNormalizer(toUpperCaseNormalizer)
                        .build();
        
        JsonParser parser = new JsonParser();
        JsonElement jsonElement = parser.parse(json);
        Multimap<String,String> fieldMap = flattener.flatten(jsonElement.getAsJsonObject());
        
        // printJson(json);
        // printMap(fieldMap);
        
        Assert.assertTrue(fieldMap.containsKey("ROOTOBJECT.STRING2") && fieldMap.keySet().size() == 1);
    }
    
    @Test
    public void testFlattenPreserveCaseIncludingPrefix() throws Exception {
        JsonObjectFlattener flattener = new JsonObjectFlattenerImpl.Builder().pathDelimiter("_").mapKeyValueNormalizer(noOpNormalizer).build();
        
        JsonParser parser = new JsonParser();
        JsonElement jsonElement = parser.parse(json);
        Multimap<String,String> fieldMap = flattener.flatten(jsonElement.getAsJsonObject());
        
        // printJson(json);
        // printMap(fieldMap);
        
        Assert.assertEquals(25, fieldMap.keySet().size());
        Assert.assertEquals(29, fieldMap.values().size());
        Assert.assertTrue(fieldMap.containsKey("rootobject_sTrInG1"));
    }
    
    @Test
    public void testFlattenModeSIMPLE() throws Exception {
        JsonObjectFlattener flattener = new JsonObjectFlattenerImpl.Builder().flattenMode(FlattenMode.SIMPLE).mapKeyValueNormalizer(noOpNormalizer).build();
        
        JsonParser parser = new JsonParser();
        JsonElement jsonElement = parser.parse(json);
        Multimap<String,String> fieldMap = flattener.flatten(jsonElement.getAsJsonObject());
        
        // printJson(json);
        // printMap(fieldMap);
        
        Assert.assertEquals(4, fieldMap.keySet().size());
        Assert.assertEquals(6, fieldMap.values().size());
        Assert.assertTrue(fieldMap.containsKey("rootarray"));
        Assert.assertTrue(fieldMap.containsKey("rootdate"));
        Assert.assertTrue(fieldMap.containsKey("rootid"));
        Assert.assertTrue(fieldMap.containsKey("rootnumber"));
        Assert.assertEquals(3, fieldMap.get("rootarray").size());
    }
    
    @Test
    public void testFlattenWithGroupingContext() throws Exception {
        JsonObjectFlattener flattener = new JsonObjectFlattenerImpl.Builder().flattenMode(FlattenMode.GROUPED).occurrenceInGroupDelimiter("#")
                        .mapKeyValueNormalizer(noOpNormalizer).build();
        
        JsonParser parser = new JsonParser();
        JsonElement jsonElement = parser.parse(json);
        Multimap<String,String> fieldMap = flattener.flatten(jsonElement.getAsJsonObject());
        
        // printJson(json);
        // printMap(fieldMap);
        
        Assert.assertEquals(27, fieldMap.keySet().size());
        Assert.assertEquals(29, fieldMap.values().size());
        Assert.assertEquals(1, fieldMap.get("date.rootobject#0.date#0").size());
        Assert.assertEquals(1, fieldMap.get("date.rootobject#0.date#1").size());
        Assert.assertEquals(1, fieldMap.get("date.rootobject#0.date#2").size());
        Assert.assertEquals(1, fieldMap.get("name.rootobject#0.properties#0.array#0.name#0").size());
        
    }
    
    @Test
    public void testFlattenGROUPED_AND_NORMAL() throws Exception {
        JsonObjectFlattener flattener = new JsonObjectFlattenerImpl.Builder().flattenMode(FlattenMode.GROUPED_AND_NORMAL).occurrenceInGroupDelimiter("_")
                        .pathDelimiter(".").mapKeyValueNormalizer(noOpNormalizer).build();
        
        JsonParser parser = new JsonParser();
        JsonElement jsonElement = parser.parse(json);
        Multimap<String,String> fieldMap = flattener.flatten(jsonElement.getAsJsonObject());
        
        // printJson(json);
        // printMap(fieldMap);
        
        Assert.assertEquals(41, fieldMap.keySet().size());
        Assert.assertEquals(52, fieldMap.values().size());
        
        Assert.assertEquals(1, fieldMap.get("date.rootobject_0.date_0").size());
        Assert.assertTrue(fieldMap.containsKey("rootobject.date"));
        Assert.assertEquals(3, fieldMap.get("rootobject.date").size());
        
        Assert.assertEquals(1, fieldMap.get("date.rootobject_0.date_1").size());
        Assert.assertEquals(1, fieldMap.get("date.rootobject_0.date_2").size());
        
        Assert.assertEquals(1, fieldMap.get("name.rootobject_0.properties_0.array_0.name_0").size());
        Assert.assertTrue(fieldMap.containsKey("rootobject.properties.array.name"));
        Assert.assertEquals(4, fieldMap.get("rootobject.properties.array.name").size());
        Assert.assertEquals(4, fieldMap.get("rootobject.properties.array.value").size());
        
    }
    
    @Test(expected = IllegalStateException.class)
    public void testGroupingContextWithBadJson() throws Exception {
        JsonObjectFlattener flattener = new JsonObjectFlattenerImpl.Builder().flattenMode(FlattenMode.GROUPED).occurrenceInGroupDelimiter("#").build();
        
        JsonParser parser = new JsonParser();
        JsonElement jsonElement = parser.parse(json);
        JsonObject job = jsonElement.getAsJsonObject();
        job.addProperty("illegal.key.format", "key name already has our path delimiter!");
        
        try {
            flattener.flatten(job);
        } catch (IllegalStateException ill) {
            Assert.assertTrue(ill.getMessage().lastIndexOf("delimiter found in json element") > -1);
            throw ill;
        }
    }
    
    @Test(expected = IllegalStateException.class)
    public void testGroupingContextWithBadDelimiterConfig() throws Exception {
        JsonObjectFlattener flattener = new JsonObjectFlattenerImpl.Builder().pathDelimiter(".").occurrenceInGroupDelimiter(".")
                        .flattenMode(FlattenMode.GROUPED).build();
        
        JsonParser parser = new JsonParser();
        JsonElement jsonElement = parser.parse(json);
        JsonObject job = jsonElement.getAsJsonObject();
        
        try {
            flattener.flatten(job);
        } catch (IllegalStateException ill) {
            Assert.assertEquals("path delimiter and occurrence delimiter cannot be the same", ill.getMessage());
            throw ill;
        }
    }
    
    private void printMap(Multimap<String,String> fieldMap) {
        TreeMultimap<String,String> sorted = TreeMultimap.create(fieldMap);
        for (String key : sorted.keySet()) {
            System.out.print(key + ": ");
            Collection<String> values = fieldMap.get(key);
            for (String value : values) {
                System.out.print("[" + value + "]");
            }
            System.out.print("\n");
        }
        System.out.println();
    }
    
    private void printJson(String json) {
        System.out.println();
        System.out.println(json);
        System.out.println();
    }
}
