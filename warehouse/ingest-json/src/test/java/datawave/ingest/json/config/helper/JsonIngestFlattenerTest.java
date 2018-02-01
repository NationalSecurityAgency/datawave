package datawave.ingest.json.config.helper;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import datawave.ingest.json.util.JsonObjectFlattener;
import datawave.ingest.json.util.JsonObjectFlattener.FlattenMode;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;

public class JsonIngestFlattenerTest {
    
    protected static String jsonFile = "/input/tvmaze-seinfeld.json";
    protected static String json;
    
    @BeforeClass
    public static void setup() throws URISyntaxException, IOException {
        URL data = JsonIngestFlattenerTest.class.getResource(jsonFile);
        Assert.assertNotNull(data);
        json = new String(Files.readAllBytes(Paths.get(data.toURI())));
    }
    
    @Test
    public void testFlattenGROUPED() throws Exception {
        JsonObjectFlattener flattener = new JsonIngestFlattener.Builder().flattenMode(FlattenMode.GROUPED).build();
        
        JsonParser parser = new JsonParser();
        JsonElement jsonElement = parser.parse(json);
        Multimap<String,String> fieldMap = flattener.flatten(jsonElement.getAsJsonObject());
        
        // printJson(json);
        // printMap(fieldMap);
        
        Assert.assertEquals(74, fieldMap.keySet().size());
        Assert.assertEquals(74, fieldMap.values().size());
        Assert.assertTrue(fieldMap.containsKey("URL.EMBEDDED_0.CAST_3.PERSON_0.URL_0"));
        Assert.assertEquals("Jerry Seinfeld", fieldMap.get("NAME.EMBEDDED_0.CAST_0.CHARACTER_0.NAME_0").iterator().next());
        Assert.assertEquals("Cosmo Kramer", fieldMap.get("NAME.EMBEDDED_0.CAST_1.CHARACTER_0.NAME_0").iterator().next());
        Assert.assertEquals("George Louis Costanza", fieldMap.get("NAME.EMBEDDED_0.CAST_2.CHARACTER_0.NAME_0").iterator().next());
        Assert.assertEquals("Elaine Marie Benes", fieldMap.get("NAME.EMBEDDED_0.CAST_3.CHARACTER_0.NAME_0").iterator().next());
    }
    
    @Test
    public void testFlattenGROUPED_AND_NORMAL() throws Exception {
        JsonObjectFlattener flattener = new JsonIngestFlattener.Builder().flattenMode(FlattenMode.GROUPED_AND_NORMAL).build();
        
        JsonParser parser = new JsonParser();
        JsonElement jsonElement = parser.parse(json);
        Multimap<String,String> fieldMap = flattener.flatten(jsonElement.getAsJsonObject());
        
        // printJson(json);
        // printMap(fieldMap);
        
        Assert.assertEquals(74, fieldMap.keySet().size());
        Assert.assertEquals(74, fieldMap.values().size());
        
        Assert.assertEquals(1, fieldMap.get("EMBEDDED_CAST_CHARACTER_NAME.EMBEDDED_0.CAST_0.CHARACTER_0.NAME_0").size());
        Assert.assertEquals(1, fieldMap.get("EMBEDDED_CAST_CHARACTER_NAME.EMBEDDED_0.CAST_1.CHARACTER_0.NAME_0").size());
        Assert.assertEquals(1, fieldMap.get("EMBEDDED_CAST_CHARACTER_NAME.EMBEDDED_0.CAST_2.CHARACTER_0.NAME_0").size());
        Assert.assertEquals(1, fieldMap.get("EMBEDDED_CAST_CHARACTER_NAME.EMBEDDED_0.CAST_3.CHARACTER_0.NAME_0").size());
        
        Assert.assertEquals("Jerry Seinfeld", fieldMap.get("EMBEDDED_CAST_CHARACTER_NAME.EMBEDDED_0.CAST_0.CHARACTER_0.NAME_0").iterator().next());
        Assert.assertEquals("Cosmo Kramer", fieldMap.get("EMBEDDED_CAST_CHARACTER_NAME.EMBEDDED_0.CAST_1.CHARACTER_0.NAME_0").iterator().next());
        Assert.assertEquals("George Louis Costanza", fieldMap.get("EMBEDDED_CAST_CHARACTER_NAME.EMBEDDED_0.CAST_2.CHARACTER_0.NAME_0").iterator().next());
        Assert.assertEquals("Elaine Marie Benes", fieldMap.get("EMBEDDED_CAST_CHARACTER_NAME.EMBEDDED_0.CAST_3.CHARACTER_0.NAME_0").iterator().next());
        
    }
    
    @Test
    public void testTester() throws IOException {
        
        String jsonFile = JsonIngestFlattenerTest.class.getResource("/input/tvmaze-seinfeld.json").getFile();
        String configFile1 = JsonIngestFlattenerTest.class.getResource("/config/ingest/tvmaze-ingest-config.xml").getFile();
        
        // Add all-config.xml only for its *.ingest.policy.enforcer.class declaration. Without it, DataTypeHelperImpl will complain
        String configFile2 = JsonIngestFlattenerTest.class.getResource("/config/ingest/all-config.xml").getFile();
        
        JsonIngestFlattener.Test.main(new String[] {"--file", jsonFile, "--config", configFile1 + "," + configFile2});
        
    }
    
    @Test(expected = java.lang.IllegalStateException.class)
    public void testTesterNullArgs() throws IOException {
        
        JsonIngestFlattener.Test.main(null);
        
    }
    
    @Test(expected = java.lang.IllegalStateException.class)
    public void testTesterJsonFileDoesNotExist() throws IOException {
        
        JsonIngestFlattener.Test.main(new String[] {"--file", "/this/file/definitely/does/not/exist.json"});
        
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
