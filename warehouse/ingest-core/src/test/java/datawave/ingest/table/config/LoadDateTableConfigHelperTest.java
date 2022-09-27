package datawave.ingest.table.config;

import com.google.common.collect.Sets;
import datawave.test.helpers.MockTableTest;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LoadDateTableConfigHelperTest extends MockTableTest {
    
    private Logger log = Logger.getLogger(LoadDateTableConfigHelperTest.class);
    
    private static final HashSet<Text> EXPECTED_LAC_COL_FAMILIES = Sets.newHashSet(new Text("LAC\u0000errorShard"), new Text("LAC\u0000protobufedge"),
                    new Text("LAC\u0000knowledgeShard"), new Text("LAC\u0000shard"));
    
    @Test
    public void testGetLoadDatesLocalityGroupParsing() throws Exception {
        String localityGroups = "LACS:LAC\u0000shard;LAC\u0000protobufedge;LAC\u0000knowledgeShard;LAC\u0000errorShard";
        Class<LoadDateTableConfigHelper> clazz = LoadDateTableConfigHelper.class;
        Method method = clazz.getDeclaredMethod("createMapOfLocalityGroups", Configuration.class);
        method.setAccessible(true);
        Map<String,Set<Text>> actual = (Map<String,Set<Text>>) method.invoke(new LoadDateTableConfigHelper(),
                        createConfigurationWithLocalityGroups(localityGroups));
        
        assertKeyAndColFamilies(actual, "LACS", EXPECTED_LAC_COL_FAMILIES);
        
        Assertions.assertEquals(actual.size(), 1, "Incorrect number of keys (expected 1) in " + actual);
    }
    
    @Test
    public void testGetLoadDatesLocalityGroupsParsing() throws Exception {
        String localityGroups = "LACS:LAC\u0000shard;LAC\u0000protobufedge;LAC\u0000knowledgeShard;LAC\u0000errorShard," + "JAM:band;es;mies";
        Class<LoadDateTableConfigHelper> clazz = LoadDateTableConfigHelper.class;
        Method method = clazz.getDeclaredMethod("createMapOfLocalityGroups", Configuration.class);
        method.setAccessible(true);
        Map<String,Set<Text>> actual = (Map<String,Set<Text>>) method.invoke(new LoadDateTableConfigHelper(),
                        createConfigurationWithLocalityGroups(localityGroups));
        
        assertKeyAndColFamilies(actual, "LACS", EXPECTED_LAC_COL_FAMILIES);
        
        Assertions.assertEquals(actual.size(), 2, "Incorrect number of keys (expected 2) in " + actual);
        
        assertKeyAndColFamilies(actual, "JAM", Sets.newHashSet(new Text("band"), new Text("es"), new Text("mies")));
    }
    
    @Test
    // null byte in column families because the configuration loader converts \u0000 to \\u0000
    public void testGetLoadDatesLocalityGroupParsingWithParsedNullByte() throws Exception {
        String localityGroups = "LACS:LAC\\u0000shard;LAC\\u0000protobufedge;LAC\\u0000knowledgeShard;LAC\\u0000errorShard";
        
        Class<LoadDateTableConfigHelper> clazz = LoadDateTableConfigHelper.class;
        Method method = clazz.getDeclaredMethod("createMapOfLocalityGroups", Configuration.class);
        method.setAccessible(true);
        Map<String,Set<Text>> actual = (Map<String,Set<Text>>) method.invoke(new LoadDateTableConfigHelper(),
                        createConfigurationWithLocalityGroups(localityGroups));
        
        assertKeyAndColFamilies(actual, "LACS", EXPECTED_LAC_COL_FAMILIES);
        
        Assertions.assertEquals(actual.size(), 1, "Incorrect number of keys (expected 1) in " + actual);
    }
    
    @Test
    public void testLocalityGroupsProperlyLoadedFromConfig() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        LoadDateTableConfigHelper loadDateTableConfigHelper = new LoadDateTableConfigHelper();
        Configuration configuration = new Configuration();
        configuration.addResource(ClassLoader.getSystemResource("config/metadata-config.xml"));
        configuration.set(LoadDateTableConfigHelper.LOAD_DATES_TABLE_NAME_PROP, TABLE_NAME);
        loadDateTableConfigHelper.setup(TABLE_NAME, configuration, log);
        loadDateTableConfigHelper.configure(tableOperations);
        
        String actual = tableOperations.getLocalityGroups(TABLE_NAME).toString();
        String errorMessage = "Incorrect result (possibly the null character): " + actual;
        Assertions.assertTrue(actual.contains("LAC\u0000protobufEdge"), errorMessage);
        Assertions.assertTrue(actual.contains("LAC\u0000errorShard"), errorMessage);
        Assertions.assertTrue(actual.contains("LAC\u0000shard"), errorMessage);
        Assertions.assertTrue(actual.contains("LAC\u0000knowledgeShard"), errorMessage);
    }
    
    public void assertKeyAndColFamilies(Map<String,Set<Text>> actual, final String expectedKey, HashSet<Text> expectedColFamilies) {
        Assertions.assertTrue(actual.containsKey(expectedKey), "missing expected key '" + expectedKey + "' in " + actual);
        
        Assertions.assertTrue(actual.get(expectedKey).containsAll(expectedColFamilies),
                        "Somethin isn't right with the " + expectedKey + ": " + actual.get(expectedKey));
    }
    
    public Configuration createConfigurationWithLocalityGroups(String localityGroups) {
        Configuration conf = new Configuration();
        String propertyName = "metadata.loaddates.table.locality.groups";
        conf.set(propertyName, localityGroups);
        return conf;
    }
}
