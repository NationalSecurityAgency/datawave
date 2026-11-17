package datawave.ingest.table.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import datawave.test.helpers.MockTableTestJUnit5;

public class LoadDateTableConfigHelperTest extends MockTableTestJUnit5 {

    private Logger log = LoggerFactory.getLogger(LoadDateTableConfigHelperTest.class);

    private static final HashSet<Text> EXPECTED_LAC_COL_FAMILIES = Sets.newHashSet(new Text("LAC\u0000errorShard"), new Text("LAC\u0000protobufedge"),
                    new Text("LAC\u0000knowledgeShard"), new Text("LAC\u0000shard"));

    @Test
    public void testGetLoadDatesLocalityGroupParsing() throws Exception {
        String localityGroups = "LACS:LAC\u0000shard;LAC\u0000protobufedge;LAC\u0000knowledgeShard;LAC\u0000errorShard";

        Map<String,Set<Text>> actual = invokeCreateMapOfLocalityGroups(createConfigurationWithLocalityGroups(localityGroups));

        assertKeyAndColFamilies(actual, "LACS", EXPECTED_LAC_COL_FAMILIES);

        assertEquals(actual.size(), 1, "Incorrect number of keys (expected 1) in " + actual);
    }

    @Test
    public void testGetLoadDatesLocalityGroupsParsing() throws Exception {
        String localityGroups = "LACS:LAC\u0000shard;LAC\u0000protobufedge;LAC\u0000knowledgeShard;LAC\u0000errorShard," + "JAM:band;es;mies";

        Map<String,Set<Text>> actual = invokeCreateMapOfLocalityGroups(createConfigurationWithLocalityGroups(localityGroups));

        assertKeyAndColFamilies(actual, "LACS", EXPECTED_LAC_COL_FAMILIES);

        assertEquals(actual.size(), 2, "Incorrect number of keys (expected 2) in " + actual);

        assertKeyAndColFamilies(actual, "JAM", Sets.newHashSet(new Text("band"), new Text("es"), new Text("mies")));
    }

    @Test
    // null byte in column families because the configuration loader converts \u0000 to \\u0000
    public void testGetLoadDatesLocalityGroupParsingWithParsedNullByte() throws Exception {
        String localityGroups = "LACS:LAC\\u0000shard;LAC\\u0000protobufedge;LAC\\u0000knowledgeShard;LAC\\u0000errorShard";

        Map<String,Set<Text>> actual = invokeCreateMapOfLocalityGroups(createConfigurationWithLocalityGroups(localityGroups));

        assertKeyAndColFamilies(actual, "LACS", EXPECTED_LAC_COL_FAMILIES);

        assertEquals(actual.size(), 1, "Incorrect number of keys (expected 1) in " + actual);
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
        assertTrue(actual.contains("LAC\u0000protobufEdge"), errorMessage);
        assertTrue(actual.contains("LAC\u0000errorShard"), errorMessage);
        assertTrue(actual.contains("LAC\u0000shard"), errorMessage);
        assertTrue(actual.contains("LAC\u0000knowledgeShard"), errorMessage);
    }

    public void assertKeyAndColFamilies(Map<String,Set<Text>> actual, final String expectedKey, HashSet<Text> expectedColFamilies) {
        assertTrue(actual.containsKey(expectedKey), "missing expected key '" + expectedKey + "' in " + actual);

        assertTrue(actual.get(expectedKey).containsAll(expectedColFamilies), "Somethin isn't right with the " + expectedKey + ": " + actual.get(expectedKey));
    }

    public Configuration createConfigurationWithLocalityGroups(String localityGroups) {
        Configuration conf = new Configuration();
        String propertyName = "metadata.loaddates.table.locality.groups";
        conf.set(propertyName, localityGroups);
        return conf;
    }

    @SuppressWarnings("unchecked")
    private Map<String,Set<Text>> invokeCreateMapOfLocalityGroups(Configuration conf) throws Exception {
        Method method = LoadDateTableConfigHelper.class.getDeclaredMethod("createMapOfLocalityGroups", Configuration.class);
        method.setAccessible(true);
        return (Map<String,Set<Text>>) method.invoke(new LoadDateTableConfigHelper(), conf);
    }
}
