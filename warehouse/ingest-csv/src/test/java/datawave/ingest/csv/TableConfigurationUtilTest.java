package datawave.ingest.csv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.xerces.impl.dv.util.Base64;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.ingest.config.TableConfigCache;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.mapreduce.job.TableConfigurationUtil;

public class TableConfigurationUtilTest {

    private static MiniAccumuloCluster mac;
    private static Configuration conf = new Configuration();
    private static File tempCacheFile;

    @BeforeAll
    public static void startCluster() throws Exception {
        File macDir = new File(System.getProperty("user.dir") + "/target/mac/" + TableConfigurationUtilTest.class.getName());
        if (macDir.exists())
            FileUtils.deleteDirectory(macDir);
        macDir.mkdirs();
        // Expand this with our configurable MiniCluster Work.
        mac = new MiniAccumuloCluster(new MiniAccumuloConfig(macDir, "pass"));
        mac.start();
        tempCacheFile = File.createTempFile("tempCache", null);

    }

    @AfterAll
    public static void shutdown() throws Exception {
        mac.stop();
        tempCacheFile.deleteOnExit();

    }

    @BeforeEach
    public void setup() throws IOException, NoSuchFieldException, IllegalAccessException {
        TypeRegistry.reset();
        conf = new Configuration();
        conf.addResource(ClassLoader.getSystemResource("config/ingest/all-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/ingest/csv-ingest-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/ingest/edge-ingest-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/ingest/metadata-config2.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/ingest/shard-ingest-config.xml"));

        conf.set("accumulo.username", "root");
        conf.set("accumulo.password", Base64.encode("pass".getBytes()));
        conf.set("accumulo.instance.name", mac.getInstanceName());
        conf.set("accumulo.zookeepers", mac.getZooKeepers());

        conf.set(TableConfigurationUtil.TABLE_PROPERTIES_TO_CACHE,
                        "table.file.compress.*,table.file.blocksize,table.file.replication,table.iterator.minc.*,table.group.*,crypto.*");

        // Config cache? why is this a thing?
        conf.set(TableConfigCache.ACCUMULO_CONFIG_CACHE_PATH_PROPERTY, tempCacheFile.getAbsolutePath());

        Field cache = TableConfigCache.class.getDeclaredField("cache");
        cache.setAccessible(true);
        cache.set(null, null);

    }

    @Test
    public void testTableCreationAndConfiguration() throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {

        conf.set(TableConfigCache.ACCUMULO_CONFIG_FILE_CACHE_ENABLE_PROPERTY, "false");

        TableConfigurationUtil tcu = new TableConfigurationUtil(conf);
        TableConfigurationUtil.registerTableNamesFromConfigFiles(conf);
        tcu.configureTables(conf);
        tcu.serializeTableConfgurationIntoConf(conf);

        validateTCU(tcu, conf);

        assertFalse(tcu.isUsingFileCache());

    }

    @Test
    public void testCacheFile() throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {

        conf.set(TableConfigCache.ACCUMULO_CONFIG_FILE_CACHE_ENABLE_PROPERTY, "true");
        File tempCacheFile = File.createTempFile("tempCache1", null);
        conf.set(TableConfigCache.ACCUMULO_CONFIG_CACHE_PATH_PROPERTY, tempCacheFile.getAbsolutePath());

        TableConfigurationUtil tcu = new TableConfigurationUtil(conf);
        TableConfigurationUtil.registerTableNamesFromConfigFiles(conf);
        tcu.configureTables(conf);

        assertEquals(0, tempCacheFile.length());
        tcu.updateCacheFile();
        // Changed this from original 7485 fixed size. Wondering if this is fragile due to JVM differences
        assertEquals(7487, tempCacheFile.length());

        tcu.serializeTableConfgurationIntoConf(conf);

        validateTCU(tcu, conf);

        assertTrue(tcu.isUsingFileCache());
        tempCacheFile.delete();
    }

    @Test
    public void testInvalidCacheFileCausesAccumuloRead() throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {

        conf.set(TableConfigCache.ACCUMULO_CONFIG_FILE_CACHE_ENABLE_PROPERTY, "true");
        conf.set(TableConfigCache.ACCUMULO_CONFIG_CACHE_PATH_PROPERTY, "bogusCache");

        TableConfigCache.getCurrentCache(conf).clear();
        TableConfigCache.getCurrentCache(conf).setCacheFilePath(conf);

        TableConfigurationUtil tcu = new TableConfigurationUtil(conf);
        TableConfigurationUtil.registerTableNamesFromConfigFiles(conf);
        tcu.configureTables(conf);

        tcu.serializeTableConfgurationIntoConf(conf);

        validateTCU(tcu, conf);

        assertFalse(tcu.isUsingFileCache());
    }

    @Test
    public void testEntryPointsWithEmptyCache() throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
        conf.set(TableConfigCache.ACCUMULO_CONFIG_FILE_CACHE_ENABLE_PROPERTY, "false");

        TableConfigurationUtil tcu = new TableConfigurationUtil(conf);
        TableConfigurationUtil.registerTableNamesFromConfigFiles(conf);
        tcu.configureTables(conf);

        tcu.serializeTableConfgurationIntoConf(conf);

        TableConfigCache.getCurrentCache(conf).clear();

        validateTCU(tcu, conf);
        assertFalse(tcu.isUsingFileCache());

    }

    @Test
    public void testEntryPointsWithEmptyCacheAndPropsNotSerialized() throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
        conf.set(TableConfigCache.ACCUMULO_CONFIG_FILE_CACHE_ENABLE_PROPERTY, "false");

        TableConfigurationUtil tcu = new TableConfigurationUtil(conf);
        TableConfigurationUtil.registerTableNamesFromConfigFiles(conf);
        tcu.configureTables(conf);

        TableConfigCache.getCurrentCache(conf).clear();

        validateTCU(tcu, conf);
        assertFalse(tcu.isUsingFileCache());
    }

    @Test
    public void testDeserializeOutputTableNotInCache() throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
        conf.set(TableConfigCache.ACCUMULO_CONFIG_FILE_CACHE_ENABLE_PROPERTY, "false");

        TableConfigurationUtil tcu = new TableConfigurationUtil(conf);
        TableConfigurationUtil.registerTableNamesFromConfigFiles(conf);
        tcu.configureTables(conf);

        tcu.serializeTableConfgurationIntoConf(conf);
        TableConfigCache.getCurrentCache(conf).clear();
        TableConfigurationUtil.addOutputTables("audit", conf);

        assertThrows(IOException.class, () -> TableConfigurationUtil.deserializeTableConfigs(conf));

    }

    @Test
    public void testOutputTableNotInCache() throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
        conf.set(TableConfigCache.ACCUMULO_CONFIG_FILE_CACHE_ENABLE_PROPERTY, "true");
        File tempCacheFile = File.createTempFile("tempCache2", null);
        conf.set(TableConfigCache.ACCUMULO_CONFIG_CACHE_PATH_PROPERTY, tempCacheFile.getAbsolutePath());

        TableConfigurationUtil tcu = new TableConfigurationUtil(conf);
        TableConfigurationUtil.registerTableNamesFromConfigFiles(conf);
        tcu.configureTables(conf);

        tcu.updateCacheFile();

        TableConfigurationUtil.addOutputTables("nonexistent_table", conf);
        try {
            assertThrows(IOException.class, () -> tcu.serializeTableConfgurationIntoConf(conf));
        } finally {
            tempCacheFile.delete();
        }

    }

    @Test
    public void testTableRegistration() {
        Configuration conf = new Configuration();
        conf.addResource(ClassLoader.getSystemResource("config/ingest/all-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/ingest/csv-ingest-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/ingest/edge-ingest-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/ingest/metadata-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/ingest/shard-ingest-config.xml"));

        TableConfigurationUtil.registerTableNamesFromConfigFiles(conf);
        Set<String> expectedTables = new HashSet<String>();
        expectedTables.add("edge");
        expectedTables.add("datawave.shardIndex");
        expectedTables.add("datawave.shardReverseIndex");
        expectedTables.add("datawave.shard");
        expectedTables.add("DataWaveMetadata");
        expectedTables.add("LoadDates");

        assertEquals(expectedTables, TableConfigurationUtil.getJobOutputTableNames(conf));
    }

    @Test
    public void testEmptyRegistration() {
        Configuration conf = new Configuration();
        TypeRegistry.reset();

        boolean registered = TableConfigurationUtil.registerTableNamesFromConfigFiles(conf);
        assertFalse(registered);

    }

    @Test
    public void testBogusHandler() {
        Configuration conf = new Configuration();

        conf.set("data.name", "testdatatype");
        conf.set("testdatatype.handler.classes", "datawave.handler.BogusHandler");

        assertThrows(IllegalArgumentException.class, () -> TableConfigurationUtil.registerTableNamesFromConfigFiles(conf));

    }

    @Test
    public void testOutputTables() {
        Configuration conf = new Configuration();
        conf.set(TableConfigurationUtil.JOB_OUTPUT_TABLE_NAMES, "shard,shardIndex,shardReverseIndex");
        TableConfigurationUtil.addOutputTables("myExtraTable", conf);

        Set<String> expectedTables = new HashSet<String>();
        expectedTables.add("shard");
        expectedTables.add("shardIndex");
        expectedTables.add("shardReverseIndex");
        expectedTables.add("myExtraTable");

        assertEquals(expectedTables, TableConfigurationUtil.getJobOutputTableNames(conf));

    }

    private void validateTCU(TableConfigurationUtil tcu, Configuration conf) throws IOException {
        Map<String,String> shardProps = tcu.getTableProperties("datawave.shard");
        assertEquals(13, shardProps.size());

        Map<String,String> shardIndexProps = tcu.getTableProperties("datawave.shardIndex");
        assertEquals(10, shardIndexProps.size());

        Map<String,String> metaProps = tcu.getTableProperties("datawave.metadata");
        assertEquals(18, metaProps.size());

        tcu.setTableItersPrioritiesAndOpts();

        Map<Integer,Map<String,String>> shardAggs = tcu.getTableAggregators("datawave.shard");
        assertEquals(1, shardAggs.size());
        assertEquals("datawave.ingest.table.aggregator.TextIndexAggregator", shardAggs.get(19).get("tf"));

        Map<Integer,Map<String,String>> shardIndexAggs = tcu.getTableAggregators("datawave.shardIndex");
        assertEquals(1, shardIndexAggs.size());
        assertEquals("datawave.ingest.table.aggregator.GlobalIndexUidAggregator", shardIndexAggs.get(19).get("*"));

        Map<Integer,Map<String,String>> metaCombiners = tcu.getTableCombiners("datawave.metadata");
        assertEquals(5, metaCombiners.size());
        assertEquals("datawave.iterators.FrequencyMetadataAggregator$FrequencyMetadataCombiner",
                        metaCombiners.get(10).get(TableConfigurationUtil.ITERATOR_CLASS_MARKER));
        assertEquals("datawave.iterators.FrequencyMetadataAggregator$FrequencyMetadataCombiner",
                        metaCombiners.get(11).get(TableConfigurationUtil.ITERATOR_CLASS_MARKER));
        assertEquals("datawave.iterators.FrequencyMetadataAggregator$FrequencyMetadataCombiner",
                        metaCombiners.get(12).get(TableConfigurationUtil.ITERATOR_CLASS_MARKER));
        assertEquals("datawave.iterators.CountMetadataCombiner", metaCombiners.get(15).get(TableConfigurationUtil.ITERATOR_CLASS_MARKER));
        assertEquals("datawave.iterators.EdgeMetadataCombiner", metaCombiners.get(19).get(TableConfigurationUtil.ITERATOR_CLASS_MARKER));

        Map<Integer,Map<String,String>> loadCombiners = tcu.getTableCombiners("datawave.loadDates");
        assertEquals(1, loadCombiners.size());
        assertEquals("org.apache.accumulo.core.iterators.user.SummingCombiner", loadCombiners.get(18).get(TableConfigurationUtil.ITERATOR_CLASS_MARKER));

        Map<String,Integer> priorities = TableConfigurationUtil.getTablePriorities(conf);
        assertEquals((Integer) 30, priorities.get("datawave.shard"));

        Map<String,Set<Text>> locGroups = tcu.getLocalityGroups("datawave.shard");
        assertEquals(2, locGroups.size());
    }
}
