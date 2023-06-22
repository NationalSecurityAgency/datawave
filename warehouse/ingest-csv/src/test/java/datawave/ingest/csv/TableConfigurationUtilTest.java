package datawave.ingest.csv;

import datawave.ingest.config.TableConfigCache;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.mapreduce.job.TableConfigurationUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.xerces.impl.dv.util.Base64;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TableConfigurationUtilTest {

    private static MiniAccumuloCluster mac;
    private static Configuration conf = new Configuration();
    private static File tempCacheFile;

    @BeforeClass
    public static void startCluster() throws Exception {
        File macDir = new File(System.getProperty("user.dir") + "/target/mac/" + TableConfigurationUtilTest.class.getName());
        if (macDir.exists())
            FileUtils.deleteDirectory(macDir);
        macDir.mkdirs();
        mac = new MiniAccumuloCluster(new MiniAccumuloConfig(macDir, "pass"));
        mac.start();

        tempCacheFile = File.createTempFile("tempCache", null);

    }

    @AfterClass
    public static void shutdown() throws Exception {
        mac.stop();
        tempCacheFile.deleteOnExit();

    }

    @Before
    public void setup() throws IOException {
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

        conf.set(TableConfigCache.ACCUMULO_CONFIG_CACHE_PATH_PROPERTY, tempCacheFile.getAbsolutePath());

    }

    @Test
    public void testTableCreationAndConfiguration() throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {

        conf.set(TableConfigCache.ACCUMULO_CONFIG_FILE_CACHE_ENABLE_PROPERTY, "false");

        TableConfigurationUtil tcu = new TableConfigurationUtil(conf);
        TableConfigurationUtil.registerTableNamesFromConfigFiles(conf);
        tcu.configureTables(conf);
        tcu.serializeTableConfgurationIntoConf(conf);

        validateTCU(tcu, conf);

        Assert.assertFalse(tcu.isUsingFileCache());

    }

    @Test
    public void testCacheFile() throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {

        conf.set(TableConfigCache.ACCUMULO_CONFIG_FILE_CACHE_ENABLE_PROPERTY, "true");

        TableConfigurationUtil tcu = new TableConfigurationUtil(conf);
        TableConfigurationUtil.registerTableNamesFromConfigFiles(conf);
        tcu.configureTables(conf);

        Assert.assertEquals(0, tempCacheFile.length());
        tcu.updateCacheFile();
        Assert.assertEquals(7215, tempCacheFile.length());

        tcu.serializeTableConfgurationIntoConf(conf);

        validateTCU(tcu, conf);

        Assert.assertTrue(tcu.isUsingFileCache());
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

        Assert.assertFalse(tcu.isUsingFileCache());
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
        Assert.assertFalse(tcu.isUsingFileCache());

    }

    @Test
    public void testEntryPointsWithEmptyCacheAndPropsNotSerialized() throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
        conf.set(TableConfigCache.ACCUMULO_CONFIG_FILE_CACHE_ENABLE_PROPERTY, "false");

        TableConfigurationUtil tcu = new TableConfigurationUtil(conf);
        TableConfigurationUtil.registerTableNamesFromConfigFiles(conf);
        tcu.configureTables(conf);

        TableConfigCache.getCurrentCache(conf).clear();

        validateTCU(tcu, conf);
        Assert.assertFalse(tcu.isUsingFileCache());
    }

    @Test(expected = IOException.class)
    public void testOutputTableNotInCache() throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
        conf.set(TableConfigCache.ACCUMULO_CONFIG_FILE_CACHE_ENABLE_PROPERTY, "false");

        TableConfigurationUtil tcu = new TableConfigurationUtil(conf);
        TableConfigurationUtil.registerTableNamesFromConfigFiles(conf);
        tcu.configureTables(conf);

        tcu.serializeTableConfgurationIntoConf(conf);
        TableConfigCache.getCurrentCache(conf).clear();
        tcu.addOutputTables("audit", conf);

        tcu.deserializeTableConfigs(conf);

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

        Assert.assertEquals(expectedTables, TableConfigurationUtil.getJobOutputTableNames(conf));
    }

    @Test
    public void testEmptyRegistration() {
        Configuration conf = new Configuration();
        TypeRegistry.reset();

        boolean registered = TableConfigurationUtil.registerTableNamesFromConfigFiles(conf);
        Assert.assertFalse(registered);

    }

    @Test(expected = IllegalArgumentException.class)
    public void testBogusHandler() {
        Configuration conf = new Configuration();

        conf.set("data.name", "testdatatype");
        conf.set("testdatatype.handler.classes", "datawave.handler.BogusHandler");

        TableConfigurationUtil.registerTableNamesFromConfigFiles(conf);

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

        Assert.assertEquals(expectedTables, TableConfigurationUtil.getJobOutputTableNames(conf));

    }

    private void validateTCU(TableConfigurationUtil tcu, Configuration conf) throws IOException {
        Map<String,String> shardProps = tcu.getTableProperties("datawave.shard");
        Assert.assertEquals(13, shardProps.size());

        Map<String,String> shardIndexProps = tcu.getTableProperties("datawave.shardIndex");
        Assert.assertEquals(10, shardIndexProps.size());

        Map<String,String> metaProps = tcu.getTableProperties("datawave.metadata");
        Assert.assertEquals(15, metaProps.size());

        tcu.setTableItersPrioritiesAndOpts();

        Map<Integer,Map<String,String>> shardAggs = tcu.getTableAggregators("datawave.shard");
        Assert.assertEquals(1, shardAggs.size());
        Assert.assertEquals("datawave.ingest.table.aggregator.TextIndexAggregator", shardAggs.get(10).get("tf"));

        Map<Integer,Map<String,String>> shardIndexAggs = tcu.getTableAggregators("datawave.shardIndex");
        Assert.assertEquals(1, shardIndexAggs.size());
        Assert.assertEquals("datawave.ingest.table.aggregator.GlobalIndexUidAggregator", shardIndexAggs.get(19).get("*"));

        Map<Integer,Map<String,String>> metaCombiners = tcu.getTableCombiners("datawave.metadata");
        Assert.assertEquals(3, metaCombiners.size());
        Assert.assertEquals("org.apache.accumulo.core.iterators.user.SummingCombiner", metaCombiners.get(10).get(TableConfigurationUtil.ITERATOR_CLASS_MARKER));
        Assert.assertEquals("datawave.iterators.CountMetadataCombiner", metaCombiners.get(15).get(TableConfigurationUtil.ITERATOR_CLASS_MARKER));
        Assert.assertEquals("datawave.iterators.EdgeMetadataCombiner", metaCombiners.get(19).get(TableConfigurationUtil.ITERATOR_CLASS_MARKER));

        Map<Integer,Map<String,String>> loadCombiners = tcu.getTableCombiners("datawave.loadDates");
        Assert.assertEquals(1, loadCombiners.size());
        Assert.assertEquals("org.apache.accumulo.core.iterators.user.SummingCombiner", loadCombiners.get(18).get(TableConfigurationUtil.ITERATOR_CLASS_MARKER));

        Map<String,Integer> priorities = TableConfigurationUtil.getTablePriorities(conf);
        Assert.assertEquals((Integer) 30, priorities.get("datawave.shard"));

        Map<String,Set<Text>> locGroups = tcu.getLocalityGroups("datawave.shard");
        Assert.assertEquals(2, locGroups.size());
    }
}
