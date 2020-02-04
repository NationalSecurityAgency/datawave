package datawave.ingest.mapreduce;

import datawave.ingest.mapreduce.job.TableConfigHelperFactory;
import datawave.ingest.table.config.ShardTableConfigHelper;
import datawave.ingest.table.config.TableConfigHelper;
import datawave.util.TableName;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

import java.io.File;

/**
 * Test uses mini accumulo cluster. Files are stored in warehouse/ingest-core/target/mac/datawave.ingest.mapreduce.TableConfigHelperFactoryTest
 */
public class TableConfigHelperFactoryTest {
    private static final Logger logger = Logger.getLogger(TableConfigHelperFactoryTest.class);
    private static MiniAccumuloCluster mac;
    
    private Configuration conf;
    private TableOperations tops;
    
    private static final String TEST_SHARD_TABLE_NAME = "testShard";
    
    @BeforeClass
    public static void startCluster() throws Exception {
        File macDir = new File(System.getProperty("user.dir") + "/target/mac/" + TableConfigHelperFactoryTest.class.getName());
        if (macDir.exists())
            FileUtils.deleteDirectory(macDir);
        macDir.mkdirs();
        mac = new MiniAccumuloCluster(new MiniAccumuloConfig(macDir, "pass"));
        mac.start();
    }
    
    @Before
    public void setup() throws Exception {
        conf = new Configuration();
        
        conf.set("shard.table.name", TableName.SHARD);
        conf.set("shard.table.config.class", ShardTableConfigHelper.class.getName());
        
        conf.set("test.shard.table.name", TEST_SHARD_TABLE_NAME);
        conf.set("testShard.table.config.class", ShardTableConfigHelper.class.getName());
        conf.set("testShard.table.config.prefix", "test");
        
        tops = mac.getConnector("root", "pass").tableOperations();
        
        recreateTable(tops, TableName.SHARD);
        recreateTable(tops, TEST_SHARD_TABLE_NAME);
    }
    
    @AfterClass
    public static void shutdown() throws Exception {
        mac.stop();
    }
    
    private static void recreateTable(TableOperations tops, String table) throws Exception {
        if (tops.exists(table)) {
            tops.delete(table);
        }
        tops.create(table);
    }
    
    @Test
    public void shouldSetupTableWithOverrides() throws Exception {
        TableConfigHelper helper = TableConfigHelperFactory.create(TEST_SHARD_TABLE_NAME, conf, logger);
        helper.configure(tops);
        
        TablePropertiesMap testShardProperties = new TablePropertiesMap(tops, TEST_SHARD_TABLE_NAME);
        TablePropertiesMap shardProperties = new TablePropertiesMap(tops, TableName.SHARD);
        
        assertThat(testShardProperties.get("table.iterator.majc.agg"), is("10,datawave.iterators.PropogatingIterator"));
        assertThat(shardProperties.get("table.iterator.majc.agg"), nullValue());
    }
    
    @Test
    public void shouldSetupTablesWithoutOverrides() throws Exception {
        TableConfigHelper helper = TableConfigHelperFactory.create(TableName.SHARD, conf, logger);
        helper.configure(tops);
        
        TablePropertiesMap testShardProperties = new TablePropertiesMap(tops, TEST_SHARD_TABLE_NAME);
        TablePropertiesMap shardProperties = new TablePropertiesMap(tops, TableName.SHARD);
        
        assertThat(testShardProperties.get("table.iterator.majc.agg"), nullValue());
        assertThat(shardProperties.get("table.iterator.majc.agg"), is("10,datawave.iterators.PropogatingIterator"));
    }
}
