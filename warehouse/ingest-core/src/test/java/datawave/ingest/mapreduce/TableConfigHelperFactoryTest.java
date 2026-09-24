package datawave.ingest.mapreduce;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.nio.file.Files;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.ingest.mapreduce.job.TableConfigHelperFactory;
import datawave.ingest.table.config.ShardTableConfigHelper;
import datawave.ingest.table.config.TableConfigHelper;
import datawave.table.constants.TableName;

/**
 * Test uses a temporary MiniAccumulo cluster.
 */
public class TableConfigHelperFactoryTest {
    private static final Logger logger = Logger.getLogger(TableConfigHelperFactoryTest.class);
    private static MiniAccumuloCluster mac;
    private static AccumuloClient client;

    private Configuration conf;
    private TableOperations tops;

    private static final String TEST_SHARD_TABLE_NAME = "testShard";

    @BeforeAll
    public static void startCluster() throws Exception {
        File macDir = Files.createTempDirectory("datawave-table-config-").toFile();

        MiniAccumuloConfig config = new MiniAccumuloConfig(macDir, "pass");
        config.setNumTservers(1);
        config.setSiteConfig(Map.of(Property.INSTANCE_VOLUMES.getKey(), macDir.toPath().resolve("accumulo").toUri().toString()));

        mac = new MiniAccumuloCluster(config);
        mac.start();
    }

    @BeforeEach
    public void setup() throws Exception {
        conf = new Configuration();

        conf.set("shard.table.name", TableName.SHARD);
        conf.set("shard.table.config.class", ShardTableConfigHelper.class.getName());

        conf.set("test.shard.table.name", TEST_SHARD_TABLE_NAME);
        conf.set("testShard.table.config.class", ShardTableConfigHelper.class.getName());
        conf.set("testShard.table.config.prefix", "test");

        if (client != null) {
            client.close();
        }
        client = mac.createAccumuloClient("root", new PasswordToken("pass"));
        tops = client.tableOperations();

        recreateTable(tops, TableName.SHARD);
        recreateTable(tops, TEST_SHARD_TABLE_NAME);
    }

    @AfterAll
    public static void shutdown() throws Exception {
        if (client != null) {
            client.close();
        }
        if (mac != null) {
            mac.stop();
        }
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
        assertEquals("19,datawave.iterators.PropogatingIterator", testShardProperties.get("table.iterator.majc.agg"));

        TablePropertiesMap shardProperties = new TablePropertiesMap(tops, TableName.SHARD);
        assertNull(shardProperties.get("table.iterator.majc.agg"));
    }

    @Test
    public void shouldSetupTablesWithoutOverrides() throws Exception {
        TableConfigHelper helper = TableConfigHelperFactory.create(TableName.SHARD, conf, logger);
        helper.configure(tops);

        TablePropertiesMap testShardProperties = new TablePropertiesMap(tops, TEST_SHARD_TABLE_NAME);
        assertNull(testShardProperties.get("table.iterator.majc.agg"));

        TablePropertiesMap shardProperties = new TablePropertiesMap(tops, TableName.SHARD);
        assertEquals("19,datawave.iterators.PropogatingIterator", shardProperties.get("table.iterator.majc.agg"));
    }
}
