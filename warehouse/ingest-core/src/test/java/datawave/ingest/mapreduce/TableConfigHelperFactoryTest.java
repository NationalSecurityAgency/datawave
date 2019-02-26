package datawave.ingest.mapreduce;

import datawave.ingest.mapreduce.job.TableConfigHelperFactory;
import datawave.ingest.table.config.ShardTableConfigHelper;
import datawave.ingest.table.config.TableConfigHelper;
import datawave.util.TableNames;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

public class TableConfigHelperFactoryTest {
    private static final Logger logger = Logger.getLogger(TableConfigHelperFactoryTest.class);
    
    private Configuration conf;
    private MockInstance instance;
    private Connector connector;
    private TableOperations tops;
    
    private static final String SHARD_TABLE_NAME = TableNames.SHARD_TABLE_NAME;
    private static final String TEST_SHARD_TABLE_NAME = "testShard";
    
    @Before
    public void setup() throws Exception {
        conf = new Configuration();
        
        conf.set("shard.table.name", SHARD_TABLE_NAME);
        conf.set("shard.table.config.class", ShardTableConfigHelper.class.getName());
        
        conf.set("test.shard.table.name", TEST_SHARD_TABLE_NAME);
        conf.set("testShard.table.config.class", ShardTableConfigHelper.class.getName());
        conf.set("testShard.table.config.prefix", "test");
        
        instance = new MockInstance();
        connector = instance.getConnector("root", new PasswordToken(new byte[0]));
        tops = connector.tableOperations();
        
        recreateTable(tops, SHARD_TABLE_NAME);
        recreateTable(tops, TEST_SHARD_TABLE_NAME);
    }
    
    private void recreateTable(TableOperations tops, String table) throws Exception {
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
        TablePropertiesMap shardProperties = new TablePropertiesMap(tops, SHARD_TABLE_NAME);
        
        assertThat(testShardProperties.get("table.iterator.majc.agg"), is("10,datawave.iterators.PropogatingIterator"));
        assertThat(shardProperties.get("table.iterator.majc.agg"), nullValue());
    }
    
    @Test
    public void shouldSetupTablesWithoutOverrides() throws Exception {
        TableConfigHelper helper = TableConfigHelperFactory.create(SHARD_TABLE_NAME, conf, logger);
        helper.configure(tops);
        
        TablePropertiesMap testShardProperties = new TablePropertiesMap(tops, TEST_SHARD_TABLE_NAME);
        TablePropertiesMap shardProperties = new TablePropertiesMap(tops, SHARD_TABLE_NAME);
        
        assertThat(testShardProperties.get("table.iterator.majc.agg"), nullValue());
        assertThat(shardProperties.get("table.iterator.majc.agg"), is("10,datawave.iterators.PropogatingIterator"));
    }
}
