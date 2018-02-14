package datawave.ingest.mapreduce;

import datawave.ingest.mapreduce.job.TableConfigHelperFactory;
import datawave.ingest.table.config.ShardTableConfigHelper;
import datawave.ingest.table.config.TableConfigHelper;
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
    
    @Before
    public void setup() throws Exception {
        conf = new Configuration();
        
        conf.set("shard.table.name", "shard");
        conf.set("shard.table.config.class", ShardTableConfigHelper.class.getName());
        
        conf.set("test.shard.table.name", "testShard");
        conf.set("testShard.table.config.class", ShardTableConfigHelper.class.getName());
        conf.set("testShard.table.config.prefix", "test");
        
        instance = new MockInstance();
        connector = instance.getConnector("root", new PasswordToken(new byte[0]));
        tops = connector.tableOperations();
        
        recreateTable(tops, "shard");
        recreateTable(tops, "testShard");
    }
    
    private void recreateTable(TableOperations tops, String table) throws Exception {
        if (tops.exists(table)) {
            tops.delete(table);
        }
        tops.create(table);
    }
    
    @Test
    public void shouldSetupTableWithOverrides() throws Exception {
        TableConfigHelper helper = TableConfigHelperFactory.create("testShard", conf, logger);
        helper.configure(tops);
        
        TablePropertiesMap testShardProperties = new TablePropertiesMap(tops, "testShard");
        TablePropertiesMap shardProperties = new TablePropertiesMap(tops, "shard");
        
        assertThat(testShardProperties.get("table.iterator.majc.agg"), is("10,datawave.iterators.PropogatingIterator"));
        assertThat(shardProperties.get("table.iterator.majc.agg"), nullValue());
    }
    
    @Test
    public void shouldSetupTablesWithoutOverrides() throws Exception {
        TableConfigHelper helper = TableConfigHelperFactory.create("shard", conf, logger);
        helper.configure(tops);
        
        TablePropertiesMap testShardProperties = new TablePropertiesMap(tops, "testShard");
        TablePropertiesMap shardProperties = new TablePropertiesMap(tops, "shard");
        
        assertThat(testShardProperties.get("table.iterator.majc.agg"), nullValue());
        assertThat(shardProperties.get("table.iterator.majc.agg"), is("10,datawave.iterators.PropogatingIterator"));
    }
}
