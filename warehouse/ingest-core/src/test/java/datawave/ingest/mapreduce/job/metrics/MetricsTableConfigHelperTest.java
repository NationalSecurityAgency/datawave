package datawave.ingest.mapreduce.job.metrics;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.TableOperations;
import datawave.accumulo.inmemory.InMemoryInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

public class MetricsTableConfigHelperTest {
    
    private static final String CONFIG_LOC = "/datawave/ingest/mapreduce/job/metrics/test-metrics-config.xml";
    
    private Logger logger = Logger.getLogger(this.getClass());
    private Configuration conf = getClasspathConfig(CONFIG_LOC);
    private MetricsTableConfigHelper configHelper;
    private TableOperations tops;
    
    @Before
    public void setUp() throws Exception {
        tops = new InMemoryInstance().getConnector("user", new PasswordToken("pass")).tableOperations();
        
        String tableName = MetricsConfiguration.getTable(conf);
        
        tops.create(tableName);
        
        configHelper = new MetricsTableConfigHelper();
        configHelper.setup(tableName, conf, logger);
    }
    
    @Test
    public void shouldDisableMetricsWhenConfigFails() throws Exception {
        String table = MetricsConfiguration.getTable(conf);
        
        // Using mocks to force error during tops.getIteratorSetting()
        TableOperations tops = EasyMock.createMock(TableOperations.class);
        EasyMock.expect(tops.getIteratorSetting(EasyMock.anyString(), EasyMock.anyString(), EasyMock.anyObject(IteratorScope.class))).andThrow(
                        new AccumuloException(""));
        EasyMock.replay(tops);
        
        // run test
        configHelper.configure(tops);
        
        assertFalse(MetricsConfiguration.isEnabled(conf));
    }
    
    @Test
    public void shouldDisableIfTableNamesDontMatch() throws Exception {
        configHelper = new MetricsTableConfigHelper();
        configHelper.setup("wrongTableName", conf, logger);
        configHelper.configure(tops);
        
        assertFalse(MetricsConfiguration.isEnabled(conf));
    }
    
    @Test
    public void shouldConfigureAggregators() throws Exception {
        String table = MetricsConfiguration.getTable(conf);
        
        configHelper.configure(tops);
        IteratorSetting is = tops.getIteratorSetting(table, "sum", IteratorScope.scan);
        
        assertEquals(SummingCombiner.class.getName(), is.getIteratorClass());
        assertEquals(TestKeyValueCountMetricsReceiver.ITER_PRIORITY, is.getPriority());
        
        Map<String,String> options = is.getOptions();
        String type = options.get("type");
        String columns = options.get("columns");
        
        assertEquals(Metric.KV_PER_TABLE.toString(), columns);
        assertEquals("STRING", type);
    }
    
    @Test
    public void shouldUpdateCombinerIfAlreadyExists() throws Exception {
        String table = MetricsConfiguration.getTable(conf);
        
        // Setup an initial iterator
        Map<String,String> options = new TreeMap<>();
        options.put("type", "STRING");
        options.put("columns", "COLUMN1");
        
        IteratorSetting is = new IteratorSetting(TestKeyValueCountMetricsReceiver.ITER_PRIORITY, TestKeyValueCountMetricsReceiver.ITER_NAME,
                        SummingCombiner.class.getName(), options);
        
        tops.attachIterator(table, is);
        
        // run test
        configHelper.configure(tops);
        
        // verify updates
        IteratorSetting newIter = tops.getIteratorSetting(table, "sum", IteratorScope.scan);
        
        assertEquals(SummingCombiner.class.getName(), newIter.getIteratorClass());
        assertEquals(TestKeyValueCountMetricsReceiver.ITER_PRIORITY, newIter.getPriority());
        
        Map<String,String> newOptions = newIter.getOptions();
        String type = newOptions.get("type");
        String columns = newOptions.get("columns");
        
        assertEquals("COLUMN1," + Metric.KV_PER_TABLE, columns);
        assertEquals("STRING", type);
    }
    
    private Configuration getClasspathConfig(String classpathLoc) {
        Configuration conf = new Configuration();
        conf.addResource(this.getClass().getResourceAsStream(classpathLoc));
        return conf;
    }
}
