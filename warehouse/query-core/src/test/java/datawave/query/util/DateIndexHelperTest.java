package datawave.query.util;

import com.github.benmanes.caffeine.cache.Cache;
import datawave.ingest.mapreduce.handler.dateindex.DateIndexUtil;
import datawave.query.MockAccumuloRecordWriter;
import datawave.util.TableName;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import datawave.accumulo.inmemory.InMemoryInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.runner.RunWith;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/MetadataHelperContext.xml", "classpath:/CacheContext.xml"})
public class DateIndexHelperTest implements ApplicationContextAware {
    private static Connector connector = null;
    private static final Logger log = Logger.getLogger(DateIndexHelperTest.class);
    private static MockAccumuloRecordWriter recordWriter;
    private static Authorizations auths = new Authorizations("A", "BB", "CCCC", "DDD", "E");
    // added to look at the cache
    protected ApplicationContext applicationContext;
    
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
        log.warn("applicationcontextaware setting of applicationContext:" + applicationContext);
    }
    
    @Autowired
    private DateIndexHelperFactory dateIndexHelperFactory;
    
    @Before
    public void setup() throws Exception {}
    
    @BeforeClass
    public static void setUp() throws Exception {
        // Set logging levels
        Logger.getRootLogger().setLevel(Level.OFF);
        
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        System.setProperty("file.encoding", "UTF8");
        
        // create mock instance and connector
        InMemoryInstance i = new InMemoryInstance(DateIndexHelperTest.class.getName());
        connector = i.getConnector("root", new PasswordToken(""));
        recordWriter = new MockAccumuloRecordWriter();
        TableOperations tops = connector.tableOperations();
        tops.create(TableName.DATE_INDEX);
        tops = connector.tableOperations();
        tops.create("FOO_TABLE"); // unused except for testing the cache key
        
        BatchWriterConfig bwCfg = new BatchWriterConfig().setMaxLatency(1, TimeUnit.SECONDS).setMaxMemory(1000L).setMaxWriteThreads(1);
        recordWriter.addWriter(new Text(TableName.DATE_INDEX), connector.createBatchWriter(TableName.DATE_INDEX, bwCfg));
        
        // intialize some mappings
        write("20100101", new int[] {1}, "test", "LOADED", "LOAD_DATE", "20100102", "A");
        write("20100102", new int[] {5}, "test", "LOADED", "LOAD_DATE", "20100102", "BB");
        write("20100103", new int[] {1, 3}, "test", "LOADED", "LOAD_DATE", "20100104", "CCCC");
        
        dumpTable(auths);
    }
    
    public static void dumpTable(Authorizations auths) throws TableNotFoundException {
        TableOperations tops = connector.tableOperations();
        org.apache.accumulo.core.client.Scanner scanner = connector.createScanner(TableName.DATE_INDEX, auths);
        Iterator<Map.Entry<Key,Value>> iterator = scanner.iterator();
        System.out.println("*************** " + TableName.DATE_INDEX + " ********************");
        while (iterator.hasNext()) {
            Map.Entry<Key,Value> entry = iterator.next();
            System.out.println(entry);
        }
        System.out.println("*******************************************************************");
        scanner.close();
    }
    
    private static void write(String shardDate, int[] shardIndicies, String dataType, String type, String dateField, String dateValue, String visibility)
                    throws ParseException, IOException, InterruptedException {
        ColumnVisibility vis = new ColumnVisibility(visibility);
        KeyValue kv = getDateIndexEntry(shardDate, shardIndicies, dataType, type, dateField, dateValue, vis);
        Mutation m = new Mutation(kv.getKey().getRow());
        m.put(kv.getKey().getColumnFamily(), kv.getKey().getColumnQualifier(), vis, kv.getKey().getTimestamp(), kv.getValue());
        recordWriter.write(new Text(TableName.DATE_INDEX), m);
    }
    
    public static KeyValue getDateIndexEntry(String shardDate, int[] shardIndicies, String dataType, String type, String dateField, String dateValue,
                    ColumnVisibility visibility) throws ParseException {
        // The row is the date to index yyyyMMdd
        
        // the colf is the type (e.g. LOAD or ACTIVITY)
        
        // the colq is the event date yyyyMMdd \0 the datatype \0 the field name
        String colq = shardDate + '\0' + dataType + '\0' + dateField;
        
        // the value is a bitset denoting the shard
        BitSet bits = DateIndexUtil.getBits(shardIndicies[0]);
        for (int i = 1; i < shardIndicies.length; i++) {
            bits = DateIndexUtil.merge(bits, DateIndexUtil.getBits(shardIndicies[i]));
        }
        Value shardList = new Value(bits.toByteArray());
        
        // create the key
        Key key = new Key(dateValue, type, colq, visibility, DateIndexUtil.getBeginDate(dateValue).getTime());
        
        return new KeyValue(key, shardList);
    }
    
    @Test
    public void testDateIndexHelperDescription() throws Exception {
        DateIndexHelper helper = this.dateIndexHelperFactory.createDateIndexHelper().initialize(connector, TableName.DATE_INDEX, Collections.singleton(auths),
                        2, 0.9f);
        
        DateIndexHelper.DateTypeDescription dtd = helper.getTypeDescription("LOADED", DateIndexUtil.getBeginDate("20100102"),
                        DateIndexUtil.getEndDate("20100102"), Collections.singleton("test"));
        Assert.assertEquals(Collections.singleton("LOAD_DATE"), dtd.getFields());
        Assert.assertEquals(DateIndexUtil.getBeginDate("20100101"), dtd.getBeginDate());
        Assert.assertEquals(DateIndexUtil.getEndDate("20100102"), dtd.getEndDate());
        
        dtd = helper.getTypeDescription("LOADED", DateIndexUtil.getBeginDate("20100104"), DateIndexUtil.getEndDate("20100104"), Collections.singleton("test"));
        Assert.assertEquals(Collections.singleton("LOAD_DATE"), dtd.getFields());
        Assert.assertEquals(DateIndexUtil.getBeginDate("20100103"), dtd.getBeginDate());
        Assert.assertEquals(DateIndexUtil.getEndDate("20100103"), dtd.getEndDate());
        
        dtd = helper.getTypeDescription("LOADED", DateIndexUtil.getBeginDate("20100103"), DateIndexUtil.getEndDate("20100103"), Collections.singleton("test"));
        Assert.assertEquals(Collections.emptySet(), dtd.getFields());
        // the alg will default to the specified date range if no dates found
        Assert.assertEquals(DateIndexUtil.getBeginDate("20100103"), dtd.getBeginDate());
        Assert.assertEquals(DateIndexUtil.getEndDate("20100103"), dtd.getEndDate());
        
        // there should be 3 entries in the cache
        Assert.assertEquals(3, countCacheEntries());
        
        // create a new DateIndexHelper for each of 3 new calls. There should still be only 3 entries in the cache
        helper = this.dateIndexHelperFactory.createDateIndexHelper().initialize(connector, TableName.DATE_INDEX, Collections.singleton(auths), 2, 0.9f);
        
        helper.getTypeDescription("LOADED", DateIndexUtil.getBeginDate("20100102"), DateIndexUtil.getEndDate("20100102"), Collections.singleton("test"));
        
        helper = this.dateIndexHelperFactory.createDateIndexHelper().initialize(connector, TableName.DATE_INDEX, Collections.singleton(auths), 2, 0.9f);
        helper.getTypeDescription("LOADED", DateIndexUtil.getBeginDate("20100104"), DateIndexUtil.getEndDate("20100104"), Collections.singleton("test"));
        
        helper = this.dateIndexHelperFactory.createDateIndexHelper().initialize(connector, TableName.DATE_INDEX, Collections.singleton(auths), 2, 0.9f);
        helper.getTypeDescription("LOADED", DateIndexUtil.getBeginDate("20100103"), DateIndexUtil.getEndDate("20100103"), Collections.singleton("test"));
        
        Assert.assertEquals(3, countCacheEntries());
        
        // call with different auths, there should be one more map entry in the cache
        helper = this.dateIndexHelperFactory.createDateIndexHelper().initialize(connector, TableName.DATE_INDEX,
                        Collections.singleton(new Authorizations("Z")), 2, 0.9f);
        helper.getTypeDescription("LOADED", DateIndexUtil.getBeginDate("20100102"), DateIndexUtil.getEndDate("20100102"), Collections.singleton("test"));
        Assert.assertEquals(4, countCacheEntries());
        
        // call with different table name, there should be one more map entry in the cache
        helper = this.dateIndexHelperFactory.createDateIndexHelper().initialize(connector, "FOO_TABLE", Collections.singleton(auths), 2, 0.9f);
        helper.getTypeDescription("LOADED", DateIndexUtil.getBeginDate("20100102"), DateIndexUtil.getEndDate("20100102"), Collections.singleton("test"));
        Assert.assertEquals(5, countCacheEntries());
    }
    
    private int countCacheEntries() {
        CacheManager cacheManager = this.applicationContext.getBean("dateIndexHelperCacheManager", CacheManager.class);
        String cacheName = "getTypeDescription";
        Object nativeCache = cacheManager.getCache(cacheName).getNativeCache();
        Cache cache = (Cache) nativeCache;
        Map map = cache.asMap();
        return map.size();
    }
    
    @Test
    public void testDateIndexHelperHint() throws Exception {
        DateIndexHelper helper = this.dateIndexHelperFactory.createDateIndexHelper().initialize(connector, TableName.DATE_INDEX, Collections.singleton(auths),
                        2, 0.9f);
        
        String hint = helper.getShardsAndDaysHint("LOAD_DATE", DateIndexUtil.getBeginDate("20100102"), DateIndexUtil.getEndDate("20100102"),
                        DateIndexUtil.getBeginDate("20090101"), DateIndexUtil.getEndDate("20120101"), Collections.singleton("test"));
        Assert.assertEquals("20100101_1,20100102_5", hint);
        
        hint = helper.getShardsAndDaysHint("LOAD_DATE", DateIndexUtil.getBeginDate("20100102"), DateIndexUtil.getEndDate("20100102"),
                        DateIndexUtil.getBeginDate("20100101"), DateIndexUtil.getEndDate("20100102"), Collections.singleton("test"));
        Assert.assertEquals("20100101_1,20100102_5", hint);
        
        hint = helper.getShardsAndDaysHint("LOAD_DATE", DateIndexUtil.getBeginDate("20100102"), DateIndexUtil.getEndDate("20100102"),
                        DateIndexUtil.getBeginDate("20100102"), DateIndexUtil.getEndDate("20100102"), Collections.singleton("test"));
        Assert.assertEquals("20100102_5", hint);
        
        hint = helper.getShardsAndDaysHint("LOAD_DATE", DateIndexUtil.getBeginDate("20100104"), DateIndexUtil.getEndDate("20100104"),
                        DateIndexUtil.getBeginDate("20090101"), DateIndexUtil.getEndDate("20120101"), Collections.singleton("test"));
        Assert.assertEquals("20100103_1,20100103_3", hint);
        
        hint = helper.getShardsAndDaysHint("LOAD_DATE", DateIndexUtil.getBeginDate("20100104"), DateIndexUtil.getEndDate("20100104"),
                        DateIndexUtil.getBeginDate("20100104"), DateIndexUtil.getEndDate("20100104"), Collections.singleton("test"));
        Assert.assertEquals("", hint);
        
        hint = helper.getShardsAndDaysHint("LOAD_DATE", DateIndexUtil.getBeginDate("20100103"), DateIndexUtil.getEndDate("20100103"),
                        DateIndexUtil.getBeginDate("20090101"), DateIndexUtil.getEndDate("20120101"), Collections.singleton("test"));
        Assert.assertEquals("", hint);
    }
}
