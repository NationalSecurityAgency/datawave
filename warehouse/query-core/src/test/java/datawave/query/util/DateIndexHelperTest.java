package datawave.query.util;

import datawave.ingest.mapreduce.handler.dateindex.DateIndexUtil;
import datawave.query.MockAccumuloRecordWriter;
import org.apache.accumulo.core.client.*;
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
import org.junit.*;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class DateIndexHelperTest {
    private static final String DATE_INDEX_TABLE_NAME = "dateIndex";
    private static Connector connector = null;
    private static final Logger log = Logger.getLogger(DateIndexHelperTest.class);
    private static MockAccumuloRecordWriter recordWriter;
    private static Authorizations auths = new Authorizations("A", "BB", "CCCC", "DDD", "E");
    
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
        tops.create(DATE_INDEX_TABLE_NAME);
        BatchWriterConfig bwCfg = new BatchWriterConfig().setMaxLatency(1, TimeUnit.SECONDS).setMaxMemory(1000L).setMaxWriteThreads(1);
        recordWriter.addWriter(new Text(DATE_INDEX_TABLE_NAME), connector.createBatchWriter(DATE_INDEX_TABLE_NAME, bwCfg));
        
        // intialize some mappings
        write("20100101", new int[] {1}, "test", "LOADED", "LOAD_DATE", "20100102", "A");
        write("20100102", new int[] {5}, "test", "LOADED", "LOAD_DATE", "20100102", "BB");
        write("20100103", new int[] {1, 3}, "test", "LOADED", "LOAD_DATE", "20100104", "CCCC");
        
        dumpTable(auths);
    }
    
    public static void dumpTable(Authorizations auths) throws TableNotFoundException {
        TableOperations tops = connector.tableOperations();
        org.apache.accumulo.core.client.Scanner scanner = connector.createScanner(DATE_INDEX_TABLE_NAME, auths);
        Iterator<Map.Entry<Key,Value>> iterator = scanner.iterator();
        System.out.println("*************** " + DATE_INDEX_TABLE_NAME + " ********************");
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
        recordWriter.write(new Text(DATE_INDEX_TABLE_NAME), m);
    }
    
    public static KeyValue getDateIndexEntry(String shardDate, int[] shardIndicies, String dataType, String type, String dateField, String dateValue,
                    ColumnVisibility visibility) throws ParseException {
        // The row is the date to index yyyyMMdd
        String row = dateValue;
        
        // the colf is the type (e.g. LOAD or ACTIVITY)
        String colf = type;
        
        // the colq is the event date yyyyMMdd \0 the datatype \0 the field name
        String colq = shardDate + '\0' + dataType + '\0' + dateField;
        
        // the value is a bitset denoting the shard
        BitSet bits = DateIndexUtil.getBits(shardIndicies[0]);
        for (int i = 1; i < shardIndicies.length; i++) {
            bits = DateIndexUtil.merge(bits, DateIndexUtil.getBits(shardIndicies[i]));
        }
        Value shardList = new Value(bits.toByteArray());
        
        // create the key
        Key key = new Key(row, colf, colq, visibility, DateIndexUtil.getBeginDate(dateValue).getTime());
        
        return new KeyValue(key, shardList);
    }
    
    @Test
    public void testDateIndexHelperDescription() throws Exception {
        DateIndexHelper helper = new DateIndexHelperFactory().createDateIndexHelper().initialize(connector, DATE_INDEX_TABLE_NAME,
                        Collections.singleton(auths), 2, 0.9f);
        
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
    }
    
    @Test
    public void testDateIndexHelperHint() throws Exception {
        DateIndexHelper helper = new DateIndexHelperFactory().createDateIndexHelper().initialize(connector, DATE_INDEX_TABLE_NAME,
                        Collections.singleton(auths), 2, 0.9f);
        
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
