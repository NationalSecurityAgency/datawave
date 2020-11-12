package datawave.iterators;

import com.google.common.collect.Maps;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.ColumnFamilyConstants;
import datawave.query.composite.CompositeMetadataHelper;
import datawave.query.util.AllFieldMetadataHelper;
import datawave.query.util.Frequency;
import datawave.query.util.FrequencyFamilyCounter;
import datawave.query.util.MetadataHelper;
import datawave.query.util.TypeMetadataHelper;
import datawave.query.util.YearMonthDay;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

public class FrequencyColumnTransformIteratorTest {
    
    static InMemoryInstance instance;
    private static Connector connector;
    static BatchWriterConfig bwConfig = new BatchWriterConfig().setMaxLatency(1, TimeUnit.SECONDS).setMaxMemory(1000L).setMaxWriteThreads(1);
    private static long timestamp;
    static String METADATA_TABLE_NAME = "metadata";
    
    private Authorizations auths = new Authorizations("STUFF,THINGS");
    
    @BeforeClass
    public static void setUp() throws Exception {
        instance = new InMemoryInstance();
        connector = instance.getConnector("root", new PasswordToken(""));
    }
    
    @Before
    public void setup() throws Exception {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        Date date = format.parse("20190101");
        timestamp = date.getTime();
        NewTableConfiguration newTableConfiguration = new NewTableConfiguration();
        HashMap<String,String> properties = new HashMap<>();
        properties.put("table.compaction.minor.idle", "5s");
        newTableConfiguration.setProperties(properties);
        connector.tableOperations().create(METADATA_TABLE_NAME, newTableConfiguration);
        HashMap<String,String> propertiesIt = new HashMap<>();
        propertiesIt.put("ageOffDate", "20100101");
        propertiesIt.put("type", "VARLEN");
        IteratorSetting settings = new IteratorSetting(19, FrequencyColumnIterator.class, propertiesIt);
        EnumSet<IteratorUtil.IteratorScope> scopes = EnumSet.allOf(IteratorUtil.IteratorScope.class);
        connector.tableOperations().attachIterator(METADATA_TABLE_NAME, settings, scopes);
    }
    
    @After
    public void cleanUp() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        connector.tableOperations().delete(METADATA_TABLE_NAME);
    }
    
    private void loadData() throws Exception {
        Text colqPrefix = new Text("csv\u0000");
        
        // metadata
        BatchWriter bw = connector.createBatchWriter(METADATA_TABLE_NAME, bwConfig);
        
        Mutation m = new Mutation("BAR_FIELD");
        m.put(new Text("f"), new Text(colqPrefix + "20090426"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(1L)));
        m.put(new Text("f"), new Text(colqPrefix + "20090426"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(1L)));
        m.put(new Text("f"), new Text(colqPrefix + "20090426"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(0x10L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160426"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(1L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160426"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(1L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160426"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(0x10L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160427"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(1L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160427"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(2L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160427"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(0x10L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160428"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(1L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160428"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(3L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160428"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(0x10L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160429"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(1L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160429"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(4L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160429"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(0x10L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160501"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(1L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160501"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(5L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160501"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(0x10L)));
        
        bw.addMutation(m);
        
        m = new Mutation("NAME_FIELD");
        m.put(new Text("f"), new Text(colqPrefix + "20090526"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(2L)));
        m.put(new Text("f"), new Text(colqPrefix + "20090526"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(2L)));
        m.put(new Text("f"), new Text(colqPrefix + "20090526"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(0x20L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160526"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(2L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160526"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(2L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160526"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(0x20L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160527"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(2L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160527"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(3L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160527"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(0x20L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160528"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(2L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160528"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(4L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160528"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(0x20L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160529"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(2L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160529"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(5L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160529"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(0x20L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160601"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(2L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160601"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(6L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160601"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(0x20L)));
        bw.addMutation(m);
        
        m = new Mutation("PUB_FIELD");
        m.put(new Text("f"), new Text(colqPrefix + "20090726"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(3L)));
        m.put(new Text("f"), new Text(colqPrefix + "20090726"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(3L)));
        m.put(new Text("f"), new Text(colqPrefix + "20090726"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(0x30L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160726"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(3L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160726"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(3L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160726"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(0x30L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160727"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(3L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160727"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(4L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160727"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(0x30L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160728"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(3L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160728"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(5L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160728"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(0x30L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160729"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(3L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160729"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(6L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160729"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(0x30L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160801"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(3L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160801"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(7L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160801"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(0x30L)));
        bw.addMutation(m);
        bw.close();
        
        colqPrefix = new Text("json\u0000");
        
        m = new Mutation("AGE_FIELD");
        m.put(new Text("f"), new Text(colqPrefix + "20090826"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(4L)));
        m.put(new Text("f"), new Text(colqPrefix + "20090826"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(4L)));
        m.put(new Text("f"), new Text(colqPrefix + "20090826"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(0x40L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160826"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(4L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160826"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(4L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160826"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(0x40L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160827"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(4L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160827"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(5L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160827"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(0x40L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160828"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(4L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160828"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(6L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160828"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(0x40L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160829"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(4L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160829"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(7L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160829"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(0x40L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160901"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(4L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160901"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(8L)));
        m.put(new Text("f"), new Text(colqPrefix + "20160901"), nextTimeStamp(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(0x40L)));
        bw.addMutation(m);
        bw.close();
    }
    
    private long nextTimeStamp() {
        return timestamp++;
    }
    
    @Test
    public void testFrequencyTransformIterator() throws Throwable {
        
        loadData();
        
        Scanner scanner = connector.createScanner(METADATA_TABLE_NAME, auths);
        scanner.setBatchSize(200);
        scanner.fetchColumnFamily(new Text(ColumnFamilyConstants.COLF_F));
        int numEntries = 0;
        HashMap<String,FrequencyFamilyCounter> counterHashMap = new HashMap<>();
        
        for (Map.Entry<Key,Value> entry : scanner) {
            Assert.assertTrue(MetadataHelper.isAggregatedFreqKey(entry.getKey()));
            FrequencyFamilyCounter counter = new FrequencyFamilyCounter();
            counter.deserializeCompressedValue(entry.getValue());
            TreeMap<YearMonthDay,Frequency> dateFreqMap = counter.getDateToFrequencyValueMap();
            // for (Map.Entry<YearMonthDay,Frequency> entry2 : dateFreqMap.entrySet()) {
            // System.out.println("key: " + entry.getKey() + " date: " + entry2.getKey() + " frequency: " + entry2.getValue());
            // }
            counterHashMap.put(entry.getKey().getRow().toString(), counter);
            numEntries++;
        }
        
        checkFrequencyCompressedData(numEntries, counterHashMap);
        
    }
    
    @Test
    public void testFrequencyTransformIteratorAgeOff() throws Throwable {
        
        loadData();
        
        Scanner scanner = connector.createScanner(METADATA_TABLE_NAME, auths);
        scanner.setBatchSize(200);
        scanner.fetchColumnFamily(new Text(ColumnFamilyConstants.COLF_F));
        int numEntries = 0;
        HashMap<String,FrequencyFamilyCounter> counterHashMap = new HashMap<>();
        
        for (Map.Entry<Key,Value> entry : scanner) {
            Assert.assertTrue(MetadataHelper.isAggregatedFreqKey(entry.getKey()));
            FrequencyFamilyCounter counter = new FrequencyFamilyCounter();
            counter.deserializeCompressedValue(entry.getValue());
            TreeMap<YearMonthDay,Frequency> dateFreqMap = counter.getDateToFrequencyValueMap();
            // for (Map.Entry<YearMonthDay,Frequency> entry2 : dateFreqMap.entrySet()) {
            // System.out.println("key: " + entry.getKey() + " date: " + entry2.getKey() + " frequency: " + entry2.getValue());
            // }
            counterHashMap.put(entry.getKey().getRow().toString(), counter);
            numEntries++;
        }
        
        checkFrequencyCompressedDataForAgeOff(numEntries, counterHashMap);
        
    }
    
    private void checkFrequencyCompressedData(int numEntries, HashMap<String,FrequencyFamilyCounter> counterHashMap) {
        // Also verifies AgeOff
        Assert.assertEquals(4, numEntries);
        Assert.assertEquals(null, counterHashMap.get("BAR_FIELD").getDateToFrequencyValueMap().get(new YearMonthDay("20090426")));
        Assert.assertEquals(18, counterHashMap.get("BAR_FIELD").getDateToFrequencyValueMap().get(new YearMonthDay("20160426")).getValue());
        Assert.assertEquals(19, counterHashMap.get("BAR_FIELD").getDateToFrequencyValueMap().get(new YearMonthDay("20160427")).getValue());
        Assert.assertEquals(20, counterHashMap.get("BAR_FIELD").getDateToFrequencyValueMap().get(new YearMonthDay("20160428")).getValue());
        Assert.assertEquals(21, counterHashMap.get("BAR_FIELD").getDateToFrequencyValueMap().get(new YearMonthDay("20160429")).getValue());
        Assert.assertEquals(22, counterHashMap.get("BAR_FIELD").getDateToFrequencyValueMap().get(new YearMonthDay("20160501")).getValue());
        
        Assert.assertEquals(null, counterHashMap.get("NAME_FIELD").getDateToFrequencyValueMap().get(new YearMonthDay("20090526")));
        Assert.assertEquals(36, counterHashMap.get("NAME_FIELD").getDateToFrequencyValueMap().get(new YearMonthDay("20160526")).getValue());
        Assert.assertEquals(37, counterHashMap.get("NAME_FIELD").getDateToFrequencyValueMap().get(new YearMonthDay("20160527")).getValue());
        Assert.assertEquals(38, counterHashMap.get("NAME_FIELD").getDateToFrequencyValueMap().get(new YearMonthDay("20160528")).getValue());
        Assert.assertEquals(39, counterHashMap.get("NAME_FIELD").getDateToFrequencyValueMap().get(new YearMonthDay("20160529")).getValue());
        Assert.assertEquals(40, counterHashMap.get("NAME_FIELD").getDateToFrequencyValueMap().get(new YearMonthDay("20160601")).getValue());
        
        Assert.assertEquals(null, counterHashMap.get("PUB_FIELD").getDateToFrequencyValueMap().get(new YearMonthDay("20090726")));
        Assert.assertEquals(54, counterHashMap.get("PUB_FIELD").getDateToFrequencyValueMap().get(new YearMonthDay("20160726")).getValue());
        Assert.assertEquals(55, counterHashMap.get("PUB_FIELD").getDateToFrequencyValueMap().get(new YearMonthDay("20160727")).getValue());
        Assert.assertEquals(56, counterHashMap.get("PUB_FIELD").getDateToFrequencyValueMap().get(new YearMonthDay("20160728")).getValue());
        Assert.assertEquals(57, counterHashMap.get("PUB_FIELD").getDateToFrequencyValueMap().get(new YearMonthDay("20160729")).getValue());
        Assert.assertEquals(58, counterHashMap.get("PUB_FIELD").getDateToFrequencyValueMap().get(new YearMonthDay("20160801")).getValue());
        
        Assert.assertEquals(null, counterHashMap.get("AGE_FIELD").getDateToFrequencyValueMap().get(new YearMonthDay("20090826")));
        Assert.assertEquals(72, counterHashMap.get("AGE_FIELD").getDateToFrequencyValueMap().get(new YearMonthDay("20160826")).getValue());
        Assert.assertEquals(73, counterHashMap.get("AGE_FIELD").getDateToFrequencyValueMap().get(new YearMonthDay("20160827")).getValue());
        Assert.assertEquals(74, counterHashMap.get("AGE_FIELD").getDateToFrequencyValueMap().get(new YearMonthDay("20160828")).getValue());
        Assert.assertEquals(75, counterHashMap.get("AGE_FIELD").getDateToFrequencyValueMap().get(new YearMonthDay("20160829")).getValue());
        Assert.assertEquals(76, counterHashMap.get("AGE_FIELD").getDateToFrequencyValueMap().get(new YearMonthDay("20160901")).getValue());
    }
    
    private void checkFrequencyCompressedDataForAgeOff(int numEntries, HashMap<String,FrequencyFamilyCounter> counterHashMap) {
        Assert.assertEquals(4, numEntries);
        Assert.assertEquals(new YearMonthDay("20160426"), counterHashMap.get("BAR_FIELD").getDateToFrequencyValueMap().firstKey());
        Assert.assertEquals(new YearMonthDay("20160526"), counterHashMap.get("NAME_FIELD").getDateToFrequencyValueMap().firstKey());
        Assert.assertEquals(new YearMonthDay("20160726"), counterHashMap.get("PUB_FIELD").getDateToFrequencyValueMap().firstKey());
        Assert.assertEquals(new YearMonthDay("20160826"), counterHashMap.get("AGE_FIELD").getDateToFrequencyValueMap().firstKey());
        
    }
    
    private static AllFieldMetadataHelper createAllFieldMetadataHelper(Connector connector) {
        final Set<Authorizations> allMetadataAuths = Collections.emptySet();
        final Set<Authorizations> auths = Collections.emptySet();
        TypeMetadataHelper tmh = new TypeMetadataHelper(Maps.newHashMap(), allMetadataAuths, connector, METADATA_TABLE_NAME, auths, false);
        CompositeMetadataHelper cmh = new CompositeMetadataHelper(connector, METADATA_TABLE_NAME, auths);
        return new AllFieldMetadataHelper(tmh, cmh, connector, METADATA_TABLE_NAME, auths, allMetadataAuths);
        
    }
    
    @Test
    public void testMetadataHelper() throws Exception {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        loadData();
        HashSet<Authorizations> authorizations = new HashSet<>();
        authorizations.add(auths);
        MetadataHelper metadataHelper = new MetadataHelper(createAllFieldMetadataHelper(connector), Collections.emptySet(), connector, METADATA_TABLE_NAME,
                        authorizations, authorizations);
        HashSet<String> dataTypes = new HashSet<>();
        dataTypes.add("csv");
        long count = metadataHelper.getCountsByFieldInDayWithTypes("BAR_FIELD", "20160426", dataTypes);
        Assert.assertEquals(18, count);
        count = metadataHelper.getCountsByFieldInDayWithTypes("BAR_FIELD", "20160427", dataTypes);
        Assert.assertEquals(19, count);
        count = metadataHelper.getCountsByFieldInDayWithTypes("BAR_FIELD", "20160428", dataTypes);
        Assert.assertEquals(20, count);
        count = metadataHelper.getCountsByFieldInDayWithTypes("BAR_FIELD", "20160429", dataTypes);
        Assert.assertEquals(21, count);
        count = metadataHelper.getCountsByFieldInDayWithTypes("BAR_FIELD", "20160501", dataTypes);
        Assert.assertEquals(22, count);
        
        count = metadataHelper.getCountsByFieldInDayWithTypes("NAME_FIELD", "20160526", dataTypes);
        Assert.assertEquals(36, count);
        count = metadataHelper.getCountsByFieldInDayWithTypes("NAME_FIELD", "20160527", dataTypes);
        Assert.assertEquals(37, count);
        count = metadataHelper.getCountsByFieldInDayWithTypes("NAME_FIELD", "20160528", dataTypes);
        Assert.assertEquals(38, count);
        count = metadataHelper.getCountsByFieldInDayWithTypes("NAME_FIELD", "20160529", dataTypes);
        Assert.assertEquals(39, count);
        count = metadataHelper.getCountsByFieldInDayWithTypes("NAME_FIELD", "20160601", dataTypes);
        Assert.assertEquals(40, count);
        
        count = metadataHelper.getCountsByFieldInDayWithTypes("PUB_FIELD", "20160726", dataTypes);
        Assert.assertEquals(54, count);
        count = metadataHelper.getCountsByFieldInDayWithTypes("PUB_FIELD", "20160727", dataTypes);
        Assert.assertEquals(55, count);
        count = metadataHelper.getCountsByFieldInDayWithTypes("PUB_FIELD", "20160728", dataTypes);
        Assert.assertEquals(56, count);
        count = metadataHelper.getCountsByFieldInDayWithTypes("PUB_FIELD", "20160729", dataTypes);
        Assert.assertEquals(57, count);
        count = metadataHelper.getCountsByFieldInDayWithTypes("PUB_FIELD", "20160801", dataTypes);
        Assert.assertEquals(58, count);
        
        count = metadataHelper.getCardinalityForField("PUB_FIELD", "csv", formatter.parse("20160725"), formatter.parse("20160802"));
        Assert.assertEquals(280, count);
        count = metadataHelper.getCardinalityForField("PUB_FIELD", "json", formatter.parse("20160725"), formatter.parse("20160802"));
        Assert.assertEquals(0, count);
        count = metadataHelper.getCardinalityForField("AGE_FIELD", "json", formatter.parse("20160725"), formatter.parse("20160902"));
        Assert.assertEquals(370, count);
    }
    
}
