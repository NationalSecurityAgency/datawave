package datawave.iterators;

import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.inject.internal.cglib.core.$ClassNameReader;
import com.google.inject.internal.util.$SourceProvider;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.ColumnFamilyConstants;
import datawave.ingest.protobuf.Uid;
import datawave.marking.MarkingFunctions;
import datawave.query.util.FrequencyFamilyCounter;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MetadataHelperFactory;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnUtils;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.junit.*;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class FrequencyColumnTransformIteratorTest {
    
    static InMemoryInstance instance;
    private static Connector connector;
    static BatchWriterConfig bwConfig = new BatchWriterConfig().setMaxLatency(1, TimeUnit.SECONDS).setMaxMemory(1000L).setMaxWriteThreads(1);
    private static long timestamp;
    static String METADATA_TABLE_NAME = "metadata";
    
    private DatawavePrincipal datawavePrincipal;
    private Authorizations auths = new Authorizations("STUFF,THINGS");
    
    private static final Value NULL_VALUE = new Value(new byte[0]);
    
    private Path temporaryFolder;
    
    @BeforeClass
    public static void setUp() throws Exception {
        instance = new InMemoryInstance();
        connector = instance.getConnector("root", new PasswordToken(""));
        
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        Date date = format.parse("20190101");
        timestamp = date.getTime();
        NewTableConfiguration newTableConfiguration = new NewTableConfiguration();
        HashMap<String,String> properties = new HashMap<>();
        properties.put("table.iterator.majc.vers.opt.maxVersions", "10");
        properties.put("table.iterator.minc.vers.opt.maxVersions", "10");
        properties.put("table.iterator.scan.vers.opt.maxVersions", "10");
        newTableConfiguration.setProperties(properties);
        connector.tableOperations().create(METADATA_TABLE_NAME, newTableConfiguration);
        IteratorSetting settings = new IteratorSetting(19, FrequencyColumnIterator.class, properties);
        EnumSet<IteratorUtil.IteratorScope> scopes = EnumSet.allOf(IteratorUtil.IteratorScope.class);
        scopes.remove(IteratorUtil.IteratorScope.minc);
        connector.tableOperations().attachIterator(METADATA_TABLE_NAME, settings, scopes);
        
    }
    
    @Before
    public void setup() throws Exception {
        //TODO would like to set up and tear down data before each test
    }
    
    @After
    public void cleanUp() {
        //TODO - might need to implement this later.
    }
    
    private void loadData() throws Exception {
        Text colqPrefix = new Text("csv\u0000");
        
        // metadata
        BatchWriter bw = connector.createBatchWriter(METADATA_TABLE_NAME, bwConfig);
        
        Mutation m = new Mutation("BAR_FIELD");
        m.put(new Text("f"), new Text(colqPrefix + "20160426"), nextTimeStamp(), new Value("1"));
        m.put(new Text("f"), new Text(colqPrefix + "20160426"), nextTimeStamp(), new Value("1"));
        m.put(new Text("f"), new Text(colqPrefix + "20160426"), nextTimeStamp(), new Value("0x10"));
        m.put(new Text("f"), new Text(colqPrefix + "20160427"), nextTimeStamp(), new Value("1"));
        m.put(new Text("f"), new Text(colqPrefix + "20160427"), nextTimeStamp(), new Value("2"));
        m.put(new Text("f"), new Text(colqPrefix + "20160427"), nextTimeStamp(), new Value("0x10"));
        m.put(new Text("f"), new Text(colqPrefix + "20160428"), nextTimeStamp(), new Value("1"));
        m.put(new Text("f"), new Text(colqPrefix + "20160428"), nextTimeStamp(), new Value("3"));
        m.put(new Text("f"), new Text(colqPrefix + "20160428"), nextTimeStamp(), new Value("0x10"));
        m.put(new Text("f"), new Text(colqPrefix + "20160429"), nextTimeStamp(), new Value("1"));
        m.put(new Text("f"), new Text(colqPrefix + "20160429"), nextTimeStamp(), new Value("4"));
        m.put(new Text("f"), new Text(colqPrefix + "20160429"), nextTimeStamp(), new Value("0x10"));
        m.put(new Text("f"), new Text(colqPrefix + "20160501"), nextTimeStamp(), new Value("1"));
        m.put(new Text("f"), new Text(colqPrefix + "20160501"), nextTimeStamp(), new Value("5"));
        m.put(new Text("f"), new Text(colqPrefix + "20160501"), nextTimeStamp(), new Value("0x10"));
        
        bw.addMutation(m);
        
        m = new Mutation("NAME_FIELD");
        m.put(new Text("f"), new Text(colqPrefix + "20160526"), nextTimeStamp(), new Value("2"));
        m.put(new Text("f"), new Text(colqPrefix + "20160526"), nextTimeStamp(), new Value("2"));
        m.put(new Text("f"), new Text(colqPrefix + "20160526"), nextTimeStamp(), new Value("0x20"));
        m.put(new Text("f"), new Text(colqPrefix + "20160527"), nextTimeStamp(), new Value("2"));
        m.put(new Text("f"), new Text(colqPrefix + "20160527"), nextTimeStamp(), new Value("3"));
        m.put(new Text("f"), new Text(colqPrefix + "20160527"), nextTimeStamp(), new Value("0x20"));
        m.put(new Text("f"), new Text(colqPrefix + "20160528"), nextTimeStamp(), new Value("2"));
        m.put(new Text("f"), new Text(colqPrefix + "20160528"), nextTimeStamp(), new Value("4"));
        m.put(new Text("f"), new Text(colqPrefix + "20160528"), nextTimeStamp(), new Value("0x20"));
        m.put(new Text("f"), new Text(colqPrefix + "20160529"), nextTimeStamp(), new Value("2"));
        m.put(new Text("f"), new Text(colqPrefix + "20160529"), nextTimeStamp(), new Value("5"));
        m.put(new Text("f"), new Text(colqPrefix + "20160529"), nextTimeStamp(), new Value("0x20"));
        m.put(new Text("f"), new Text(colqPrefix + "20160601"), nextTimeStamp(), new Value("2"));
        m.put(new Text("f"), new Text(colqPrefix + "20160601"), nextTimeStamp(), new Value("6"));
        m.put(new Text("f"), new Text(colqPrefix + "20160601"), nextTimeStamp(), new Value("0x20"));
        bw.addMutation(m);
        
        m = new Mutation("PUB_FIELD");
        m.put(new Text("f"), new Text(colqPrefix + "20160726"), nextTimeStamp(), new Value("3"));
        m.put(new Text("f"), new Text(colqPrefix + "20160726"), nextTimeStamp(), new Value("3"));
        m.put(new Text("f"), new Text(colqPrefix + "20160726"), nextTimeStamp(), new Value("0x30"));
        m.put(new Text("f"), new Text(colqPrefix + "20160727"), nextTimeStamp(), new Value("3"));
        m.put(new Text("f"), new Text(colqPrefix + "20160727"), nextTimeStamp(), new Value("4"));
        m.put(new Text("f"), new Text(colqPrefix + "20160727"), nextTimeStamp(), new Value("0x30"));
        m.put(new Text("f"), new Text(colqPrefix + "20160728"), nextTimeStamp(), new Value("3"));
        m.put(new Text("f"), new Text(colqPrefix + "20160728"), nextTimeStamp(), new Value("5"));
        m.put(new Text("f"), new Text(colqPrefix + "20160728"), nextTimeStamp(), new Value("0x30"));
        m.put(new Text("f"), new Text(colqPrefix + "20160729"), nextTimeStamp(), new Value("3"));
        m.put(new Text("f"), new Text(colqPrefix + "20160729"), nextTimeStamp(), new Value("6"));
        m.put(new Text("f"), new Text(colqPrefix + "20160729"), nextTimeStamp(), new Value("0x30"));
        m.put(new Text("f"), new Text(colqPrefix + "20160801"), nextTimeStamp(), new Value("3"));
        m.put(new Text("f"), new Text(colqPrefix + "20160801"), nextTimeStamp(), new Value("7"));
        m.put(new Text("f"), new Text(colqPrefix + "20160801"), nextTimeStamp(), new Value("0x30"));
        bw.addMutation(m);
        bw.close();
        
    }
    
    private long nextTimeStamp() {
        return timestamp++;
    }
    
    @Test
    public void testFrequencyTransformIterator() throws Throwable {
        // TODO scan the metadata table and looked for compressed keys and inspect the values.
        
        loadData();
        
        Scanner scanner = connector.createScanner(METADATA_TABLE_NAME, auths);
        scanner.setBatchSize(200);
        scanner.fetchColumnFamily(new Text(ColumnFamilyConstants.COLF_F));
        int numEntries = 0;
        HashMap<String,FrequencyFamilyCounter> counterHashMap = new HashMap<>();
        
        for (Map.Entry<Key,Value> entry : scanner) {
            Assert.assertTrue(entry.getKey().getColumnQualifier().toString().startsWith(MetadataHelper.COL_QUAL_PREFIX));
            FrequencyFamilyCounter counter = new FrequencyFamilyCounter();
            counter.deserializeCompressedValue(entry.getValue());
            HashMap<String,Integer> dateFreqMap = counter.getDateToFrequencyValueMap();
            for (Map.Entry<String,Integer> entry2 : dateFreqMap.entrySet()) {
                System.out.println("Date: " + entry2.getKey() + " frequency: " + entry2.getValue());
            }
            counterHashMap.put(entry.getKey().getRow().toString(), counter);
            numEntries++;
        }
        
        checkFrequencyCompressedData(numEntries, counterHashMap);
        
        compactMetadataTable();
        
        scanner = connector.createScanner(METADATA_TABLE_NAME, auths);
        scanner.setBatchSize(200);
        scanner.fetchColumnFamily(new Text(ColumnFamilyConstants.COLF_F));
        numEntries = 0;
        counterHashMap = new HashMap<>();
        
        for (Map.Entry<Key,Value> entry : scanner) {
            Assert.assertTrue(entry.getKey().getColumnQualifier().toString().startsWith(MetadataHelper.COL_QUAL_PREFIX));
            FrequencyFamilyCounter counter = new FrequencyFamilyCounter();
            counter.deserializeCompressedValue(entry.getValue());
            HashMap<String,Integer> dateFreqMap = counter.getDateToFrequencyValueMap();
            for (Map.Entry<String,Integer> entry2 : dateFreqMap.entrySet()) {
                System.out.println("Date: " + entry2.getKey() + " frequency: " + entry2.getValue());
            }
            counterHashMap.put(entry.getKey().getRow().toString(), counter);
            numEntries++;
        }
        
        checkFrequencyCompressedData(numEntries, counterHashMap);
        
    }
    
    private void compactMetadataTable() throws AccumuloSecurityException, TableNotFoundException, AccumuloException {
        
        connector.tableOperations().compact(METADATA_TABLE_NAME, new Text("A"), new Text("Z"), true, true);
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Assert.fail("did not sleep for compaction");
        }

        // Test for indempotency
        connector.tableOperations().compact(METADATA_TABLE_NAME, new Text("A"), new Text("Z"), true, true);
    }
    
    private void checkFrequencyCompressedData(int numEntries, HashMap<String,FrequencyFamilyCounter> counterHashMap) {
        Assert.assertTrue(numEntries == 3);
        Assert.assertTrue(counterHashMap.get("BAR_FIELD").getDateToFrequencyValueMap().get("20160426").equals(18));
        Assert.assertTrue(counterHashMap.get("BAR_FIELD").getDateToFrequencyValueMap().get("20160427").equals(19));
        Assert.assertTrue(counterHashMap.get("BAR_FIELD").getDateToFrequencyValueMap().get("20160428").equals(20));
        Assert.assertTrue(counterHashMap.get("BAR_FIELD").getDateToFrequencyValueMap().get("20160429").equals(21));
        Assert.assertTrue(counterHashMap.get("BAR_FIELD").getDateToFrequencyValueMap().get("20160501").equals(22));
        Assert.assertTrue(counterHashMap.get("NAME_FIELD").getDateToFrequencyValueMap().get("20160526").equals(36));
        Assert.assertTrue(counterHashMap.get("NAME_FIELD").getDateToFrequencyValueMap().get("20160527").equals(37));
        Assert.assertTrue(counterHashMap.get("NAME_FIELD").getDateToFrequencyValueMap().get("20160528").equals(38));
        Assert.assertTrue(counterHashMap.get("NAME_FIELD").getDateToFrequencyValueMap().get("20160529").equals(39));
        Assert.assertTrue(counterHashMap.get("NAME_FIELD").getDateToFrequencyValueMap().get("20160601").equals(40));
        Assert.assertTrue(counterHashMap.get("PUB_FIELD").getDateToFrequencyValueMap().get("20160726").equals(54));
        Assert.assertTrue(counterHashMap.get("PUB_FIELD").getDateToFrequencyValueMap().get("20160727").equals(55));
        Assert.assertTrue(counterHashMap.get("PUB_FIELD").getDateToFrequencyValueMap().get("20160728").equals(56));
        Assert.assertTrue(counterHashMap.get("PUB_FIELD").getDateToFrequencyValueMap().get("20160729").equals(57));
        Assert.assertTrue(counterHashMap.get("PUB_FIELD").getDateToFrequencyValueMap().get("20160801").equals(58));
    }
    
}
