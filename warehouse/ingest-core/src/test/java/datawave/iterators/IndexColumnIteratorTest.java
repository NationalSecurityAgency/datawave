package datawave.iterators;

import com.google.common.collect.Maps;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.ColumnFamilyConstants;
import datawave.query.composite.CompositeMetadataHelper;
import datawave.query.util.*;
import datawave.util.time.DateHelper;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.*;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class IndexColumnIteratorTest {
    
    static InMemoryInstance instance;
    private static Connector connector;
    static BatchWriterConfig bwConfig = new BatchWriterConfig().setMaxLatency(1, TimeUnit.SECONDS).setMaxMemory(1000L).setMaxWriteThreads(1);
    private static long timestamp;
    static String METADATA_TABLE_NAME = "metadata";
    
    private Authorizations auths = new Authorizations("STUFF,THINGS");
    private static final String DATA_TYPE = "TheDataType";
    Text colqPrefix = new Text(DATA_TYPE + "\u0000TheNormalizer");
    
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
        properties.put("table.iterator.majc.vers.opt.maxVersions", "10");
        properties.put("table.iterator.minc.vers.opt.maxVersions", "10");
        properties.put("table.iterator.scan.vers.opt.maxVersions", "10");
        properties.put("table.compaction.minor.idle", "10s");
        newTableConfiguration.setProperties(properties);
        connector.tableOperations().create(METADATA_TABLE_NAME, newTableConfiguration);
        HashMap<String,String> propertiesIt = new HashMap<>();
        propertiesIt.put("ageOffDate", "20100101");
        propertiesIt.put("type", "VARLEN");
        IteratorSetting settings = new IteratorSetting(19, IndexColumnIterator.class, propertiesIt);
        EnumSet<IteratorUtil.IteratorScope> scopes = EnumSet.allOf(IteratorUtil.IteratorScope.class);
        connector.tableOperations().attachIterator(METADATA_TABLE_NAME, settings, scopes);
    }
    
    @After
    public void cleanUp() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        connector.tableOperations().delete(METADATA_TABLE_NAME);
    }
    
    private Value getNewValue(String yyyymmdd) {
        
        IndexedDatesValue indexedDatesValue = new IndexedDatesValue(new YearMonthDay(yyyymmdd));
        return indexedDatesValue.serialize();
    }
    
    private void loadData() throws Exception {
        
        // metadata
        BatchWriter bw = connector.createBatchWriter(METADATA_TABLE_NAME, bwConfig);
        Mutation m = new Mutation("BAR_FIELD");
        putMutatation(m, "20190101");
        putMutatation(m, "20190102");
        putMutatation(m, "20190103");
        putMutatation(m, "20190201");
        putMutatation(m, "20190202");
        putMutatation(m, "20190203");
        putMutatation(m, "20190301");
        putMutatation(m, "20190302");
        putMutatation(m, "20190303");
        putMutatation(m, "20190401");
        putMutatation(m, "20190402");
        putMutatation(m, "20190403");
        putMutatation(m, "20190404");
        putMutatation(m, "20190405");
        putMutatation(m, "20190501");
        putMutatation(m, "20190502");
        putMutatation(m, "20190503");
        putMutatation(m, "20190504");
        
        bw.addMutation(m);
        
        m = new Mutation("NAME_FIELD");
        putMutatation(m, "20190101");
        putMutatation(m, "20190102");
        putMutatation(m, "20190103");
        putMutatation(m, "20190201");
        putMutatation(m, "20190202");
        putMutatation(m, "20190203");
        putMutatation(m, "20190301");
        putMutatation(m, "20190302");
        putMutatation(m, "20190303");
        putMutatation(m, "20190401");
        putMutatation(m, "20190402");
        putMutatation(m, "20190403");
        putMutatation(m, "20190404");
        putMutatation(m, "20190405");
        putMutatation(m, "20190501");
        putMutatation(m, "20190502");
        putMutatation(m, "20190503");
        putMutatation(m, "20190504");
        bw.addMutation(m);
        
        m = new Mutation("PUB_FIELD");
        putMutatation(m, "20190101");
        putMutatation(m, "20190102");
        putMutatation(m, "20190103");
        putMutatation(m, "20190201");
        putMutatation(m, "20190202");
        putMutatation(m, "20190203");
        putMutatation(m, "20190301");
        putMutatation(m, "20190302");
        putMutatation(m, "20190303");
        putMutatation(m, "20190401");
        putMutatation(m, "20190402");
        putMutatation(m, "20190403");
        putMutatation(m, "20190404");
        putMutatation(m, "20190405");
        putMutatation(m, "20190501");
        putMutatation(m, "20190502");
        putMutatation(m, "20190503");
        putMutatation(m, "20190504");
        bw.addMutation(m);
        bw.close();
        
    }
    
    private void putMutatation(Mutation m, String s) {
        m.put(new Text("i"), colqPrefix, nextTimeStamp(), getNewValue(s));
    }
    
    private long nextTimeStamp() {
        return timestamp++;
    }
    
    @Test
    public void testFrequencyTransformIteratorAtScanScope() throws Throwable {
        
        loadData();
        
        Scanner scanner = connector.createScanner(METADATA_TABLE_NAME, auths);
        scanner.setBatchSize(200);
        scanner.fetchColumnFamily(new Text(ColumnFamilyConstants.COLF_I));
        int numEntries = 0;
        HashMap<String,IndexedDatesValue> counterHashMap = new HashMap<>();
        
        numEntries = getNumEntries(scanner, numEntries, counterHashMap);
        
        checkFrequencyCompressedData(numEntries, counterHashMap);
        
    }
    
    private int getNumEntries(Scanner scanner, int numEntries, HashMap<String,IndexedDatesValue> counterHashMap) {
        for (Map.Entry<Key,Value> entry : scanner) {
            Assert.assertTrue(entry.getKey().getColumnQualifier().toString().startsWith(colqPrefix.toString()));
            IndexedDatesValue indexedDates = IndexedDatesValue.deserialize(entry.getValue());
            TreeSet<YearMonthDay> dateIndexSet = indexedDates.getIndexedDatesSet();
            for (YearMonthDay entry2 : dateIndexSet) {
                System.out.println("Indexed Date: " + entry2);
            }
            counterHashMap.put(entry.getKey().getRow().toString(), indexedDates);
            numEntries++;
        }
        return numEntries;
    }
    
    @Test
    public void testFrequencyTransformIteratorAtMincScope() throws Throwable {
        // TODO I have verified minimum compaction in the Accumlo Shell - I am sceptical that this test really
        // tests minimum compaction although it should work.
        loadData();
        // Sleep long enough to perform a minimum compaction.
        Thread.sleep(15000);
        Scanner scanner = connector.createScanner(METADATA_TABLE_NAME, auths);
        scanner.setBatchSize(200);
        scanner.fetchColumnFamily(new Text(ColumnFamilyConstants.COLF_I));
        int numEntries = 0;
        HashMap<String,IndexedDatesValue> counterHashMap = new HashMap<>();
        
        numEntries = getNumEntries(scanner, numEntries, counterHashMap);
        
        checkFrequencyCompressedData(numEntries, counterHashMap);
        
    }
    
    @Test
    public void testFrequencyTransformIteratorAgeOff() throws Throwable {
        
        loadData();
        // Sleep long enough to perform a minimum compaction.
        Thread.sleep(15000);
        Scanner scanner = connector.createScanner(METADATA_TABLE_NAME, auths);
        scanner.setBatchSize(200);
        scanner.fetchColumnFamily(new Text(ColumnFamilyConstants.COLF_I));
        int numEntries = 0;
        HashMap<String,IndexedDatesValue> counterHashMap = new HashMap<>();
        
        numEntries = getNumEntries(scanner, numEntries, counterHashMap);
        
        checkFrequencyCompressedDataForAgeOff(numEntries, counterHashMap);
        
    }
    
    @Test
    public void testFrequencyTransformIteratorAtMajcScope() throws Throwable {
        
        loadData();
        
        Scanner scanner = connector.createScanner(METADATA_TABLE_NAME, auths);
        scanner.setBatchSize(200);
        scanner.fetchColumnFamily(new Text(ColumnFamilyConstants.COLF_I));
        int numEntries = 0;
        HashMap<String,IndexedDatesValue> counterHashMap = new HashMap<>();
        
        numEntries = getNumEntries(scanner, numEntries, counterHashMap);
        
        checkFrequencyCompressedData(numEntries, counterHashMap);
        
        compactMetadataTable();
        
        scanner = connector.createScanner(METADATA_TABLE_NAME, auths);
        scanner.setBatchSize(200);
        scanner.fetchColumnFamily(new Text(ColumnFamilyConstants.COLF_I));
        numEntries = 0;
        counterHashMap = new HashMap<>();
        
        numEntries = getNumEntries(scanner, numEntries, counterHashMap);
        
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
    
    private void checkFrequencyCompressedData(int numEntries, HashMap<String,IndexedDatesValue> counterHashMap) {
        // Also verifies AgeOff
        Assert.assertTrue(numEntries == 3);
        // Assert.assertTrue((counterHashMap.get("BAR_FIELD").getIndexedDatesSet().contains(new YearMonthDay("20090426"))));
        Assert.assertTrue((counterHashMap.get("BAR_FIELD").getIndexedDatesSet().contains(new YearMonthDay("20190101"))));
        Assert.assertTrue((counterHashMap.get("BAR_FIELD").getIndexedDatesSet().contains(new YearMonthDay("20190102"))));
        Assert.assertTrue((counterHashMap.get("BAR_FIELD").getIndexedDatesSet().contains(new YearMonthDay("20190103"))));
        Assert.assertTrue((counterHashMap.get("BAR_FIELD").getIndexedDatesSet().contains(new YearMonthDay("20190201"))));
        Assert.assertTrue((counterHashMap.get("BAR_FIELD").getIndexedDatesSet().contains(new YearMonthDay("20190201"))));
        Assert.assertTrue((counterHashMap.get("BAR_FIELD").getIndexedDatesSet().contains(new YearMonthDay("20190504"))));
        
        Assert.assertTrue((counterHashMap.get("NAME_FIELD").getIndexedDatesSet().contains(new YearMonthDay("20190301"))));
        Assert.assertTrue((counterHashMap.get("NAME_FIELD").getIndexedDatesSet().contains(new YearMonthDay("20190302"))));
        Assert.assertTrue((counterHashMap.get("NAME_FIELD").getIndexedDatesSet().contains(new YearMonthDay("20190303"))));
        Assert.assertTrue((counterHashMap.get("NAME_FIELD").getIndexedDatesSet().contains(new YearMonthDay("20190401"))));
        Assert.assertTrue((counterHashMap.get("NAME_FIELD").getIndexedDatesSet().contains(new YearMonthDay("20190401"))));
        Assert.assertTrue((counterHashMap.get("NAME_FIELD").getIndexedDatesSet().contains(new YearMonthDay("20190502"))));
        
        Assert.assertTrue((counterHashMap.get("PUB_FIELD").getIndexedDatesSet().contains(new YearMonthDay("20190401"))));
        Assert.assertTrue((counterHashMap.get("PUB_FIELD").getIndexedDatesSet().contains(new YearMonthDay("20190402"))));
        Assert.assertTrue((counterHashMap.get("PUB_FIELD").getIndexedDatesSet().contains(new YearMonthDay("20190403"))));
        Assert.assertTrue((counterHashMap.get("PUB_FIELD").getIndexedDatesSet().contains(new YearMonthDay("20190501"))));
        Assert.assertTrue((counterHashMap.get("PUB_FIELD").getIndexedDatesSet().contains(new YearMonthDay("20190502"))));
        Assert.assertTrue((counterHashMap.get("PUB_FIELD").getIndexedDatesSet().contains(new YearMonthDay("20190503"))));
        
    }
    
    private void checkFrequencyCompressedDataForAgeOff(int numEntries, HashMap<String,IndexedDatesValue> counterHashMap) {
        Assert.assertTrue(numEntries == 3);
        
        Assert.assertTrue(!(counterHashMap.get("BAR_FIELD").getIndexedDatesSet().contains(new YearMonthDay("20090426"))));
        Assert.assertTrue(!(counterHashMap.get("NAME_FIELD").getIndexedDatesSet().contains(new YearMonthDay("20090526"))));
        Assert.assertTrue(!(counterHashMap.get("PUB_FIELD").getIndexedDatesSet().contains(new YearMonthDay("20090726"))));
        
    }
    
    private static AllFieldMetadataHelper createAllFieldMetadataHelper(Connector connector) {
        final HashSet<Authorizations> allMetadataAuths = new HashSet<>();
        final HashSet<Authorizations> auths = new HashSet<>();
        final Authorizations helperauths = new Authorizations("STUFF,THINGS");
        auths.add(helperauths);
        allMetadataAuths.add(helperauths);
        TypeMetadataHelper tmh = new TypeMetadataHelper(Maps.newHashMap(), allMetadataAuths, connector, METADATA_TABLE_NAME, auths, false);
        CompositeMetadataHelper cmh = new CompositeMetadataHelper(connector, METADATA_TABLE_NAME, auths);
        return new AllFieldMetadataHelper(tmh, cmh, connector, METADATA_TABLE_NAME, auths, allMetadataAuths);
        
    }
    
    @Test
    public void testMetadataHelper() throws Exception {
        loadData();
        HashSet<Authorizations> authorizations = new HashSet<>();
        authorizations.add(auths);
        MetadataHelper metadataHelper = new MetadataHelper(createAllFieldMetadataHelper(connector), Collections.emptySet(), connector, METADATA_TABLE_NAME,
                        authorizations, authorizations);
        HashSet<String> dataTypes = new HashSet<>();
        dataTypes.add(DATA_TYPE);
        Set<String> indexedFields = metadataHelper.getIndexedFields(dataTypes);
        Assert.assertFalse(indexedFields.isEmpty());
        Set<String> ingestTypeFilter = new HashSet<>();
        Assert.assertTrue(metadataHelper.isIndexed("BAR_FIELD", ingestTypeFilter));
        Assert.assertTrue(metadataHelper.isIndexed("NAME_FIELD", ingestTypeFilter));
        Assert.assertTrue(metadataHelper.isIndexed("PUB_FIELD", ingestTypeFilter));
        
    }
    
}
