package datawave.query.jexl.lookups;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.getCurrentArgument;
import static org.easymock.EasyMock.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMockSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.iterator.SortedListKeyValueIterator;
import datawave.query.jexl.LiteralRange;
import datawave.query.scanner.LocalBatchScanner;
import datawave.query.tables.ScannerFactory;
import datawave.table.constants.TableName;
import datawave.util.time.DateHelper;

public class BoundedRangeIndexLookupTest extends EasyMockSupport {

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final String PASSWORD = "password";

    private static final String shard = "2024070";
    private static final Set<String> fields = Set.of("FIELD_A", "FIELD_B", "FIELD_C", "FIELD_D", "FIELD_E");
    private static final Set<String> datatypes = Set.of("datatype-a", "datatype-b", "datatype-c", "datatype-d", "datatype-e");

    private static MiniAccumuloCluster cluster;
    private static AccumuloClient client;
    private ExecutorService executorService;

    private ShardQueryConfiguration config;
    private ScannerFactory scannerFactory;

    private final SortedSet<String> expected = new TreeSet<>();

    private ScanMonitor monitor;

    @BeforeClass
    public static void setupClass() throws Exception {
        cluster = new MiniAccumuloCluster(temporaryFolder.newFolder(), PASSWORD);
        cluster.start();

        client = cluster.createAccumuloClient("root", new PasswordToken(PASSWORD));

        writeData();
    }

    @Before
    public void setup() {
        scannerFactory = new ScannerFactory(client);

        config = new ShardQueryConfiguration();
        config.setClient(client);

        executorService = Executors.newFixedThreadPool(5);

        expected.clear();

        monitor = ScanMonitor.of(25_000, "test", null);
    }

    @After
    public void teardown() {
        executorService.shutdownNow();
        monitor.close();
    }

    public static void writeData() throws Exception {
        client.tableOperations().create(TableName.SHARD_INDEX);

        int numTerms = 25;

        try (BatchWriter bw = client.createBatchWriter(TableName.SHARD_INDEX, new BatchWriterConfig())) {
            for (int i = 0; i < numTerms; i++) {
                Mutation m = new Mutation("value-" + i);
                for (String field : fields) {
                    for (int j = 0; j < 10; j++) {
                        for (String datatype : datatypes) {
                            for (int k = 0; k < 5; k++) {
                                m.put(field, shard + j + '_' + k + '\u0000' + datatype, new Value());
                            }
                        }
                    }
                }
                bw.addMutation(m);
            }
        }
    }

    @Test
    public void testSingleDay_singleValue() {
        withDateRange("20240701", "20240701");
        withDatatypeFilter(Set.of("datatype-b"));
        withExpected(Set.of("value-1"));
        BoundedRangeIndexLookup lookup = createLookup("FIELD_A", "value-1", "value-1");
        test(lookup, "FIELD_A");
    }

    @Test
    public void testSingleDay_multiValue() {
        withDateRange("20240701", "20240701");
        withExpected(Set.of("value-10", "value-12", "value-11", "value-14", "value-13", "value-16", "value-15", "value-18", "value-17", "value-19", "value-1",
                        "value-2"));
        BoundedRangeIndexLookup lookup = createLookup("FIELD_A", "value-1", "value-2");
        test(lookup, "FIELD_A");
    }

    @Test
    public void testSingleDay_allValues() {
        withDateRange("20240701", "20240701");
        withExpected(createAllValues(1, 25));
        BoundedRangeIndexLookup lookup = createLookup("FIELD_A", "value-1", "value-9");
        test(lookup, "FIELD_A");
    }

    @Test
    public void testMultiDay_singleValue() {
        withDateRange("20240701", "20240703");
        withExpected(Set.of("value-1"));
        BoundedRangeIndexLookup lookup = createLookup("FIELD_A", "value-1", "value-1");
        test(lookup, "FIELD_A");
    }

    @Test
    public void testMultiDay_multiValue() {
        withDateRange("20240701", "20240703");
        withExpected(Set.of("value-3", "value-4", "value-5"));
        BoundedRangeIndexLookup lookup = createLookup("FIELD_A", "value-3", "value-5");
        test(lookup, "FIELD_A");
    }

    @Test
    public void testMultiDay_allValues() {
        withDateRange("20240701", "20240703");
        withExpected(createAllValues(1, 25));
        BoundedRangeIndexLookup lookup = createLookup("FIELD_A", "value-1", "value-9");
        test(lookup, "FIELD_A");
    }

    @Test
    public void testAllDays_singleValue() {
        withDateRange("20240701", "20240709");
        withExpected(Set.of("value-1"));
        BoundedRangeIndexLookup lookup = createLookup("FIELD_A", "value-1", "value-1");
        test(lookup, "FIELD_A");
    }

    @Test
    public void testAllDays_multiValue() {
        withDateRange("20240701", "20240709");
        withExpected(Set.of("value-21", "value-3", "value-2", "value-20", "value-23", "value-22", "value-24"));
        BoundedRangeIndexLookup lookup = createLookup("FIELD_A", "value-2", "value-3");
        test(lookup, "FIELD_A");
    }

    @Test
    public void testAllDays_allValues() {
        withDateRange("20240701", "20240709");
        withExpected(createAllValues(1, 25));

        BoundedRangeIndexLookup lookup = createLookup("FIELD_A", "value-1", "value-9");
        test(lookup, "FIELD_A");
    }

    @Test
    public void testInvalidDateRange() {
        withDateRange("20240808", "20240909");
        BoundedRangeIndexLookup lookup = createLookup("FIELD_A", "value-1", "value-1");
        test(lookup, "FIELD_A");
    }

    @Test
    public void testInvalidBoundedRange() {
        withDateRange("20240701", "20240709");
        BoundedRangeIndexLookup lookup = createLookup("FIELD_A", "abc", "def");
        test(lookup, "FIELD_A");
    }

    @Test
    public void testInvalidField() {
        withDateRange("20240701", "20240709");
        BoundedRangeIndexLookup lookup = createLookup("FIELD_Z", "value-1", "value-1");
        test(lookup, "FIELD_Z");
    }

    @Test
    public void testInvalidDataTypeFilter() {
        withDateRange("20240701", "20240709");
        withDatatypeFilter(Set.of("datatype-z"));
        BoundedRangeIndexLookup lookup = createLookup("FIELD_A", "value-1", "value-1");
        test(lookup, "FIELD_A");
    }

    @Test
    public void testWithNoBackingData() {
        withDateRange("20240701", "20240709");
        BoundedRangeIndexLookup lookup = createLookup("FIELD_A", "absent-lower", "absent-upper");
        test(lookup, "FIELD_A");
    }

    @Test
    public void testExecutionHints_expansionPoolSelectedOverIndexTable() {
        withDateRange("20240701", "20240701");
        withDatatypeFilter(Set.of("datatype-b"));
        withExpected(Set.of("value-1"));

        Map<String,String> expansionHints = new HashMap<>();
        expansionHints.put("scan_type", "expansion-pool-a");
        expansionHints.put("priority", "2");

        Map<String,String> indexHints = new HashMap<>();
        indexHints.put("scan_type", "index-a");
        indexHints.put("priority", "1");

        Map<String,Map<String,String>> tableHints = new HashMap<>();
        tableHints.put("expansion", expansionHints);
        tableHints.put("shardIndex", indexHints);
        config.setTableHints(tableHints);

        BoundedRangeIndexLookup lookup = createLookup("FIELD_A", "value-1", "value-1");
        test(lookup, "FIELD_A");

        assertNotNull(lookup.builder);
        assertEquals(expansionHints, lookup.builder.getExecutionHints());
    }

    @Test
    public void testExecutionHints_indexTableNameSelectedWhenNoExpansionPoolExists() {
        withDateRange("20240701", "20240701");
        withDatatypeFilter(Set.of("datatype-b"));
        withExpected(Set.of("value-1"));

        Map<String,String> indexHints = new HashMap<>();
        indexHints.put("scan_type", "index-a");
        indexHints.put("priority", "1");

        Map<String,Map<String,String>> tableHints = new HashMap<>();
        tableHints.put("shardIndex", indexHints);
        config.setTableHints(tableHints);

        BoundedRangeIndexLookup lookup = createLookup("FIELD_A", "value-1", "value-1");
        test(lookup, "FIELD_A");

        assertNotNull(lookup.builder);
        assertEquals(indexHints, lookup.builder.getExecutionHints());
    }

    @Test
    public void testConsistencyLevel() {
        withDateRange("20240701", "20240701");
        withDatatypeFilter(Set.of("datatype-b"));
        withExpected(Set.of("value-1"));

        Map<String,ConsistencyLevel> consistencyLevels = new HashMap<>();
        consistencyLevels.put("shardIndex", ConsistencyLevel.EVENTUAL);
        config.setTableConsistencyLevels(consistencyLevels);

        BoundedRangeIndexLookup lookup = createLookup("FIELD_A", "value-1", "value-1");
        test(lookup, "FIELD_A");

        assertNotNull(lookup.builder);
        assertEquals(ConsistencyLevel.EVENTUAL, lookup.builder.getConsistencyLevel());
    }

    private void test(BoundedRangeIndexLookup lookup, String field) {
        lookup.submit();

        IndexLookupMap lookupMap = lookup.lookup();

        if (expected.isEmpty()) {
            assertTrue(lookupMap.keySet().isEmpty());
        } else {
            assertTrue(lookupMap.containsKey(field));
            Set<String> values = new HashSet<>(lookupMap.get(field));
            assertEquals(expected, values);
        }
    }

    private BoundedRangeIndexLookup createLookup(String field, String lower, String upper) {
        LiteralRange<?> range = new LiteralRange<>(lower, true, upper, true, field, LiteralRange.NodeOperand.AND);
        return createLookup(range);
    }

    private BoundedRangeIndexLookup createLookup(LiteralRange<?> range) {
        BoundedRangeIndexLookup lookup = new BoundedRangeIndexLookup(config, scannerFactory, range, executorService);
        lookup.setScanMonitor(monitor);
        return lookup;
    }

    private void withDateRange(String start, String end) {
        assertNotNull(config);
        config.setBeginDate(DateHelper.parse(start));
        config.setEndDate(DateHelper.parse(end));
    }

    private void withDatatypeFilter(Set<String> datatypes) {
        assertNotNull(config);
        config.setDatatypeFilter(datatypes);
    }

    private void withExpected(Set<String> expected) {
        assertTrue("should only set expected values once per test", this.expected.isEmpty());
        this.expected.addAll(expected);
    }

    private Set<String> createAllValues(int start, int stop) {
        Set<String> values = new HashSet<>();
        for (int i = start; i < stop; i++) {
            values.add("value-" + i);
        }
        return values;
    }

    @Test
    public void largeRowInBoundedRangeTest() throws TableNotFoundException {
        withDateRange("20240701", "20240701");
        // defaults to 5000
        config.setMaxValueExpansionThreshold(1);
        AccumuloClient lookupClient = createMock(AccumuloClient.class);
        config.setClient(lookupClient);
        LiteralRange<String> range = new LiteralRange<>("R", true, "S", false, "FOO", LiteralRange.NodeOperand.OR);
        BoundedRangeIndexLookup largeLookup = createLookup(range);
        // create index data to iterate over
        List<Map.Entry<Key,Value>> src = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            src.add(new AbstractMap.SimpleImmutableEntry<>(new Key("R" + i, "FOO", "20240701_1" + '\0' + "myDataType"), new Value()));
        }
        SortedListKeyValueIterator itr = new SortedListKeyValueIterator(src);
        LocalBatchScanner localScanner = new LocalBatchScanner(itr, true);
        // Route the current Scanner API through the instrumented local iterator stack.
        Scanner scanner = createMock(Scanner.class);
        expect(lookupClient.createScanner(eq(TableName.SHARD_INDEX), isA(Authorizations.class))).andReturn(scanner);
        scanner.setRange(isA(Range.class));
        expectLastCall().andAnswer(() -> {
            Range scanRange = getCurrentArgument(0);
            localScanner.setRanges(Set.of(scanRange));
            return null;
        });
        scanner.fetchColumnFamily(isA(Text.class));
        expectLastCall().andAnswer(() -> {
            Text columnFamily = getCurrentArgument(0);
            localScanner.fetchColumnFamily(columnFamily);
            return null;
        });
        scanner.addScanIterator(isA(IteratorSetting.class));
        expectLastCall().andAnswer(() -> {
            localScanner.addScanIterator(getCurrentArgument(0));
            return null;
        });
        expect(scanner.iterator()).andAnswer(localScanner::iterator);
        scanner.close();
        replayAll();
        largeLookup.submit();
        IndexLookupMap map = largeLookup.lookup();
        // verify we went over all the data even though the threshold was lower than this
        assertEquals(10001, localScanner.getSeekCount()); // initial seek + one seek per unique row in the range
        // this represents data collapsed and sent back to the client by the WholeRowIterator
        assertEquals(0, localScanner.getNextCount()); // no next calls with seeking filter
        assertNotNull(map);
        assertTrue(map.containsKey("FOO"));
        assertTrue(map.get("FOO").isThresholdExceeded());
        verifyAll();
    }
}
