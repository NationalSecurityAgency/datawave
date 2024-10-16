package datawave.query.jexl.lookups;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.easymock.PowerMock.replayAll;
import static org.powermock.api.easymock.PowerMock.verifyAll;

import java.text.SimpleDateFormat;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Date;
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
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.easymock.EasyMockSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import datawave.microservice.query.QueryImpl;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.iterator.SortedListKeyValueIterator;
import datawave.query.jexl.LiteralRange;
import datawave.query.scanner.LocalBatchScanner;
import datawave.query.tables.ScannerFactory;
import datawave.util.TableName;
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

    // variables for large row test
    private BoundedRangeIndexLookup largeLookup;
    private ShardQueryConfiguration largeConfig;
    private ScannerFactory largeScannerFactory;

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

        // large lookup
        largeConfig = new ShardQueryConfiguration();
        largeScannerFactory = createMock(ScannerFactory.class);
    }

    @After
    public void teardown() {
        executorService.shutdownNow();
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
        return new BoundedRangeIndexLookup(config, scannerFactory, range, executorService);
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
        ExecutorService s = Executors.newSingleThreadExecutor();
        Date begin = new Date();
        Date end = new Date();
        config.setBeginDate(begin);
        config.setEndDate(end);
        config.setNumQueryThreads(1);
        // defaults to 5000
        config.setMaxValueExpansionThreshold(1);
        SimpleDateFormat sdf = new SimpleDateFormat("YYYYMMdd");
        LiteralRange range = new LiteralRange("R", true, "S", false, "FOO", LiteralRange.NodeOperand.OR);
        largeLookup = new BoundedRangeIndexLookup(config, largeScannerFactory, range, s);
        // create index data to iterate over
        List<Map.Entry<Key,Value>> src = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            src.add(new AbstractMap.SimpleImmutableEntry<>(new Key("R" + i, "FOO", sdf.format(begin) + "_1" + '\0' + "myDataType"), new Value()));
        }
        SortedListKeyValueIterator itr = new SortedListKeyValueIterator(src);
        LocalBatchScanner scanner = new LocalBatchScanner(itr, true);
        // add expects for the scanner factory
        expect(largeScannerFactory.newScanner(eq("shardIndex"), isA(Set.class), eq(1), isA(QueryImpl.class), eq("shardIndex"))).andAnswer(() -> scanner);
        expect(largeScannerFactory.close(scanner)).andReturn(true);
        replayAll();
        largeLookup.submit();
        IndexLookupMap map = largeLookup.lookup();
        // verify we went over all the data even though the threshold was lower than this
        assertEquals(10001, scanner.getSeekCount()); // with new iterator this is initial seek + one seek per unique row in the range
        // this represents data collapsed and sent back to the client by the WholeRowIterator
        assertEquals(0, scanner.getNextCount()); // no next cals with seeking filter
        assertTrue(map.get("FOO").isThresholdExceeded());
        verifyAll();
    }
}
