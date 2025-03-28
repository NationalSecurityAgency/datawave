package datawave.ingest.table.aggregator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.ingest.protobuf.Uid;
import datawave.util.TableName;

/**
 * A series of integration tests using {@link MiniAccumuloCluster} to exercise various global index aggregators
 * <p>
 *
 */
public class AggregatorIntegrationTests {

    private static final Logger log = LoggerFactory.getLogger(AggregatorIntegrationTests.class);

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final String PASSWORD = "password";
    private static MiniAccumuloCluster mac;
    private static AccumuloClient client;

    private static final String DEFAULT_AGGREGATOR = TableName.SHARD_INDEX;
    private static final String UID_AGGREGATOR = "uidAggregator";
    private static final String KEEP_COUNT_AGGREGATOR = "keepCountAggregator";
    private static final String COUNT_ONLY_AGGREGATOR = "countOnlyAggregator";

    private static final String[] tableNames = new String[] {DEFAULT_AGGREGATOR, UID_AGGREGATOR, KEEP_COUNT_AGGREGATOR, COUNT_ONLY_AGGREGATOR};

    private final List<String> existingRows = new ArrayList<>();

    private Value defaultValue;
    private Value uidValue;
    private Value keepCountValue;
    private Value countOnlyValue;

    @BeforeClass
    public static void setup() throws Exception {
        MiniAccumuloConfig config = new MiniAccumuloConfig(temporaryFolder.newFolder(), PASSWORD);
        config.setNumTservers(1);

        mac = new MiniAccumuloCluster(config);
        mac.start();

        client = mac.createAccumuloClient("root", new PasswordToken(PASSWORD));

        configureTables();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        mac.stop();
    }

    private static void configureTables() throws Exception {
        for (String tableName : tableNames) {
            client.tableOperations().create(tableName);
        }

        // NOTE: accumulo sets the VersioningIterator at priority 20

        String name = "table.iterator.scan.agg";
        String opt = "table.iterator.scan.agg.opt.*";

        client.tableOperations().setProperty(DEFAULT_AGGREGATOR, name, "19,datawave.iterators.TotalAggregatingIterator");
        client.tableOperations().setProperty(DEFAULT_AGGREGATOR, opt, "datawave.iterators.PropogatingIterator");

        client.tableOperations().setProperty(UID_AGGREGATOR, name, "19,datawave.iterators.TotalAggregatingIterator");
        client.tableOperations().setProperty(UID_AGGREGATOR, opt, "datawave.ingest.table.aggregator.GlobalIndexUidAggregator");

        client.tableOperations().setProperty(KEEP_COUNT_AGGREGATOR, name, "19,datawave.iterators.TotalAggregatingIterator");
        client.tableOperations().setProperty(KEEP_COUNT_AGGREGATOR, opt, "datawave.ingest.table.aggregator.KeepCountOnlyUidAggregator");

        client.tableOperations().setProperty(COUNT_ONLY_AGGREGATOR, name, "19,datawave.iterators.TotalAggregatingIterator");
        client.tableOperations().setProperty(COUNT_ONLY_AGGREGATOR, opt, "datawave.ingest.table.aggregator.KeepCountOnlyNoUidAggregator");

        name = "table.iterator.minc.agg";
        opt = "table.iterator.minc.agg.opt.*";

        client.tableOperations().setProperty(DEFAULT_AGGREGATOR, name, "19,datawave.iterators.TotalAggregatingIterator");
        client.tableOperations().setProperty(DEFAULT_AGGREGATOR, opt, "datawave.iterators.PropogatingIterator");

        client.tableOperations().setProperty(UID_AGGREGATOR, name, "19,datawave.iterators.TotalAggregatingIterator");
        client.tableOperations().setProperty(UID_AGGREGATOR, opt, "datawave.ingest.table.aggregator.GlobalIndexUidAggregator");

        client.tableOperations().setProperty(KEEP_COUNT_AGGREGATOR, name, "19,datawave.iterators.TotalAggregatingIterator");
        client.tableOperations().setProperty(KEEP_COUNT_AGGREGATOR, opt, "datawave.ingest.table.aggregator.KeepCountOnlyUidAggregator");

        client.tableOperations().setProperty(COUNT_ONLY_AGGREGATOR, name, "19,datawave.iterators.TotalAggregatingIterator");
        client.tableOperations().setProperty(COUNT_ONLY_AGGREGATOR, opt, "datawave.ingest.table.aggregator.KeepCountOnlyNoUidAggregator");

        name = "table.iterator.majc.agg";
        opt = "table.iterator.majc.agg.opt.*";

        client.tableOperations().setProperty(DEFAULT_AGGREGATOR, name, "19,datawave.iterators.TotalAggregatingIterator");
        client.tableOperations().setProperty(DEFAULT_AGGREGATOR, opt, "datawave.iterators.PropogatingIterator");

        client.tableOperations().setProperty(UID_AGGREGATOR, name, "19,datawave.iterators.TotalAggregatingIterator");
        client.tableOperations().setProperty(UID_AGGREGATOR, opt, "datawave.ingest.table.aggregator.GlobalIndexUidAggregator");

        client.tableOperations().setProperty(KEEP_COUNT_AGGREGATOR, name, "19,datawave.iterators.TotalAggregatingIterator");
        client.tableOperations().setProperty(KEEP_COUNT_AGGREGATOR, opt, "datawave.ingest.table.aggregator.KeepCountOnlyUidAggregator");

        client.tableOperations().setProperty(COUNT_ONLY_AGGREGATOR, name, "19,datawave.iterators.TotalAggregatingIterator");
        client.tableOperations().setProperty(COUNT_ONLY_AGGREGATOR, opt, "datawave.ingest.table.aggregator.KeepCountOnlyNoUidAggregator");
    }

    @Test
    public void testAddSingleUid() {
        String row = getRandomRow();
        writeUid(row, 10L, "uid");

        scanTables(row);
        assertList(DEFAULT_AGGREGATOR, "uid");
        assertList(UID_AGGREGATOR, "uid");
        assertList(KEEP_COUNT_AGGREGATOR, "uid");
        assertList(COUNT_ONLY_AGGREGATOR, true, 1);

        compactRow(row);

        scanTables(row);
        assertList(DEFAULT_AGGREGATOR, "uid");
        assertList(UID_AGGREGATOR, "uid");
        assertList(KEEP_COUNT_AGGREGATOR, "uid");
        assertList(COUNT_ONLY_AGGREGATOR, true, 1);
    }

    @Test
    public void testAddRemoveUid() {
        String row = getRandomRow();
        writeUid(row, 10L, "uid");
        writeRemoval(row, 10L, "uid");

        scanTables(row);
        assertList(DEFAULT_AGGREGATOR, false, -1);
        assertNoList(UID_AGGREGATOR);
        assertNoList(KEEP_COUNT_AGGREGATOR);
        assertList(COUNT_ONLY_AGGREGATOR, true, 0);

        compactRow(row);

        scanTables(row);
        assertList(DEFAULT_AGGREGATOR, false, -1);
        assertNoList(UID_AGGREGATOR);
        assertNoList(KEEP_COUNT_AGGREGATOR);
        assertList(COUNT_ONLY_AGGREGATOR, true, 0);
    }

    @Test
    public void testAddUidsUntilIgnoreThresholdIsTriggered() {
        // at 20 uids the uids are removed from the list and only the count is retained
        String row = getRandomRow();
        for (int i = 0; i < 25; i++) {
            writeUid(row, 10L, "uid-" + i);
        }

        scanTables(row);
        assertList(DEFAULT_AGGREGATOR, false, 1);
        assertList(UID_AGGREGATOR, true, 25);
        assertList(KEEP_COUNT_AGGREGATOR, true, 25);
        assertList(COUNT_ONLY_AGGREGATOR, true, 25);

        compactRow(row);

        scanTables(row);
        assertList(DEFAULT_AGGREGATOR, false, 1);
        assertList(UID_AGGREGATOR, true, 25);
        assertList(KEEP_COUNT_AGGREGATOR, true, 25);
        assertList(COUNT_ONLY_AGGREGATOR, true, 25);
    }

    @Test
    public void testAddUidsWithIncreasingTimeStampsUntilIgnoreThresholdIsTriggered() {
        // at 20 uids the uids are removed from the list and only the count is retained
        String row = getRandomRow();
        for (int i = 0; i < 25; i++) {
            writeUid(row, 10 + i, "uid-" + i);
        }

        scanTables(row);
        assertList(DEFAULT_AGGREGATOR, false, 1);
        assertList(UID_AGGREGATOR, true, 25);
        assertList(KEEP_COUNT_AGGREGATOR, true, 25);
        assertList(COUNT_ONLY_AGGREGATOR, true, 25);

        compactRow(row);

        scanTables(row);
        assertList(DEFAULT_AGGREGATOR, false, 1);
        assertList(UID_AGGREGATOR, true, 25);
        assertList(KEEP_COUNT_AGGREGATOR, true, 25);
        assertList(COUNT_ONLY_AGGREGATOR, true, 25);
    }

    @Test
    public void testAddAndRemoveUidsWithIncreasingTimeStamps() {
        String row = getRandomRow();
        for (int i = 0; i < 25; i++) {
            writeUid(row, 10 + i, "uid-" + i);
            writeRemoval(row, 10 + i, "uid-" + i);
        }

        scanTables(row);
        assertList(DEFAULT_AGGREGATOR, false, -1);
        assertNoList(UID_AGGREGATOR);
        assertList(KEEP_COUNT_AGGREGATOR, true, 0);
        assertList(COUNT_ONLY_AGGREGATOR, true, 0);

        compactRow(row);

        scanTables(row);
        assertList(DEFAULT_AGGREGATOR, false, -1);
        assertNoList(UID_AGGREGATOR);
        assertList(KEEP_COUNT_AGGREGATOR, true, 0);
        assertList(COUNT_ONLY_AGGREGATOR, true, 0);
    }

    @Test
    public void testAddDuplicateUids() {
        String row = getRandomRow();
        for (int i = 0; i < 10; i++) {
            writeUid(row, 10L, "uid");
        }

        scanTables(row);
        assertList(DEFAULT_AGGREGATOR, false, 1);
        assertList(UID_AGGREGATOR, false, 1);
        assertList(KEEP_COUNT_AGGREGATOR, false, 1);
        assertList(COUNT_ONLY_AGGREGATOR, true, 10);

        compactRow(row);

        scanTables(row);
        assertList(DEFAULT_AGGREGATOR, false, 1);
        assertList(UID_AGGREGATOR, false, 1);
        assertList(KEEP_COUNT_AGGREGATOR, false, 1);
        assertList(COUNT_ONLY_AGGREGATOR, true, 10);
    }

    @Test
    public void testAddDuplicateUidsWithIncreasingTimeStamps() {
        String row = getRandomRow();
        for (int i = 0; i < 10; i++) {
            writeUid(row, 10 + i, "uid");
        }

        scanTables(row);
        assertList(DEFAULT_AGGREGATOR, false, 1);
        assertList(UID_AGGREGATOR, false, 1);
        assertList(KEEP_COUNT_AGGREGATOR, false, 1);
        assertList(COUNT_ONLY_AGGREGATOR, true, 10);

        compactRow(row);

        scanTables(row);
        assertList(DEFAULT_AGGREGATOR, false, 1);
        assertList(UID_AGGREGATOR, false, 1);
        assertList(KEEP_COUNT_AGGREGATOR, false, 1);
        assertList(COUNT_ONLY_AGGREGATOR, true, 10);
    }

    @Test
    public void testRemoveThenAddSameTimeStamp() {
        String row = getRandomRow();
        writeRemoval(row, 10L, "uid");
        writeUid(row, 10L, "uid");

        scanTables(row);
        assertList(DEFAULT_AGGREGATOR, false, 1);
        assertNoList(UID_AGGREGATOR);
        assertNoList(KEEP_COUNT_AGGREGATOR);
        assertList(COUNT_ONLY_AGGREGATOR, true, 0);

        compactRow(row);

        scanTables(row);
        assertList(DEFAULT_AGGREGATOR, false, 1);
        assertNoList(UID_AGGREGATOR);
        assertNoList(KEEP_COUNT_AGGREGATOR);
        assertList(COUNT_ONLY_AGGREGATOR, true, 0);
    }

    @Test
    public void testOrderByTimeStampRemoveThenAdd() {
        String row = getRandomRow();
        writeRemoval(row, 10L, "uid");
        writeUid(row, 11L, "uid");

        scanTables(row);
        assertList(DEFAULT_AGGREGATOR, false, 1);
        assertNoList(UID_AGGREGATOR);
        assertNoList(KEEP_COUNT_AGGREGATOR);
        assertList(COUNT_ONLY_AGGREGATOR, true, 0);

        compactRow(row);

        scanTables(row);
        assertList(DEFAULT_AGGREGATOR, false, 1);
        assertNoList(UID_AGGREGATOR);
        assertNoList(KEEP_COUNT_AGGREGATOR);
        assertList(COUNT_ONLY_AGGREGATOR, true, 0);
    }

    @Test
    public void testCountOnly() {
        String row = getRandomRow();
        writeCount(row, 10L, 23);

        scanTables(row);
        assertList(DEFAULT_AGGREGATOR, true, 23);
        assertList(UID_AGGREGATOR, true, 23);
        assertList(KEEP_COUNT_AGGREGATOR, true, 23);
        assertList(COUNT_ONLY_AGGREGATOR, true, 23);

        compactRow(row);

        scanTables(row);
        assertList(DEFAULT_AGGREGATOR, true, 23);
        assertList(UID_AGGREGATOR, true, 23);
        assertList(KEEP_COUNT_AGGREGATOR, true, 23);
        assertList(COUNT_ONLY_AGGREGATOR, true, 23);
    }

    @Test
    public void testMultipleCountOnly() {
        String row = getRandomRow();
        writeCount(row, 10L, 23);
        writeCount(row, 10L, 34);

        scanTables(row);
        assertList(DEFAULT_AGGREGATOR, true, 34);
        assertList(UID_AGGREGATOR, true, 57);
        assertList(KEEP_COUNT_AGGREGATOR, true, 57);
        assertList(COUNT_ONLY_AGGREGATOR, true, 57);

        compactRow(row);

        scanTables(row);
        assertList(DEFAULT_AGGREGATOR, true, 34);
        assertList(UID_AGGREGATOR, true, 57);
        assertList(KEEP_COUNT_AGGREGATOR, true, 57);
        assertList(COUNT_ONLY_AGGREGATOR, true, 57);
    }

    @Test
    public void testCountOnlyAndUid() {
        String row = getRandomRow();
        writeCount(row, 10L, 23);
        writeUid(row, 10L, "uid");

        scanTables(row);
        assertList(DEFAULT_AGGREGATOR, "uid");
        assertList(UID_AGGREGATOR, true, 24);
        assertList(KEEP_COUNT_AGGREGATOR, true, 24);
        assertList(COUNT_ONLY_AGGREGATOR, true, 24);

        compactRow(row);

        scanTables(row);
        assertList(DEFAULT_AGGREGATOR, "uid");
        assertList(UID_AGGREGATOR, true, 24);
        assertList(KEEP_COUNT_AGGREGATOR, true, 24);
        assertList(COUNT_ONLY_AGGREGATOR, true, 24);
    }

    @Test
    public void testCountOnlyAndUidWithRemoval() {
        String row = getRandomRow();
        writeCount(row, 10L, 23);
        writeRemoval(row, 10L, "uid");

        scanTables(row);
        assertList(DEFAULT_AGGREGATOR, false, -1);
        assertList(UID_AGGREGATOR, true, 22);
        assertList(KEEP_COUNT_AGGREGATOR, true, 22);
        assertList(COUNT_ONLY_AGGREGATOR, true, 22);

        compactRow(row);

        scanTables(row);
        assertList(DEFAULT_AGGREGATOR, false, -1);
        assertList(UID_AGGREGATOR, true, 22);
        assertList(KEEP_COUNT_AGGREGATOR, true, 22);
        assertList(COUNT_ONLY_AGGREGATOR, true, 22);
    }

    protected void scanTables(String row) {
        for (String tableName : tableNames) {
            try (Scanner scanner = client.createScanner(tableName)) {
                scanner.setRange(new Range(row));

                int count = 0;
                Value value = null;
                for (Map.Entry<Key,Value> entry : scanner) {
                    log.trace("k: {} v: {}", entry.getKey(), entry.getValue());
                    count++;
                    value = entry.getValue();
                }

                assertTrue(count <= 1);

                // some test cases may hide keys, for example the case where a single uid is added and then removed

                switch (tableName) {
                    case DEFAULT_AGGREGATOR:
                        defaultValue = value;
                        break;
                    case UID_AGGREGATOR:
                        uidValue = value;
                        break;
                    case KEEP_COUNT_AGGREGATOR:
                        keepCountValue = value;
                        break;
                    case COUNT_ONLY_AGGREGATOR:
                        countOnlyValue = value;
                        break;
                    default:
                        fail("Unknown table: " + tableName);
                }

            } catch (Exception e) {
                e.printStackTrace();
                fail("Failed to scan table " + tableName);
                throw new RuntimeException(e);
            }
        }
    }

    private String getRandomRow() {
        String row = null;
        while (row == null || existingRows.contains(row)) {
            row = RandomStringUtils.randomAlphabetic(6);
        }
        existingRows.add(row);
        return row;
    }

    private void compactRow(String row) {
        for (String tableName : tableNames) {
            try {
                client.tableOperations().compact(tableName, new Text(row), new Text(row + '~'), true, true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void assertNoList(String tableName) {
        assertNull(getValueForTable(tableName));
    }

    private void assertList(String table, boolean ignore, int count) {
        Value value = getValueForTable(table);
        assertNotNull("Expected value to not be null", value);
        Uid.List list = UidTestUtils.valueToUidList(value);
        assertNotNull(list);
        assertEquals(ignore, list.getIGNORE());
        assertTrue(list.hasCOUNT());
        assertEquals(count, list.getCOUNT());
    }

    private void assertList(String table, String... uids) {
        Value value = getValueForTable(table);
        assertNotNull("Expected value to not be null", value);
        Uid.List list = UidTestUtils.valueToUidList(value);
        assertNotNull(list);
        assertFalse(list.getIGNORE());
        assertTrue(list.hasCOUNT());
        assertTrue(uids.length > 0);
        assertEquals(uids.length, list.getUIDList().size());
        for (String uid : uids) {
            assertTrue(list.getUIDList().contains(uid));
        }
    }

    private Value getValueForTable(String table) {
        switch (table) {
            case DEFAULT_AGGREGATOR:
                return defaultValue;
            case UID_AGGREGATOR:
                return uidValue;
            case KEEP_COUNT_AGGREGATOR:
                return keepCountValue;
            case COUNT_ONLY_AGGREGATOR:
                return countOnlyValue;
            default:
                fail("Unknown table: " + table);
                // technically unreachable but test framework isn't smart enough to know
                return null;
        }
    }

    protected void writeUid(String row, long timestamp, String uid) {
        Key key = new Key(row, "cf", "cq", "cv", timestamp);
        Value value = UidTestUtils.uidList(uid);
        writeKeyValue(key, value);
    }

    protected void writeCount(String row, long timestamp, int count) {
        Key key = new Key(row, "cf", "cq", "cv", timestamp);
        Value value = UidTestUtils.countOnlyList(count);
        writeKeyValue(key, value);
    }

    protected void writeRemoval(String row, long timestamp, String uid) {
        Key key = new Key(row, "cf", "cq", "cv", timestamp);
        Value value = UidTestUtils.removeUidList(uid);
        writeKeyValue(key, value);
    }

    protected void writeKeyValue(Key key, Value value) {
        for (String tableName : tableNames) {
            try (BatchWriter bw = client.createBatchWriter(tableName)) {
                Mutation m = new Mutation(key.getRow());
                m.put(key.getColumnFamily(), key.getColumnQualifier(), key.getTimestamp(), value);
                bw.addMutation(m);
            } catch (TableNotFoundException | MutationsRejectedException e) {
                fail("Failed to write test data to table " + tableName);
                throw new RuntimeException(e);
            }
        }
    }
}
