package datawave.test.index;

import static datawave.util.TableName.SHARD_INDEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CloneConfiguration;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.CloneConfigurationImpl;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import datawave.accumulo.inmemory.InMemoryAccumulo;
import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.ingest.protobuf.Uid;
import datawave.util.TableName;

/**
 * This class contains all boilerplate code to support declarative style tests
 */
public abstract class IndexConversionUtils {

    private static final Logger log = LoggerFactory.getLogger(IndexConversionUtils.class);

    @TempDir
    public static Path macFolder;

    private static final String PASS = "password";
    private static MiniAccumuloCluster mac;

    protected static AccumuloClient macClient;
    protected static AccumuloClient imaClient;

    protected static TableOperations macTops;
    protected static TableOperations imaTops;

    private final String DEFAULT_VISIBILITY = "VIZ-A";

    private static final Authorizations auths = new Authorizations("VIZ-A", "VIZ-B", "VIZ-C");

    protected final List<Map.Entry<Key,Value>> expected = new ArrayList<>();

    @BeforeAll
    public static void beforeAll() throws Exception {
        MiniAccumuloConfig config = new MiniAccumuloConfig(macFolder.toFile(), PASS);
        config.setNumTservers(1);

        mac = new MiniAccumuloCluster(config);
        mac.start();

        macClient = mac.createAccumuloClient("root", new PasswordToken(PASS));
        macTops = macClient.tableOperations();

        InMemoryInstance instance = new InMemoryInstance(IndexConversionUtils.class.getName());
        imaClient = new InMemoryAccumuloClient("root", instance);
        imaTops = imaClient.tableOperations();

        macClient.securityOperations().changeUserAuthorizations("root", auths);
        imaClient.securityOperations().changeUserAuthorizations("root", auths);
    }

    @BeforeEach
    public void beforeEach() {
        recreateTable(imaTops, SHARD_INDEX);
        recreateTable(macTops, SHARD_INDEX);
        configureShardIndex(imaTops);
        configureShardIndex(macTops);
        expected.clear();
    }

    @AfterEach
    public void afterEach() {
        deleteTable(imaTops, getCloneTableName());
        deleteTable(macTops, getCloneTableName());
    }

    protected void recreateTable(TableOperations tops, String tableName) {
        if (tops.exists(tableName)) {
            try {
                tops.delete(tableName);
            } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
                fail("Failed to delete: " + tableName, e);
            }
        }
        try {
            tops.create(tableName);
        } catch (AccumuloException | AccumuloSecurityException | TableExistsException e) {
            fail("Failed to create: " + tableName, e);
        }
    }

    protected void deleteTable(TableOperations tops, String tableName) {
        if (tops.exists(tableName)) {
            try {
                tops.delete(tableName);
            } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
                fail("Failed to delete: " + tableName, e);
            }
        }
    }

    protected void configureShardIndex(TableOperations tops) {
        try {
            for (var scope : IteratorUtil.IteratorScope.values()) {
                String name = "table.iterator." + scope.name() + ".agg";
                String opt = "table.iterator." + scope.name() + ".agg.opt.*";

                tops.setProperty(SHARD_INDEX, name, "19,datawave.iterators.TotalAggregatingIterator");
                tops.setProperty(SHARD_INDEX, opt, "datawave.ingest.table.aggregator.KeepCountOnlyUidAggregator");
            }
        } catch (Exception e) {
            fail("Failed to configure shard index", e);
        }
    }

    @AfterAll
    public static void afterAll() throws Exception {
        mac.stop();
    }

    /**
     * Write a key and value to the shard index for both the InMemoryAccumulo and MiniAccumuloCluster instances
     *
     * @param key
     *            the key
     * @param value
     *            the value
     */
    protected void write(Key key, Value value) {
        write(SHARD_INDEX, key, value);
    }

    /**
     * Write a key and value to the specified table for both the InMemoryAccumulo and MiniAccumuloCluster instances
     *
     * @param tableName
     *            the table name
     * @param key
     *            the key
     * @param value
     *            the value
     */
    protected void write(String tableName, Key key, Value value) {
        try (var bw = macClient.createBatchWriter(tableName)) {
            Mutation m = new Mutation(key.getRow());
            m.put(key.getColumnFamily(), key.getColumnQualifier(), key.getColumnVisibilityParsed(), key.getTimestamp(), value);
            bw.addMutation(m);
        } catch (TableNotFoundException | MutationsRejectedException e) {
            fail(e.getMessage(), e);
        }

        try (var bw = imaClient.createBatchWriter(tableName)) {
            Mutation m = new Mutation(key.getRow());
            m.put(key.getColumnFamily(), key.getColumnQualifier(), key.getColumnVisibilityParsed(), key.getTimestamp(), value);
            bw.addMutation(m);
        } catch (TableNotFoundException | MutationsRejectedException e) {
            fail(e.getMessage(), e);
        }
    }

    /**
     * Add a key and value to the list of expected results
     *
     * @param key
     *            the key
     * @param value
     *            the value
     */
    protected void expect(Key key, Value value) {
        expected.add(new AbstractMap.SimpleEntry<>(key, value));
    }

    protected Key create(String row, String cf, String cq) {
        return create(row, cf, cq, DEFAULT_VISIBILITY);
    }

    protected Key create(String row, String cf, String cq, String cv) {
        return new Key(row, cf, cq, cv, 0L);
    }

    protected Key delete(String row, String cf, String cq) {
        return new Key(row.getBytes(), cf.getBytes(), cq.getBytes(), DEFAULT_VISIBILITY.getBytes(), 1L, true);
    }

    /**
     * Clone the {@link TableName#SHARD_INDEX} to the configured {@link #getCloneTableName()}, configure the cloned table, and compact it.
     */
    protected void cloneAndCompactMac() {
        cloneAndCompactMac(macTops, getCloneTableName());
    }

    /**
     * Clone the {@link TableName#SHARD_INDEX} to the configured {@link #getCloneTableName()}, configure the cloned table, and compact it.
     * <p>
     * TableOperations dictates usage of {@link MiniAccumuloCluster} or {@link InMemoryAccumulo}
     *
     * @param tops
     *            the TableOperations
     * @param tableName
     *            the new table name
     */
    protected void cloneAndCompactMac(TableOperations tops, String tableName) {
        try {
            //  @formatter:off
            CloneConfiguration cloneConfiguration = new CloneConfigurationImpl()
                    .setFlush(true)
                    .setKeepOffline(false)
                    .build();
            //  @formatter:on
            tops.clone(SHARD_INDEX, tableName, cloneConfiguration);
        } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException | TableExistsException e) {
            fail("failed to clone table: " + tableName, e);
        }

        configureClonedTable(tops, tableName);

        try {
            CompactionConfig compactionConfig = new CompactionConfig();
            compactionConfig.setFlush(true);
            compactionConfig.setWait(true);
            compactionConfig.setStartRow(new Text("\0"));
            compactionConfig.setEndRow(new Text("~"));

            tops.compact(tableName, compactionConfig);
        } catch (AccumuloSecurityException | TableNotFoundException | AccumuloException e) {
            fail("failed to compact table: " + tableName, e);
        }
    }

    protected abstract void configureClonedTable(TableOperations tops, String tableName);

    /**
     * {@link InMemoryAccumulo} does not support a compact operation, so create a new table, configure it, and copy data in.
     * <p>
     * The configured iterator at scan time simulates a compaction and should match the output of {@link #cloneAndCompactMac()}
     */
    protected void cloneAndCopyIma() {
        recreateTable(imaTops, getCloneTableName());
        configureClonedTable(imaTops, getCloneTableName());

        try (var scanner = imaClient.createScanner(SHARD_INDEX, auths); var writer = imaClient.createBatchWriter(getCloneTableName())) {

            scanner.setRange(new Range());
            for (var entry : scanner) {
                Key k = entry.getKey();
                Mutation m = new Mutation(k.getRow());
                m.put(k.getColumnFamily(), k.getColumnQualifier(), k.getColumnVisibilityParsed(), k.getTimestamp(), entry.getValue());
                writer.addMutation(m);
            }
        } catch (Exception e) {
            fail("failed to clone and copy table: " + SHARD_INDEX + " to " + getCloneTableName(), e);
        }
    }

    /**
     * Scan the table hosted by the {@link MiniAccumuloCluster} instance
     *
     * @param tableName
     *            the table name
     * @return a {@link ScanResult}
     */
    protected ScanResult scanMac(String tableName) {
        return scanTable(tableName, macClient);
    }

    /**
     * Scan the table hosted by the {@link InMemoryAccumulo} instance
     *
     * @param tableName
     *            the table name
     * @return a {@link ScanResult}
     */
    protected ScanResult scanIma(String tableName) {
        return scanTable(tableName, imaClient);
    }

    /**
     * Scan the table and return a {@link ScanResult}
     *
     * @param tableName
     *            the table name
     * @param client
     *            the client used to create the scanner
     * @return a scan result
     */
    protected ScanResult scanTable(String tableName, AccumuloClient client) {
        ScanResult scanResult = new ScanResult(tableName);
        try (var scanner = client.createScanner(tableName, auths)) {
            scanner.setRange(new Range());
            for (var entry : scanner) {
                scanResult.addResult(entry);
            }
            return scanResult;
        } catch (TableNotFoundException e) {
            log.error("failed to scan {}", tableName);
            throw new RuntimeException(e);
        } finally {
            log.info("scan of {} has k: {} v: {} bytes", tableName, scanResult.keyBytes, scanResult.valueBytes);
        }
    }

    protected void assertUidTable(String tableName) {
        assertUidTable(macClient, tableName);
        assertUidTable(imaClient, tableName);
    }

    protected void assertUidTable(AccumuloClient client, String tableName) {
        ScanResult result = scanTable(tableName, client);
        assertEquals(expected.size(), result.numKeys);
        assertUidResults(result);
    }

    /**
     * Method handles asserting uid results. The order of elements in the uid list is not guaranteed, so this method enforces order for better comparison.
     *
     * @param results
     *            the {@link ScanResult}
     */
    protected void assertUidResults(ScanResult results) {
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i).getKey(), results.getResults().get(i).getKey(), "key mismatch");
            try {
                Uid.List expectedList = Uid.List.parseFrom(expected.get(i).getValue().get());
                Uid.List resultList = Uid.List.parseFrom(results.getResults().get(i).getValue().get());

                List<String> expectedUids = new ArrayList<>(expectedList.getUIDList());
                List<String> resultUids = new ArrayList<>(resultList.getUIDList());

                Collections.sort(expectedUids);
                Collections.sort(resultUids);

                assertEquals(expectedUids, resultUids, "uid list mismatch");
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected abstract String getCloneTableName();

    protected abstract void assertClonedResults(ScanResult results);

    protected void assertClonedResults() {
        assertClonedResults(getCloneTableName(), imaClient);
        assertClonedResults(getCloneTableName(), macClient);
    }

    protected void assertClonedResults(String tableName, AccumuloClient client) {
        ScanResult result = scanTable(tableName, client);
        assertEquals(expected.size(), result.numKeys);
        assertClonedResults(result);
    }

    protected Value createNoUidValue(int count) {
        Uid.List.Builder b = Uid.List.newBuilder();
        b.setIGNORE(true);
        b.setCOUNT(count);
        return new Value(b.build().toByteArray());
    }

    protected Value createUidValue(String uid) {
        Uid.List.Builder b = Uid.List.newBuilder();
        b.setIGNORE(false);
        b.addAllUID(List.of(uid));
        b.setCOUNT(1);
        return new Value(b.build().toByteArray());
    }

    protected Value createUidValue(String... uids) {
        Uid.List.Builder b = Uid.List.newBuilder();
        b.setIGNORE(false);
        b.addAllUID(List.of(uids));
        b.setCOUNT(uids.length);
        return new Value(b.build().toByteArray());
    }

    protected Value createBitSetValue(int... bits) {
        return new Value(createBitSet(bits).toByteArray());
    }

    protected BitSet createBitSet(int... bits) {
        BitSet bitSet = new BitSet();
        for (int bit : bits) {
            bitSet.set(bit);
        }
        return bitSet;
    }
}
