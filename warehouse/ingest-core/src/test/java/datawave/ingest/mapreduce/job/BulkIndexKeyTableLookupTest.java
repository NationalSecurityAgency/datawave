package datawave.ingest.mapreduce.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Covers {@link BulkIndexKeyTableLookup} and the {@link BulkIngestKey} encoding built on it.
 * <p>
 * The dictionary is JVM wide state, so every test here restores {@link BulkIndexKeyTableLookup#EMPTY} in teardown; without that, a dictionary installed by one
 * test would change how {@link BulkIngestKey} serializes in every test that ran after it, in this class and in any other test class sharing the surefire JVM.
 */
public class BulkIndexKeyTableLookupTest {

    private static final List<String> TABLES = Arrays.asList("shard", "shardIndex", "shardReverseIndex", "DatawaveMetadata");

    @AfterEach
    public void tearDown() {
        BulkIndexKeyTableLookup.reset();
    }

    @Test
    public void testIdsAreAssignedInTableNameOrder() {
        BulkIndexKeyTableLookup dictionary = BulkIndexKeyTableLookup.install(TABLES);

        // capital D sorts before the lower case names in unsigned UTF-8 order, which is the order Text.compareTo imposes
        assertEquals(4, dictionary.size());
        assertEquals(new Text("DatawaveMetadata"), dictionary.nameFor(0));
        assertEquals(new Text("shard"), dictionary.nameFor(1));
        assertEquals(new Text("shardIndex"), dictionary.nameFor(2));
        assertEquals(new Text("shardReverseIndex"), dictionary.nameFor(3));

        for (int id = 0; id < dictionary.size(); id++) {
            assertEquals(id, dictionary.idFor(dictionary.nameFor(id)));
        }
    }

    @Test
    public void testUnknownTablesAndIds() {
        BulkIndexKeyTableLookup dictionary = BulkIndexKeyTableLookup.install(TABLES);

        assertEquals(BulkIndexKeyTableLookup.UNKNOWN_ID, dictionary.idFor(new Text("notATable")));
        assertEquals(BulkIndexKeyTableLookup.UNKNOWN_ID, dictionary.idFor(null));
        assertNull(dictionary.nameFor(-1));
        assertNull(dictionary.nameFor(dictionary.size()));
    }

    /**
     * {@link BulkIngestKey#getTableId()} memoizes the last id it resolved across keys, so it must be exercised over a sequence that repeats, switches, and asks
     * for tables the dictionary does not know - a stale memo would show up as a key taking the id of whatever key preceded it.
     */
    @Test
    public void testTableIdIsCorrectAcrossRepeatsSwitchesAndMisses() {
        BulkIndexKeyTableLookup.install(TABLES);

        List<String> probes = new ArrayList<>();
        Random rand = new Random(42);
        for (int i = 0; i < 500; i++) {
            // runs of the same table, punctuated by switches and by tables the dictionary does not know
            String table = TABLES.get(rand.nextInt(TABLES.size()));
            int run = 1 + rand.nextInt(4);
            for (int r = 0; r < run; r++) {
                probes.add(table);
            }
            if (rand.nextInt(4) == 0) {
                probes.add("undeclared" + rand.nextInt(3));
            }
        }

        List<String> sorted = sortedTables();
        for (String probe : probes) {
            int expected = TABLES.contains(probe) ? sorted.indexOf(probe) : BulkIndexKeyTableLookup.UNKNOWN_ID;
            // a fresh key per probe, since a key caches its own id and would never consult the memo twice
            BulkIngestKey bik = new BulkIngestKey(new Text(probe), key("row", 1234L));
            assertEquals(expected, bik.getTableId(), "wrong id for " + probe);
        }
    }

    /**
     * The memo is validated against the installed dictionary's names, so replacing the dictionary must invalidate it rather than let an id resolved under the
     * old mapping leak into the new one.
     */
    @Test
    public void testMemoDoesNotSurviveADictionarySwap() {
        BulkIndexKeyTableLookup.install(TABLES);
        assertEquals(3, new BulkIngestKey(new Text("shardReverseIndex"), key("row", 1L)).getTableId());

        // a dictionary in which shardReverseIndex is id 1, not 3
        BulkIndexKeyTableLookup.install(Arrays.asList("shard", "shardReverseIndex"));
        assertEquals(1, new BulkIngestKey(new Text("shardReverseIndex"), key("row", 1L)).getTableId());
        assertEquals(0, new BulkIngestKey(new Text("shard"), key("row", 1L)).getTableId());
    }

    /** @return the declared tables in the ascending {@link Text} order the dictionary assigns ids in */
    private static List<String> sortedTables() {
        List<String> sorted = new ArrayList<>(TABLES);
        sorted.sort((a, b) -> new Text(a).compareTo(new Text(b)));
        return sorted;
    }

    @Test
    public void testEmptyDictionaryKnowsNothing() {
        assertEquals(0, BulkIndexKeyTableLookup.get().size());
        assertEquals(BulkIndexKeyTableLookup.UNKNOWN_ID, BulkIndexKeyTableLookup.get().idFor(new Text("shard")));
    }

    @Test
    public void testCompareIdsOrdersUnknownLast() {
        assertTrue(BulkIndexKeyTableLookup.compareIds(0, 1) < 0);
        assertTrue(BulkIndexKeyTableLookup.compareIds(1, 0) > 0);
        assertEquals(0, BulkIndexKeyTableLookup.compareIds(2, 2));

        assertTrue(BulkIndexKeyTableLookup.compareIds(BulkIndexKeyTableLookup.UNKNOWN_ID, 0) > 0);
        assertTrue(BulkIndexKeyTableLookup.compareIds(0, BulkIndexKeyTableLookup.UNKNOWN_ID) < 0);
        assertEquals(0, BulkIndexKeyTableLookup.compareIds(BulkIndexKeyTableLookup.UNKNOWN_ID, BulkIndexKeyTableLookup.UNKNOWN_ID));
    }

    @Test
    public void testConfigureReadsJobOutputTableNames() {
        Configuration conf = new Configuration();
        TableConfigurationUtil.addOutputTables(String.join(",", TABLES), conf);

        BulkIndexKeyTableLookup.configure(conf);

        assertEquals(4, BulkIndexKeyTableLookup.get().size());
        assertEquals(1, BulkIndexKeyTableLookup.get().idFor(new Text("shard")));
        assertSame(conf, BulkIndexKeyTableLookup.get().getConf());
    }

    @Test
    public void testConfigureIsIdempotentForOneJob() {
        Configuration conf = new Configuration();
        TableConfigurationUtil.addOutputTables(String.join(",", TABLES), conf);

        BulkIndexKeyTableLookup.configure(conf);
        BulkIndexKeyTableLookup first = BulkIndexKeyTableLookup.get();

        // a second, equivalent configuration must not rebuild - the comparator hook fires more than once per JVM
        BulkIndexKeyTableLookup.configure(new Configuration(conf));
        assertSame(first, BulkIndexKeyTableLookup.get());
    }

    @Test
    public void testConfigureCanBeDisabled() {
        Configuration conf = new Configuration();
        TableConfigurationUtil.addOutputTables(String.join(",", TABLES), conf);
        conf.setBoolean(BulkIndexKeyTableLookup.DICTIONARY_ENABLED, false);

        BulkIndexKeyTableLookup.configure(conf);

        assertEquals(0, BulkIndexKeyTableLookup.get().size());
        assertEquals(BulkIndexKeyTableLookup.UNKNOWN_ID, BulkIndexKeyTableLookup.get().idFor(new Text("shard")));
    }

    @Test
    public void testConfigureIgnoresANullConfiguration() {
        BulkIndexKeyTableLookup.install(TABLES);
        BulkIndexKeyTableLookup installed = BulkIndexKeyTableLookup.get();

        BulkIndexKeyTableLookup.configure(null);
        assertSame(installed, BulkIndexKeyTableLookup.get());
    }

    @Test
    public void testConfigureOfAJobWithNoDeclaredTablesEncodesNothing() {
        BulkIndexKeyTableLookup.install(TABLES);

        BulkIndexKeyTableLookup.configure(new Configuration());

        assertEquals(0, BulkIndexKeyTableLookup.get().size());
    }

    @Test
    public void testRoundTripWithDictionary() throws IOException {
        BulkIndexKeyTableLookup.install(TABLES);

        BulkIngestKey expected = new BulkIngestKey(new Text("shardIndex"), key("row", 1234L));
        BulkIngestKey actual = roundTrip(expected);

        assertEquals(new Text("shardIndex"), actual.getTableName());
        assertEquals(expected.getKey(), actual.getKey());
        assertEquals(0, expected.compareTo(actual));
        assertEquals(expected, actual);
        assertEquals(expected.hashCode(), actual.hashCode());
    }

    @Test
    public void testRoundTripOfUndeclaredTableFallsBackToTheName() throws IOException {
        BulkIndexKeyTableLookup.install(TABLES);

        BulkIngestKey expected = new BulkIngestKey(new Text("someOtherTable"), key("row", 1234L));
        BulkIngestKey actual = roundTrip(expected);

        assertEquals(new Text("someOtherTable"), actual.getTableName());
        assertEquals(0, expected.compareTo(actual));
    }

    @Test
    public void testDeserializedTableNameIsNotTheDictionaryInstance() throws IOException {
        BulkIndexKeyTableLookup dictionary = BulkIndexKeyTableLookup.install(TABLES);

        BulkIngestKey read = roundTrip(new BulkIngestKey(new Text("shard"), key("row", 1234L)));

        // setTableName writes through the Text it holds, so a key must never hold the dictionary's copy
        read.setTableName(new Text("shardIndex"));
        assertEquals(new Text("shard"), dictionary.nameFor(1));
        assertEquals(2, read.getTableId());
    }

    @Test
    public void testEncodingShrinksTheRecord() throws IOException {
        String table = "shardReverseIndex";
        int inline = serialize(new BulkIngestKey(new Text(table), key("row", 1234L))).length;

        BulkIndexKeyTableLookup.install(TABLES);
        int encoded = serialize(new BulkIngestKey(new Text(table), key("row", 1234L))).length;

        // the inline form spends the escape id, a length prefix, and the name; the encoded form spends one id byte
        assertEquals(inline - (1 + table.length()), encoded);
    }

    @Test
    public void testUnresolvableIdFailsLoudly() throws IOException {
        BulkIndexKeyTableLookup.install(TABLES);
        byte[] bytes = serialize(new BulkIngestKey(new Text("shardReverseIndex"), key("row", 1234L)));

        // a reader whose dictionary disagrees with the writer's must not silently invent a table name
        BulkIndexKeyTableLookup.install(Collections.singletonList("shard"));
        IOException e = assertThrows(IOException.class, () -> deserialize(bytes));
        assertTrue(e.getMessage().contains(TableConfigurationUtil.JOB_OUTPUT_TABLE_NAMES), e.getMessage());
    }

    @Test
    public void testSetTableNameReresolvesTheId() {
        BulkIndexKeyTableLookup.install(TABLES);

        BulkIngestKey bik = new BulkIngestKey(new Text("shard"), key("row", 1234L));
        assertEquals(1, bik.getTableId());

        bik.setTableName(new Text("shardReverseIndex"));
        assertEquals(3, bik.getTableId());

        bik.setTableName(new Text("undeclared"));
        assertEquals(BulkIndexKeyTableLookup.UNKNOWN_ID, bik.getTableId());
    }

    /**
     * The claim the encoding rests on: assigning ids in table name order leaves the total order of a job whose tables are all declared exactly as it was when
     * the name was written inline. Verified over a population spanning every declared table, both through the object comparator and through the raw one.
     *
     * @throws IOException
     *             if a key cannot be serialized
     */
    @Test
    public void testDictionaryPreservesTheInlineSortOrder() throws IOException {
        List<BulkIngestKey> inlineOrder = population(TABLES);
        Collections.sort(inlineOrder);
        List<String> inlineRendering = render(inlineOrder);
        assertOrderingsAgree(inlineOrder);

        BulkIndexKeyTableLookup.install(TABLES);
        List<BulkIngestKey> encodedOrder = population(TABLES);
        Collections.sort(encodedOrder);
        assertEquals(inlineRendering, render(encodedOrder));
        assertOrderingsAgree(encodedOrder);
    }

    /**
     * The same check where only some of the tables are declared, so that ids and inline names are mixed within one sort. The order is no longer the inline
     * order - undeclared tables move to the end - but it must still be a total order on which the object and raw comparators agree.
     *
     * @throws IOException
     *             if a key cannot be serialized
     */
    @Test
    public void testMixedKnownAndUnknownTablesStillOrderConsistently() throws IOException {
        BulkIndexKeyTableLookup.install(Arrays.asList("shard", "shardIndex"));

        List<String> tables = new ArrayList<>(TABLES);
        tables.add("undeclaredOne");
        tables.add("aardvark");

        List<BulkIngestKey> keys = population(tables);
        Collections.sort(keys);
        assertOrderingsAgree(keys);

        // every declared table precedes every undeclared one
        int lastDeclared = -1;
        int firstUndeclared = keys.size();
        for (int i = 0; i < keys.size(); i++) {
            if (keys.get(i).getTableId() < 0) {
                firstUndeclared = Math.min(firstUndeclared, i);
            } else {
                lastDeclared = Math.max(lastDeclared, i);
            }
        }
        assertTrue(lastDeclared < firstUndeclared, "declared tables must sort before undeclared ones");
    }

    /**
     * Assert that the raw comparator agrees in sign with the object comparator for every pair in the list, which is the invariant that keeps the framework's
     * sort of serialized bytes coherent with the reducer's grouping of deserialized keys.
     *
     * @param keys
     *            the keys to compare pairwise
     * @throws IOException
     *             if a key cannot be serialized
     */
    private void assertOrderingsAgree(List<BulkIngestKey> keys) throws IOException {
        BulkIngestKey.Comparator comparator = new BulkIngestKey.Comparator();
        byte[][] bytes = new byte[keys.size()][];
        for (int i = 0; i < keys.size(); i++) {
            bytes[i] = serialize(keys.get(i));
        }

        for (int i = 0; i < keys.size(); i++) {
            for (int j = 0; j < keys.size(); j++) {
                int object = keys.get(i).compareTo(keys.get(j));
                int raw = comparator.compare(bytes[i], 0, bytes[i].length, bytes[j], 0, bytes[j].length);
                assertEquals(Integer.signum(object), Integer.signum(raw), "raw and object comparators disagree on " + keys.get(i) + " vs " + keys.get(j));
            }
        }
    }

    /**
     * A fresh population of keys spread over the given tables. Freshly built rather than reused across a test's two sorts, because a key caches the id it
     * resolved and the point of those tests is to sort the same keys under two different dictionaries.
     *
     * @param tables
     *            the tables to spread the keys over
     * @return the population, in a mutable list
     */
    private List<BulkIngestKey> population(List<String> tables) {
        Random rand = new Random(42);
        List<BulkIngestKey> keys = new ArrayList<>();
        for (String table : tables) {
            for (int i = 0; i < 6; i++) {
                keys.add(new BulkIngestKey(new Text(table), key("row" + rand.nextInt(4), rand.nextInt(3))));
            }
        }
        return keys;
    }

    private List<String> render(List<BulkIngestKey> keys) {
        List<String> rendered = new ArrayList<>(keys.size());
        for (BulkIngestKey bik : keys) {
            rendered.add(bik.getTableName() + " " + bik.getKey());
        }
        return rendered;
    }

    private static Key key(String row, long timestamp) {
        return new Key(new Text(row), new Text("colFam"), new Text("col\0qual"), new Text("col\0vis"), timestamp);
    }

    private static byte[] serialize(BulkIngestKey bik) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(bos)) {
            bik.write(dos);
        }
        return bos.toByteArray();
    }

    private static BulkIngestKey deserialize(byte[] bytes) throws IOException {
        BulkIngestKey bik = new BulkIngestKey();
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
            bik.readFields(dis);
        }
        return bik;
    }

    private static BulkIngestKey roundTrip(BulkIngestKey bik) throws IOException {
        BulkIngestKey read = deserialize(serialize(bik));
        assertNotNull(read.getTableName());
        return read;
    }
}
