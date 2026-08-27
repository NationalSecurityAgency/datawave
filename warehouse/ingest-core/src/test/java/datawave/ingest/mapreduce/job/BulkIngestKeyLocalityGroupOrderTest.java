package datawave.ingest.mapreduce.job;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises the locality-group-aware sort order added to {@link BulkIngestKey}: {@code table, lgOrdinal, row, cf, cq, cv, ts desc, deleted}. Named
 * {@code OrderTest} rather than {@code ComparatorTest} so it does not collide with the separate comparator-optimization branch's test of similar shape (see the
 * integration plan, section 3, item 2).
 * <p>
 * A locality-group lookup is installed for a shard-like table ({@code fullcontent -> d}, {@code termfrequency -> tf}) before every test via
 * {@link BulkIngestKeyLocalityGroupLookup#install(Map)}; a second table name ("other") is deliberately left out of that installed map so it is not enabled.
 */
public class BulkIngestKeyLocalityGroupOrderTest {

    private static final String SHARD = "shard";
    private static final String OTHER = "other";

    @BeforeEach
    public void installLookup() {
        Map<String,Set<Text>> shardGroups = new HashMap<>();
        shardGroups.put("fullcontent", Set.of(new Text("d")));
        shardGroups.put("termfrequency", Set.of(new Text("tf")));
        BulkIngestKeyLocalityGroupLookup.install(Map.of(SHARD, shardGroups));
    }

    @AfterEach
    public void resetLookup() {
        BulkIngestKeyLocalityGroupLookup.reset();
    }

    @Test
    public void testTermFrequencyKeySortsBeforeDefaultKeyInLaterRow() throws IOException {
        BulkIngestKey tfInR2 = bik(SHARD, "r2", "tf");
        BulkIngestKey defaultInR1 = bik(SHARD, "r1", "fi\0F");

        assertEquals(1, tfInR2.getLocalityGroupOrdinal());
        assertEquals(2, defaultInR1.getLocalityGroupOrdinal());

        assertTrue(tfInR2.compareTo(defaultInR1) < 0,
                        "termfrequency (ordinal 1) in a later row must sort before the default group (ordinal 2) in an earlier row -- LG sorts before row");
        assertTrue(compareBytes(tfInR2, defaultInR1) < 0);
    }

    @Test
    public void testNamedGroupsOrderedByOrdinalThenDefaultLast() throws IOException {
        BulkIngestKey fullcontent = bik(SHARD, "r1", "d");
        BulkIngestKey termfrequency = bik(SHARD, "r1", "tf");
        BulkIngestKey defaultFi = bik(SHARD, "r1", "fi\0F");
        BulkIngestKey defaultDt = bik(SHARD, "r1", "dt\0uid");

        assertEquals(0, fullcontent.getLocalityGroupOrdinal());
        assertEquals(1, termfrequency.getLocalityGroupOrdinal());
        assertEquals(2, defaultFi.getLocalityGroupOrdinal());
        assertEquals(2, defaultDt.getLocalityGroupOrdinal());

        assertTrue(fullcontent.compareTo(termfrequency) < 0);
        assertTrue(termfrequency.compareTo(defaultFi) < 0);
        assertTrue(termfrequency.compareTo(defaultDt) < 0);
        assertTrue(fullcontent.compareTo(defaultFi) < 0);
        assertTrue(fullcontent.compareTo(defaultDt) < 0);

        assertTrue(compareBytes(fullcontent, termfrequency) < 0);
        assertTrue(compareBytes(termfrequency, defaultFi) < 0);
        assertTrue(compareBytes(fullcontent, defaultDt) < 0);
    }

    @Test
    public void testNonEnabledTableIgnoresLocalityGroup() throws IOException {
        BulkIngestKey d = bik(OTHER, "r1", "d");
        BulkIngestKey tf = bik(OTHER, "r1", "tf");
        BulkIngestKey fi = bik(OTHER, "r1", "fi\0F");
        BulkIngestKey dt = bik(OTHER, "r1", "dt\0uid");

        assertTrue(BulkIngestKeyLocalityGroupLookup.get().isEnabled(new Text(SHARD)));
        assertEquals(0, d.getLocalityGroupOrdinal());
        assertEquals(0, tf.getLocalityGroupOrdinal());
        assertEquals(0, fi.getLocalityGroupOrdinal());
        assertEquals(0, dt.getLocalityGroupOrdinal());

        // every ordinal is 0 for a non-enabled table, so order collapses to plain Key order
        assertEquals(sign(d.getKey().compareTo(tf.getKey())), sign(d.compareTo(tf)));
        assertEquals(sign(d.getKey().compareTo(fi.getKey())), sign(d.compareTo(fi)));
        assertEquals(sign(fi.getKey().compareTo(dt.getKey())), sign(fi.compareTo(dt)));
        assertEquals(sign(tf.getKey().compareTo(dt.getKey())), sign(tf.compareTo(dt)));

        assertEquals(sign(d.compareTo(tf)), sign(compareBytes(d, tf)));
        assertEquals(sign(fi.compareTo(dt)), sign(compareBytes(fi, dt)));
    }

    @Test
    public void testWriteReadFieldsAdoptsOrdinalAndRewriteIsByteIdentical() throws IOException {
        BulkIngestKey original = bik(SHARD, "r7", "tf", "cq1", "cv1", 12345L, true);
        assertEquals(1, original.getLocalityGroupOrdinal());

        byte[] firstBytes = serialize(original);
        BulkIngestKey roundTripped = deserialize(firstBytes);

        // adopted from the stream rather than recomputed -- happens to agree here because it's the same lookup
        assertEquals(1, roundTripped.getLocalityGroupOrdinal());
        assertEquals(0, original.compareTo(roundTripped));

        byte[] secondBytes = serialize(roundTripped);
        assertArrayEquals(firstBytes, secondBytes);
    }

    @Test
    public void testEveryOrderedPairAgreesBetweenComparatorAndCompareTo() throws IOException {
        List<BulkIngestKey> population = buildPopulation();

        for (BulkIngestKey a : population) {
            for (BulkIngestKey b : population) {
                int objectSign = sign(a.compareTo(b));
                int byteSign = sign(compareBytes(a, b));
                assertEquals(objectSign, byteSign, () -> "Comparator.compare disagreed with compareTo comparing " + a + " to " + b);

                // antisymmetry
                assertEquals(-objectSign, sign(b.compareTo(a)), () -> "compareTo is not antisymmetric for " + a + " / " + b);
                assertEquals(-byteSign, sign(compareBytes(b, a)), () -> "Comparator.compare is not antisymmetric for " + a + " / " + b);
            }
        }
    }

    @Test
    public void testEmptyLookupCollapsesToPlainTableAndKeyOrder() throws IOException {
        BulkIngestKeyLocalityGroupLookup.reset();
        assertTrue(BulkIngestKeyLocalityGroupLookup.get().isEmpty());

        List<BulkIngestKey> population = buildPopulation();
        for (BulkIngestKey key : population) {
            assertEquals(0, key.getLocalityGroupOrdinal());
        }

        for (BulkIngestKey a : population) {
            for (BulkIngestKey b : population) {
                int expected = sign(plainCompare(a, b));
                assertEquals(expected, sign(a.compareTo(b)));
                assertEquals(expected, sign(compareBytes(a, b)));
            }
        }
    }

    // ------------------------------------------------------------------------------------------------------------------------------------------

    /**
     * With 128 or more named groups the ordinal no longer fits in a single vint byte; the raw comparator must decode it fully and stay consistent with
     * {@link BulkIngestKey#compareTo(BulkIngestKey)}.
     */
    @Test
    public void testRawComparatorHandlesMultiByteOrdinal() throws IOException {
        String table = "wide";
        Map<String,Set<Text>> groups = new HashMap<>();
        for (int i = 0; i < 130; i++) {
            groups.put(String.format("g%03d", i), Set.of(new Text("cf" + i)));
        }
        BulkIngestKeyLocalityGroupLookup.install(Map.of(table, groups));

        BulkIngestKey ord126 = bik(table, "r9", "cf126");
        BulkIngestKey ord127 = bik(table, "r9", "cf127");
        BulkIngestKey ord128 = bik(table, "r1", "cf128");
        BulkIngestKey ord129 = bik(table, "r1", "cf129");
        BulkIngestKey dflt = bik(table, "r0", "zz");

        assertEquals(126, ord126.getLocalityGroupOrdinal());
        assertEquals(127, ord127.getLocalityGroupOrdinal());
        assertEquals(128, ord128.getLocalityGroupOrdinal());
        assertEquals(129, ord129.getLocalityGroupOrdinal());
        assertEquals(130, dflt.getLocalityGroupOrdinal());

        List<BulkIngestKey> keys = List.of(ord126, ord127, ord128, ord129, dflt);
        for (BulkIngestKey a : keys) {
            for (BulkIngestKey b : keys) {
                assertEquals(Integer.signum(a.compareTo(b)), Integer.signum(compareBytes(a, b)),
                                "raw comparator disagrees with compareTo for ordinals " + a.getLocalityGroupOrdinal() + " vs " + b.getLocalityGroupOrdinal());
            }
        }
        assertTrue(compareBytes(ord127, ord128) < 0);
        assertTrue(compareBytes(ord129, dflt) < 0);
    }

    private static BulkIngestKey bik(String table, String row, String cf) {
        return bik(table, row, cf, "cq", "cv", 0L, false);
    }

    private static BulkIngestKey bik(String table, String row, String cf, String cq, String cv, long ts, boolean deleted) {
        Key key = new Key(new Text(row), new Text(cf), new Text(cq), new Text(cv), ts);
        key.setDeleted(deleted);
        return new BulkIngestKey(new Text(table), key);
    }

    private static byte[] serialize(BulkIngestKey key) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(bos)) {
            key.write(dos);
        }
        return bos.toByteArray();
    }

    private static BulkIngestKey deserialize(byte[] bytes) throws IOException {
        BulkIngestKey key = new BulkIngestKey();
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
            key.readFields(dis);
        }
        return key;
    }

    private static int compareBytes(BulkIngestKey a, BulkIngestKey b) throws IOException {
        byte[] ab = serialize(a);
        byte[] bb = serialize(b);
        BulkIngestKey.Comparator comparator = new BulkIngestKey.Comparator();
        return comparator.compare(ab, 0, ab.length, bb, 0, bb.length);
    }

    /** Table then key order, exactly as {@code BulkIngestKey#compareTo} behaved before locality-group awareness was added. */
    private static int plainCompare(BulkIngestKey a, BulkIngestKey b) {
        int result = a.getTableName().compareTo(b.getTableName());
        if (result == 0) {
            result = a.getKey().compareTo(b.getKey());
        }
        return result;
    }

    private static int sign(int value) {
        return Integer.compare(value, 0);
    }

    /** {@code d}/{@code tf}/{@code fi\0F}/{@code dt\0uid} column families across the enabled ("shard") and non-enabled ("other") tables. */
    private static List<BulkIngestKey> buildPopulation() {
        List<BulkIngestKey> population = new ArrayList<>();
        String[] tables = {SHARD, OTHER};
        String[] cfs = {"d", "tf", "fi\0F", "dt\0uid"};
        String[] rows = {"r1", "r2"};
        String[] cqs = {"cq1", "cq2"};
        long[] timestamps = {100L, 200L};
        boolean[] deletedFlags = {false, true};

        for (String table : tables) {
            for (String row : rows) {
                for (String cf : cfs) {
                    for (String cq : cqs) {
                        for (long ts : timestamps) {
                            for (boolean deleted : deletedFlags) {
                                population.add(bik(table, row, cf, cq, "cv", ts, deleted));
                            }
                        }
                    }
                }
            }
        }
        return population;
    }
}
