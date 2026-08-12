package datawave.ingest.mapreduce.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Covers {@link BulkIngestKey.Comparator}, the raw comparator MapReduce sorts serialized bulk ingest map output with.
 * <p>
 * The comparator does not walk the record component by component: the serialized layout groups the four component lengths into a header ahead of the data, and
 * the comparator uses that header to settle the components with one {@code Arrays.mismatch} over the bytes the two records lay out identically, falling back to
 * a single {@code compareBytes} on the first component whose lengths disagree. Every branch of that decision is reachable from the shape of a key alone, so the
 * tests below name the shape they are aimed at rather than the line they cover:
 *
 * <ul>
 * <li>records whose headers agree, deciding inside each of the four components in turn - the fused scan;</li>
 * <li>records whose headers disagree at each component index in turn, including the case where one component is a proper prefix of the other and only the
 * length difference separates them;</li>
 * <li>components long enough that a header entry needs more than one vint byte;</li>
 * <li>component bytes above {@code 0x7f}, which must compare unsigned, as {@code Text} and {@code Key} do;</li>
 * <li>records on tables the dictionary does not know, whose names are written inline ahead of the header;</li>
 * <li>equal components decided by the timestamp, which sorts in reverse, and by the deleted flag, which sorts deleted first.</li>
 * </ul>
 *
 * {@link #agreesWithTheObjectComparatorOnEveryOrderedPair()} then requires the raw comparator to agree in sign with
 * {@link BulkIngestKey#compareTo(BulkIngestKey)} over every ordered pair of a population built from all of those shapes, which is the invariant the framework
 * depends on: the sort of the serialized bytes and the reducer's grouping of the deserialized keys must be the same order.
 * <p>
 * The dictionary is JVM wide state, so every test restores {@link BulkIndexKeyTableLookup#EMPTY} in teardown.
 */
public class BulkIngestKeyComparatorTest {

    /** the tables the dictionary knows; anything else is written with its name inline */
    private static final List<String> TABLES = Arrays.asList("shard", "shardIndex");

    private static final Text SHARD = new Text("shard");

    private static final long TS = 1785110400000L;

    private final BulkIngestKey.Comparator comparator = new BulkIngestKey.Comparator();

    @AfterEach
    public void tearDown() {
        BulkIndexKeyTableLookup.reset();
    }

    // -------------------------------------------------------------------------------------------------------------
    // equal headers: the fused scan decides
    // -------------------------------------------------------------------------------------------------------------

    @Test
    public void testEqualHeadersDecideInTheColumnQualifier() throws IOException {
        BulkIndexKeyTableLookup.install(TABLES);

        // identical lengths everywhere, so the whole data region is scanned as one aligned range and the first
        // differing byte - in the column qualifier - decides
        BulkIngestKey lesser = bik(SHARD, key("20260727_7", "fi\0COLOR", "blue\0csvtype\0uid", "PUBLIC", TS));
        BulkIngestKey greater = bik(SHARD, key("20260727_7", "fi\0COLOR", "blue\0csvtype\0uie", "PUBLIC", TS));

        assertOrder(lesser, greater);
    }

    @Test
    public void testEqualHeadersDecideInEachComponent() throws IOException {
        BulkIndexKeyTableLookup.install(TABLES);

        BulkIngestKey base = bik(SHARD, key("20260727_7", "fi\0COLOR", "blue\0csvtype", "PUBLIC", TS));

        // one component at a time, same length, one byte later in the alphabet
        assertOrder(base, bik(SHARD, key("20260727_8", "fi\0COLOR", "blue\0csvtype", "PUBLIC", TS)));
        assertOrder(base, bik(SHARD, key("20260727_7", "fi\0COLOS", "blue\0csvtype", "PUBLIC", TS)));
        assertOrder(base, bik(SHARD, key("20260727_7", "fi\0COLOR", "blue\0csvtypf", "PUBLIC", TS)));
        assertOrder(base, bik(SHARD, key("20260727_7", "fi\0COLOR", "blue\0csvtype", "PUBLID", TS)));
    }

    // -------------------------------------------------------------------------------------------------------------
    // headers that disagree: the first length-differing component decides, and nothing after it is examined
    // -------------------------------------------------------------------------------------------------------------

    @Test
    public void testLengthMismatchInEachComponent() throws IOException {
        BulkIndexKeyTableLookup.install(TABLES);

        BulkIngestKey base = bik(SHARD, key("20260727_7", "fi\0COLOR", "blue\0csvtype", "PUBLIC", TS));

        // a longer component whose extra byte is what separates them - the length differs at index 0, 1, 2, 3 in turn
        assertOrder(base, bik(SHARD, key("20260727_70", "fi\0COLOR", "blue\0csvtype", "PUBLIC", TS)));
        assertOrder(base, bik(SHARD, key("20260727_7", "fi\0COLORS", "blue\0csvtype", "PUBLIC", TS)));
        assertOrder(base, bik(SHARD, key("20260727_7", "fi\0COLOR", "blue\0csvtypes", "PUBLIC", TS)));
        assertOrder(base, bik(SHARD, key("20260727_7", "fi\0COLOR", "blue\0csvtype", "PUBLICS", TS)));
    }

    /**
     * The case the header shortcut has to get right: the two column families differ only in that one is a proper prefix of the other, so no byte separates them
     * and the length difference alone decides. Components after the column family are deliberately made to sort the other way, to prove that the comparator
     * stops at the first length-differing component instead of continuing past it.
     *
     * @throws IOException
     *             if a key cannot be serialized
     */
    @Test
    public void testProperPrefixComponentIsDecidedByItsLength() throws IOException {
        BulkIndexKeyTableLookup.install(TABLES);

        BulkIngestKey shorter = bik(SHARD, key("20260727_7", "fi\0COLOR", "zzzz", "PUBLIC", TS));
        BulkIngestKey longer = bik(SHARD, key("20260727_7", "fi\0COLORX", "aaaa", "PUBLIC", TS));

        assertOrder(shorter, longer);
    }

    @Test
    public void testHeaderEntriesLongerThanOneVintByte() throws IOException {
        BulkIndexKeyTableLookup.install(TABLES);

        // 127 is the largest length a single vint byte holds; 128 and 300 need the multi byte form, so these pairs
        // exercise the header decoder's slow path on both sides of the boundary
        assertOrder(bik(SHARD, key("20260727_7", repeat('a', 127), "cq", "PUBLIC", TS)), bik(SHARD, key("20260727_7", repeat('a', 128), "cq", "PUBLIC", TS)));
        assertOrder(bik(SHARD, key("20260727_7", repeat('a', 300), "cq", "PUBLIC", TS)),
                        bik(SHARD, key("20260727_7", repeat('a', 299) + "b", "cq", "PUBLIC", TS)));
        assertOrder(bik(SHARD, key(repeat('a', 200), "cf", "cq", "PUBLIC", TS)), bik(SHARD, key(repeat('a', 201), "cf", "cq", "PUBLIC", TS)));
    }

    // -------------------------------------------------------------------------------------------------------------
    // unsigned byte order
    // -------------------------------------------------------------------------------------------------------------

    /**
     * A byte above {@code 0x7f} is negative as a Java {@code byte} and must still sort after every byte below it, which is how {@code Text}, {@code Key}, and
     * {@code WritableComparator.compareBytes} all order. Both the fused scan's own byte difference and the {@code compareBytes} fallback have to agree with
     * that.
     *
     * @throws IOException
     *             if a key cannot be serialized
     */
    @Test
    public void testComponentBytesAboveSevenBitsCompareUnsigned() throws IOException {
        BulkIndexKeyTableLookup.install(TABLES);

        // equal lengths: the fused scan stops on the differing byte itself
        assertOrder(bik(SHARD, keyBytes(bytes(0x20, 0x41), bytes(0x7f), bytes(0x01), bytes(0x50))),
                        bik(SHARD, keyBytes(bytes(0x20, 0x41), bytes(0x80), bytes(0x01), bytes(0x50))));
        assertOrder(bik(SHARD, keyBytes(bytes(0x20, 0x41), bytes((byte) 0x80), bytes(0x01), bytes(0x50))),
                        bik(SHARD, keyBytes(bytes(0x20, 0x41), bytes((byte) 0xff), bytes(0x01), bytes(0x50))));

        // unequal lengths: the compareBytes fallback on the first length-differing component
        assertOrder(bik(SHARD, keyBytes(bytes(0x20), bytes(0x7f, 0x01), bytes(0x01), bytes(0x50))),
                        bik(SHARD, keyBytes(bytes(0x20), bytes(0x80), bytes(0x01), bytes(0x50))));
    }

    // -------------------------------------------------------------------------------------------------------------
    // the table component, in both its forms
    // -------------------------------------------------------------------------------------------------------------

    /**
     * Records on a table the dictionary does not know carry their name inline between the id and the header, so the header decode has to start after a name of
     * whatever length that record happened to carry.
     *
     * @throws IOException
     *             if a key cannot be serialized
     */
    @Test
    public void testInlineTableNamesOrderByNameAndThenByKey() throws IOException {
        BulkIndexKeyTableLookup.install(TABLES);

        BulkIngestKey aardvark = bik(new Text("aardvark"), key("20260727_7", "cf", "cq", "PUBLIC", TS));
        BulkIngestKey zebraLongTableName = bik(new Text("zebraWithAMuchLongerName"), key("20260727_7", "cf", "cq", "PUBLIC", TS));

        // two undeclared tables break their id tie on the inline names
        assertOrder(aardvark, zebraLongTableName);

        // and two records on one undeclared table are still separated by the key that follows the name
        assertOrder(bik(new Text("aardvark"), key("20260727_7", "cf", "cq", "PUBLIC", TS)),
                        bik(new Text("aardvark"), key("20260727_7", "cf", "cqq", "PUBLIC", TS)));

        // every declared table sorts before every undeclared one, whatever the names would say
        assertOrder(bik(SHARD, key("zzzzzzzz", "cf", "cq", "PUBLIC", TS)), aardvark);
    }

    // -------------------------------------------------------------------------------------------------------------
    // the tail: only reached when all four components are equal
    // -------------------------------------------------------------------------------------------------------------

    @Test
    public void testTimestampSortsInReverse() throws IOException {
        BulkIndexKeyTableLookup.install(TABLES);

        // the later timestamp sorts first, as Accumulo orders versions
        assertOrder(bik(SHARD, key("20260727_7", "cf", "cq", "PUBLIC", TS + 1)), bik(SHARD, key("20260727_7", "cf", "cq", "PUBLIC", TS)));
    }

    @Test
    public void testDeletedSortsBeforeNonDeleted() throws IOException {
        BulkIndexKeyTableLookup.install(TABLES);

        Key deleted = key("20260727_7", "cf", "cq", "PUBLIC", TS);
        deleted.setDeleted(true);

        assertOrder(bik(SHARD, deleted), bik(SHARD, key("20260727_7", "cf", "cq", "PUBLIC", TS)));
    }

    @Test
    public void testIdenticalRecordsCompareEqual() throws IOException {
        BulkIndexKeyTableLookup.install(TABLES);

        byte[] record = serialize(bik(SHARD, key("20260727_7", "fi\0COLOR", "blue\0csvtype", "PUBLIC", TS)));

        assertEquals(0, comparator.compare(record, 0, record.length, record, 0, record.length));
    }

    // -------------------------------------------------------------------------------------------------------------
    // the whole population, both comparators
    // -------------------------------------------------------------------------------------------------------------

    /**
     * The invariant the framework rests on, checked exhaustively over a population spanning every shape above: the raw comparator must order serialized records
     * exactly as {@link BulkIngestKey#compareTo(BulkIngestKey)} orders the objects, and must be antisymmetric in its own right - a comparator that agreed with
     * a broken object comparator would pass the first check alone.
     *
     * @throws IOException
     *             if a key cannot be serialized
     */
    @Test
    public void agreesWithTheObjectComparatorOnEveryOrderedPair() throws IOException {
        BulkIndexKeyTableLookup.install(TABLES);

        List<BulkIngestKey> keys = population();
        byte[][] records = new byte[keys.size()][];
        for (int i = 0; i < keys.size(); i++) {
            records[i] = serialize(keys.get(i));
        }

        for (int i = 0; i < keys.size(); i++) {
            for (int j = 0; j < keys.size(); j++) {
                int object = Integer.signum(keys.get(i).compareTo(keys.get(j)));
                int raw = Integer.signum(comparator.compare(records[i], 0, records[i].length, records[j], 0, records[j].length));
                assertEquals(object, raw, "raw and object comparators disagree on " + keys.get(i) + " vs " + keys.get(j));

                int reverse = Integer.signum(comparator.compare(records[j], 0, records[j].length, records[i], 0, records[i].length));
                assertEquals(-raw, reverse, "raw comparator is not antisymmetric on " + keys.get(i) + " vs " + keys.get(j));
            }
        }
    }

    /**
     * Every record in the population must survive {@code write}/{@code readFields} with all seven of its fields intact, so that a layout change can never be
     * declared correct on the strength of its ordering alone.
     *
     * @throws IOException
     *             if a key cannot be serialized
     */
    @Test
    public void testEveryShapeRoundTrips() throws IOException {
        BulkIndexKeyTableLookup.install(TABLES);

        for (BulkIngestKey expected : population()) {
            BulkIngestKey actual = deserialize(serialize(expected));

            assertEquals(expected.getTableName(), actual.getTableName());
            assertEquals(expected.getKey().getRow(), actual.getKey().getRow());
            assertEquals(expected.getKey().getColumnFamily(), actual.getKey().getColumnFamily());
            assertEquals(expected.getKey().getColumnQualifier(), actual.getKey().getColumnQualifier());
            assertEquals(expected.getKey().getColumnVisibility(), actual.getKey().getColumnVisibility());
            assertEquals(expected.getKey().getTimestamp(), actual.getKey().getTimestamp());
            assertEquals(expected.getKey().isDeleted(), actual.getKey().isDeleted());
        }
    }

    /**
     * A population covering every shape this class tests individually: equal and differing headers at each component, proper prefixes, multi byte header
     * entries, high bytes, both table encodings, timestamp twins, exact duplicates, and deleted twins.
     *
     * @return the keys, in no particular order
     */
    private List<BulkIngestKey> population() {
        List<BulkIngestKey> keys = new ArrayList<>();

        String[] rows = {"20260727_7", "20260727_70", "20260727_8"};
        String[] families = {"fi\0COLOR", "fi\0COLORX", "csvtype\0abc.def.ghi"};
        String[] qualifiers = {"blue", "blue\0csvtype", "bluf"};
        String[] visibilities = {"PUBLIC", "PUBLIC|(PRIVATE&ORG)"};

        for (String row : rows) {
            for (String cf : families) {
                for (String cq : qualifiers) {
                    for (String cv : visibilities) {
                        keys.add(bik(SHARD, key(row, cf, cq, cv, TS)));
                    }
                }
            }
        }

        // the same shapes on the other declared table and on two the dictionary does not know
        for (Text table : new Text[] {new Text("shardIndex"), new Text("aardvark"), new Text("zebraWithAMuchLongerName")}) {
            for (String cf : families) {
                keys.add(bik(table, key("20260727_7", cf, "blue", "PUBLIC", TS)));
                keys.add(bik(table, key("20260727_7", cf, "blue\0csvtype", "PUBLIC", TS)));
            }
        }

        // components that need more than one vint byte in the header
        keys.add(bik(SHARD, key("20260727_7", repeat('a', 127), "cq", "PUBLIC", TS)));
        keys.add(bik(SHARD, key("20260727_7", repeat('a', 128), "cq", "PUBLIC", TS)));
        keys.add(bik(SHARD, key("20260727_7", repeat('a', 300), "cq", "PUBLIC", TS)));
        keys.add(bik(SHARD, key(repeat('a', 200), "cf", "cq", "PUBLIC", TS)));

        // bytes above 0x7f, which must sort after every byte below
        keys.add(bik(SHARD, keyBytes(bytes(0x20, 0x41), bytes(0x7f), bytes(0x01), bytes(0x50))));
        keys.add(bik(SHARD, keyBytes(bytes(0x20, 0x41), bytes(0x80), bytes(0x01), bytes(0x50))));
        keys.add(bik(SHARD, keyBytes(bytes(0x20, 0x41), bytes(0xff), bytes(0x01), bytes(0x50))));
        keys.add(bik(SHARD, keyBytes(bytes(0x20), bytes(0x7f, 0x01), bytes(0x01), bytes(0x50))));

        // timestamp twins, exact duplicates, and deleted twins of everything built so far
        List<BulkIngestKey> tails = new ArrayList<>();
        for (int i = 0; i < keys.size(); i += 7) {
            BulkIngestKey source = keys.get(i);
            Key key = source.getKey();
            tails.add(new BulkIngestKey(source.getTableName(),
                            new Key(key.getRow(), key.getColumnFamily(), key.getColumnQualifier(), key.getColumnVisibility(), key.getTimestamp() + 1)));
            tails.add(new BulkIngestKey(source.getTableName(), new Key(key)));

            Key deleted = new Key(key);
            deleted.setDeleted(true);
            tails.add(new BulkIngestKey(source.getTableName(), deleted));
        }
        keys.addAll(tails);

        return keys;
    }

    /**
     * Assert that the raw comparator places the first record strictly before the second, and the second strictly after the first, and that the object
     * comparator says the same.
     *
     * @param lesser
     *            the record that must sort first
     * @param greater
     *            the record that must sort second
     * @throws IOException
     *             if a key cannot be serialized
     */
    private void assertOrder(BulkIngestKey lesser, BulkIngestKey greater) throws IOException {
        byte[] left = serialize(lesser);
        byte[] right = serialize(greater);

        String pair = lesser + " vs " + greater;
        assertTrue(comparator.compare(left, 0, left.length, right, 0, right.length) < 0, pair);
        assertTrue(comparator.compare(right, 0, right.length, left, 0, left.length) > 0, pair);
        assertTrue(lesser.compareTo(greater) < 0, "the object comparator disagrees on " + pair);
    }

    private static BulkIngestKey bik(Text tableName, Key key) {
        // a fresh Text per key: setTableName writes through, and the id a key caches is resolved from the name it holds
        return new BulkIngestKey(new Text(tableName), key);
    }

    private static Key key(String row, String cf, String cq, String cv, long ts) {
        return new Key(new Text(row), new Text(cf), new Text(cq), new Text(cv), ts);
    }

    private static Key keyBytes(byte[] row, byte[] cf, byte[] cq, byte[] cv) {
        return new Key(row, cf, cq, cv, TS, false, false);
    }

    private static byte[] bytes(int... values) {
        byte[] result = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            result[i] = (byte) values[i];
        }
        return result;
    }

    private static String repeat(char c, int count) {
        return String.join("", Collections.nCopies(count, String.valueOf(c)));
    }

    private static byte[] serialize(BulkIngestKey bik) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(bos)) {
            bik.write(dos);
        }
        return bos.toByteArray();
    }

    private static BulkIngestKey deserialize(byte[] record) throws IOException {
        BulkIngestKey bik = new BulkIngestKey();
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(record))) {
            bik.readFields(dis);
        }
        return bik;
    }
}
