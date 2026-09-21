package datawave.core.iterators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.junit.jupiter.api.Test;

import datawave.query.Constants;

/**
 * Tests for the {@link UnfieldedRegexExpansionIterator} covering defects that are triggered by specific data patterns.
 * <p>
 * An Accumulo iterator stack may be torn down and rebuilt at any point during a scan -- {@code SourceSwitchingIterator#readNext} does this on every
 * {@code next()} where the tablet's data sources have changed, and {@code ThriftScanner} does it whenever a scan session has to be restarted. In both cases the
 * rebuilt stack is seeked with {@code new Range(lastReturnedKey, false, ...)} and must not return that key a second time. The iterator used to override
 * {@code seek} to rewrite that exclusive start key to an inclusive one, so a rebuilt stack always re-emitted the key the client already had, and a scan torn
 * down between every pair of emitted keys never advanced at all.
 */
public class UnfieldedRegexExpansionIteratorRebuildTest {

    private static final Value EMPTY_VALUE = new Value();
    private static final String NULL_BYTE = "\u0000";

    private final SortedMap<Key,Value> data = new TreeMap<>();
    private final Map<String,String> options = new HashMap<>();

    private void withData(String value, String field, String columnQualifier) {
        data.put(new Key(value, field, columnQualifier), EMPTY_VALUE);
    }

    private void withOptions(String pattern, String startDate, String endDate) {
        options.put(UnfieldedRegexExpansionIterator.PATTERN, pattern);
        options.put(UnfieldedRegexExpansionIterator.START_DATE, startDate);
        options.put(UnfieldedRegexExpansionIterator.END_DATE, endDate);
    }

    private void withReverse() {
        options.put(UnfieldedRegexExpansionIterator.REVERSE, "true");
    }

    /**
     * Builds a fresh iterator stack and seeks it, the same way a tablet server does after tearing an iterator down.
     *
     * @param range
     *            the range to seek
     * @return the seeked iterator
     */
    private UnfieldedRegexExpansionIterator rebuild(Range range) throws Exception {
        UnfieldedRegexExpansionIterator iterator = new UnfieldedRegexExpansionIterator();
        iterator.init(new SortedMapIterator(data), options, null);
        iterator.seek(range, Collections.emptySet(), false);
        return iterator;
    }

    private String describe(Key key) {
        return key.getRow() + " " + key.getColumnFamily();
    }

    /**
     * Scans the configured data, tearing the iterator down and rebuilding it after every key returned.
     *
     * @param maxRebuilds
     *            the point at which a non-advancing scan is abandoned
     * @return every key emitted, in order, including any that were emitted more than once
     */
    private List<String> scanRebuildingAfterEveryKey(int maxRebuilds) throws Exception {
        List<String> emitted = new ArrayList<>();
        Range range = new Range();
        for (int i = 0; i < maxRebuilds; i++) {
            UnfieldedRegexExpansionIterator iterator = rebuild(range);
            if (!iterator.hasTop()) {
                break;
            }
            Key topKey = iterator.getTopKey();
            emitted.add(describe(topKey));
            range = new Range(topKey, false, null, false);
        }
        return emitted;
    }

    private List<String> scanToCompletion() throws Exception {
        UnfieldedRegexExpansionIterator iterator = rebuild(new Range());
        List<String> emitted = new ArrayList<>();
        while (iterator.hasTop()) {
            emitted.add(describe(iterator.getTopKey()));
            iterator.next();
        }
        return emitted;
    }

    /**
     * A rebuilt stack is seeked at the last returned key, exclusive. It must not hand that key back.
     */
    @Test
    public void testRebuiltStackDoesNotReemitTheLastKey() throws Exception {
        withData("aa", "FIELD_A", "20250804_0" + NULL_BYTE + "datatype-a");
        withData("ab", "FIELD_A", "20250804_0" + NULL_BYTE + "datatype-a");
        withOptions("a.*", "20250804", "20250804");

        UnfieldedRegexExpansionIterator iterator = rebuild(new Range());
        assertTrue(iterator.hasTop());
        Key lastKey = iterator.getTopKey();
        assertEquals("aa FIELD_A", describe(lastKey));

        // tear down and rebuild, exactly as SourceSwitchingIterator#readNext does
        UnfieldedRegexExpansionIterator rebuilt = rebuild(new Range(lastKey, false, null, false));
        assertTrue(rebuilt.hasTop());
        assertEquals("ab FIELD_A", describe(rebuilt.getTopKey()));
    }

    /**
     * When a rebuild lands between every emitted key the scan never advances past the first match. This is the infinite loop -- the client sits in
     * {@code for (Entry<Key,Value> entry : scanner)} forever, and by default {@code maxAnyFieldScanTimeMillis} is {@code Long.MAX_VALUE}, so nothing cancels
     * it.
     */
    @Test
    public void testScanTerminatesWhenRebuiltAfterEveryKey() throws Exception {
        withData("aa", "FIELD_A", "20250804_0" + NULL_BYTE + "datatype-a");
        withData("ab", "FIELD_A", "20250804_0" + NULL_BYTE + "datatype-a");
        withData("ac", "FIELD_A", "20250804_0" + NULL_BYTE + "datatype-a");
        withOptions("a.*", "20250804", "20250804");

        // a well behaved iterator needs at most one rebuild per key, so this bound is generous
        List<String> emitted = scanRebuildingAfterEveryKey(data.size() * 4);
        Set<String> distinct = new LinkedHashSet<>(emitted);

        assertEquals(data.size(), distinct.size(), "scan never advanced, emitted: " + emitted);
        assertEquals(distinct.size(), emitted.size(), "scan emitted duplicates: " + emitted);
    }

    /**
     * The reason the {@code seek} override could be removed. {@code ShardIndexQueryTableStaticMethods#getRegexRange} builds every range this iterator is given
     * as {@code new Range(new Key(queryTerm), false, new Key(queryTerm + MAX_UNICODE_STRING), false)} -- the start key is exclusive, but it is a bare row key
     * with an empty column family, so every real key in that row still sorts after it. Honoring the exclusive start therefore costs nothing here, while
     * rewriting it to inclusive is what let a rebuilt stack re-emit the last returned key.
     */
    @Test
    public void testExclusiveStartKeyFromGetRegexRangeKeepsTheStartRow() throws Exception {
        withData("abc", "FIELD_A", "20250804_0" + NULL_BYTE + "datatype-a");
        withData("abcd", "FIELD_A", "20250804_0" + NULL_BYTE + "datatype-a");
        withOptions("abc.*", "20250804", "20250804");

        Range regexRange = new Range(new Key("abc"), false, new Key("abc" + Constants.MAX_UNICODE_STRING), false);
        UnfieldedRegexExpansionIterator iterator = rebuild(regexRange);

        List<String> emitted = new ArrayList<>();
        while (iterator.hasTop()) {
            emitted.add(describe(iterator.getTopKey()));
            iterator.next();
        }

        assertEquals(List.of("abc FIELD_A", "abcd FIELD_A"), emitted);
    }

    /**
     * The shard reverse index stores every value reversed, so a leading wildcard term can be answered with a prefix range. The iterator un-reverses the row and
     * matches the original pattern against it -- {@code UnfieldedIndexExpansionVisitor#createUnfieldedRegexIndexLookup} passes the query's pattern through
     * unchanged and only flips the range and the reverse flag, and the client reverses the returned row back in {@code BaseRegexIndexLookup#reverse}.
     * <p>
     * This case had no coverage in either regex expansion iterator test.
     */
    @Test
    public void testReverseIndexExpansion() throws Exception {
        withData("raboof", "FIELD_A", "20250804_0" + NULL_BYTE + "datatype-a"); // foobar
        withData("zaboof", "FIELD_A", "20250804_0" + NULL_BYTE + "datatype-a"); // foobaz
        withOptions(".*bar", "20250804", "20250804");
        withReverse();

        assertEquals(List.of("raboof FIELD_A"), scanToCompletion());
    }

    /**
     * The teardown loop is not specific to the forward index. The reverse index is the more exposed of the two, because a leading wildcard is exactly the case
     * that forces a scan of a large row space for a small number of matches.
     */
    @Test
    public void testReverseIndexScanTerminatesWhenRebuiltAfterEveryKey() throws Exception {
        withData("raboof", "FIELD_A", "20250804_0" + NULL_BYTE + "datatype-a"); // foobar
        withData("raboog", "FIELD_A", "20250804_0" + NULL_BYTE + "datatype-a"); // goobar
        withData("rabzab", "FIELD_A", "20250804_0" + NULL_BYTE + "datatype-a"); // bazbar
        withOptions(".*bar", "20250804", "20250804");
        withReverse();

        List<String> emitted = scanRebuildingAfterEveryKey(data.size() * 4);
        Set<String> distinct = new LinkedHashSet<>(emitted);

        assertEquals(data.size(), distinct.size(), "scan never advanced, emitted: " + emitted);
        assertEquals(distinct.size(), emitted.size(), "scan emitted duplicates: " + emitted);
    }

    /**
     * A value is only worth reporting once per field, so an accepted key advances to the next column family and the remaining shards for that value and field
     * are never visited. A rebuilt stack is seeked at the last returned key, which lands inside the column family the previous stack skipped, so it has to skip
     * that column family as well rather than report the value a second time.
     */
    @Test
    public void testRebuiltStackSkipsTheColumnFamilyItAlreadyReportedOn() throws Exception {
        withData("aa", "FIELD_A", "20250804_0" + NULL_BYTE + "datatype-a");
        withData("aa", "FIELD_A", "20250804_1" + NULL_BYTE + "datatype-a");
        withData("ab", "FIELD_A", "20250804_0" + NULL_BYTE + "datatype-a");
        withOptions("a.*", "20250804", "20250804");

        UnfieldedRegexExpansionIterator iterator = rebuild(new Range());
        assertTrue(iterator.hasTop());
        Key lastKey = iterator.getTopKey();
        assertEquals("aa FIELD_A", describe(lastKey));

        UnfieldedRegexExpansionIterator rebuilt = rebuild(new Range(lastKey, false, null, false));
        assertTrue(rebuilt.hasTop());
        assertEquals("ab FIELD_A", describe(rebuilt.getTopKey()));
    }

    /**
     * The invariant the tablet server relies on: tearing the stack down between every key produces exactly the scan that was never torn down.
     */
    @Test
    public void testRebuildingAfterEveryKeyMatchesAnUninterruptedScan() throws Exception {
        withData("aa", "FIELD_A", "20250804_0" + NULL_BYTE + "datatype-a");
        withData("aa", "FIELD_A", "20250804_1" + NULL_BYTE + "datatype-a");
        withData("aa", "FIELD_B", "20250804_0" + NULL_BYTE + "datatype-a");
        withData("ab", "FIELD_A", "20250804_0" + NULL_BYTE + "datatype-a");
        withOptions("a.*", "20250804", "20250804");

        assertEquals(scanToCompletion(), scanRebuildingAfterEveryKey(data.size() * 4));
    }

}
