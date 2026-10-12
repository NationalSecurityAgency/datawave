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
 * Teardown and rebuild tests for the {@link FieldedRegexExpansionIterator}, mirroring {@link UnfieldedRegexExpansionIteratorRebuildTest}.
 * <p>
 * An Accumulo iterator stack may be torn down and rebuilt at any point during a scan, and the rebuilt stack is seeked with
 * {@code new Range(lastReturnedKey, false, ...)}. An iterator that hands that key back a second time makes no forward progress when a rebuild lands between
 * every pair of emitted keys. This iterator used to override {@code seek} to rewrite the exclusive start key to an inclusive one, which did exactly that.
 */
public class FieldedRegexExpansionIteratorRebuildTest {

    private static final Value EMPTY_VALUE = new Value();
    private static final String COLUMN_QUALIFIER = "20250804_0" + "\0" + "datatype-a";

    private final SortedMap<Key,Value> data = new TreeMap<>();
    private final Map<String,String> options = new HashMap<>();

    private void withData(String value) {
        data.put(new Key(value, "FIELD_A", COLUMN_QUALIFIER), EMPTY_VALUE);
    }

    private void withData(String value, String shard) {
        data.put(new Key(value, "FIELD_A", shard + "\0" + "datatype-a"), EMPTY_VALUE);
    }

    private void withOptions(String pattern) {
        options.put(FieldedRegexExpansionIterator.FIELD, "FIELD_A");
        options.put(FieldedRegexExpansionIterator.PATTERN, pattern);
        options.put(FieldedRegexExpansionIterator.START_DATE, "20250804");
        options.put(FieldedRegexExpansionIterator.END_DATE, "20250804");
    }

    private void withReverse() {
        options.put(FieldedRegexExpansionIterator.REVERSE, "true");
    }

    /**
     * Builds a fresh iterator stack and seeks it, the same way a tablet server does after tearing an iterator down.
     *
     * @param range
     *            the range to seek
     * @return the seeked iterator
     */
    private FieldedRegexExpansionIterator rebuild(Range range) throws Exception {
        FieldedRegexExpansionIterator iterator = new FieldedRegexExpansionIterator();
        iterator.init(new SortedMapIterator(data), options, null);
        iterator.seek(range, Collections.emptySet(), false);
        return iterator;
    }

    /**
     * Scans the configured data, tearing the iterator down and rebuilding it after every key returned.
     *
     * @param maxRebuilds
     *            the point at which a non-advancing scan is abandoned
     * @return every value emitted, in order, including any that were emitted more than once
     */
    private List<String> scanRebuildingAfterEveryKey(int maxRebuilds) throws Exception {
        List<String> emitted = new ArrayList<>();
        Range range = new Range();
        for (int i = 0; i < maxRebuilds; i++) {
            FieldedRegexExpansionIterator iterator = rebuild(range);
            if (!iterator.hasTop()) {
                break;
            }
            Key topKey = iterator.getTopKey();
            emitted.add(topKey.getRow().toString());
            range = new Range(topKey, false, null, false);
        }
        return emitted;
    }

    private void assertScanAdvances() throws Exception {
        // a well behaved iterator needs at most one rebuild per key, so this bound is generous
        List<String> emitted = scanRebuildingAfterEveryKey(data.size() * 4);
        Set<String> distinct = new LinkedHashSet<>(emitted);

        assertEquals(data.size(), distinct.size(), "scan never advanced, emitted: " + emitted);
        assertEquals(distinct.size(), emitted.size(), "scan emitted duplicates: " + emitted);
    }

    /**
     * A rebuilt stack is seeked at the last returned key, exclusive. It must not hand that key back.
     */
    @Test
    public void testRebuiltStackDoesNotReemitTheLastKey() throws Exception {
        withData("aa");
        withData("ab");
        withOptions("a.*");

        FieldedRegexExpansionIterator iterator = rebuild(new Range());
        assertTrue(iterator.hasTop());
        Key lastKey = iterator.getTopKey();
        assertEquals("aa", lastKey.getRow().toString());

        FieldedRegexExpansionIterator rebuilt = rebuild(new Range(lastKey, false, null, false));
        assertTrue(rebuilt.hasTop());
        assertEquals("ab", rebuilt.getTopKey().getRow().toString());
    }

    @Test
    public void testScanTerminatesWhenRebuiltAfterEveryKey() throws Exception {
        withData("aa");
        withData("ab");
        withData("ac");
        withOptions("a.*");

        assertScanAdvances();
    }

    /**
     * The reverse index stores each value reversed, so the iterator un-reverses the row before matching the original pattern against it.
     */
    @Test
    public void testReverseIndexScanTerminatesWhenRebuiltAfterEveryKey() throws Exception {
        withData("raboof"); // foobar
        withData("raboog"); // goobar
        withData("rabzab"); // bazbar
        withOptions(".*bar");
        withReverse();

        assertScanAdvances();
    }

    /**
     * {@code ShardIndexQueryTableStaticMethods#getRegexRange} builds every range this iterator is given with an exclusive start key, but that start key is a
     * bare row key with an empty column family, so every real key in the row still sorts after it. Honoring the exclusive start therefore costs nothing.
     */
    @Test
    public void testExclusiveStartKeyFromGetRegexRangeKeepsTheStartRow() throws Exception {
        withData("abc");
        withData("abcd");
        withOptions("abc.*");

        Range regexRange = new Range(new Key("abc"), false, new Key("abc" + Constants.MAX_UNICODE_STRING), false);
        FieldedRegexExpansionIterator iterator = rebuild(regexRange);

        List<String> emitted = new ArrayList<>();
        while (iterator.hasTop()) {
            emitted.add(iterator.getTopKey().getRow().toString());
            iterator.next();
        }

        assertEquals(List.of("abc", "abcd"), emitted);
    }

    /**
     * A value is only worth reporting once, so an accepted key advances to the next row and the remaining shards for that value are never visited. A rebuilt
     * stack is seeked at the last returned key, which lands inside the row the previous stack skipped, so it has to skip that row as well rather than report
     * the value a second time.
     */
    @Test
    public void testRebuiltStackSkipsTheRowItAlreadyReportedOn() throws Exception {
        withData("aa", "20250804_0");
        withData("aa", "20250804_1");
        withData("ab", "20250804_0");
        withOptions("a.*");

        FieldedRegexExpansionIterator iterator = rebuild(new Range());
        assertTrue(iterator.hasTop());
        Key lastKey = iterator.getTopKey();
        assertEquals("aa", lastKey.getRow().toString());

        FieldedRegexExpansionIterator rebuilt = rebuild(new Range(lastKey, false, null, false));
        assertTrue(rebuilt.hasTop());
        assertEquals("ab", rebuilt.getTopKey().getRow().toString());
    }

    /**
     * The invariant the tablet server relies on: tearing the stack down between every key produces exactly the scan that was never torn down.
     */
    @Test
    public void testRebuildingAfterEveryKeyMatchesAnUninterruptedScan() throws Exception {
        withData("aa", "20250804_0");
        withData("aa", "20250804_1");
        withData("ab", "20250804_0");
        withOptions("a.*");

        assertEquals(List.of("aa", "ab"), scanRebuildingAfterEveryKey(data.size() * 4));
    }
}
