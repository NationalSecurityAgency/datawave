package datawave.accumulo.inmemory.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

class SortedMapIteratorTest {

    private static final Collection<ByteSequence> NO_COLUMN_FAMILIES = Collections.emptySet();

    private static final Key ROW1_CQ1 = new Key("row1", "cf", "cq1");
    private static final Key ROW1_CQ2 = new Key("row1", "cf", "cq2");
    private static final Key ROW2_CQ1 = new Key("row2", "cf", "cq1");
    private static final Key ROW2_CQ2 = new Key("row2", "cf", "cq2");
    private static final Key ROW3_CQ1 = new Key("row3", "cf", "cq1");

    private static final List<Key> ALL_KEYS = List.of(ROW1_CQ1, ROW1_CQ2, ROW2_CQ1, ROW2_CQ2, ROW3_CQ1);

    private static SortedMap<Key,Value> data() {
        SortedMap<Key,Value> data = new TreeMap<>();
        data.put(ROW1_CQ1, new Value("a"));
        data.put(ROW1_CQ2, new Value("b"));
        data.put(ROW2_CQ1, new Value("c"));
        data.put(ROW2_CQ2, new Value("d"));
        data.put(ROW3_CQ1, new Value("e"));
        return data;
    }

    /** Row-only keys row000, row001, and so on, which sort in index order. */
    private static Key row(int index) {
        return new Key(String.format("row%03d", index));
    }

    private static SortedMap<Key,Value> rows(int count) {
        SortedMap<Key,Value> data = new TreeMap<>();
        for (int i = 0; i < count; i++) {
            data.put(row(i), new Value());
        }
        return data;
    }

    private static List<Key> scan(SortedMap<Key,Value> data, Range range) throws IOException {
        return readAll(new SortedMapIterator(data), range);
    }

    private static List<Key> readAll(SortedKeyValueIterator<Key,Value> iter, Range range) throws IOException {
        iter.seek(range, NO_COLUMN_FAMILIES, false);
        List<Key> keys = new ArrayList<>();
        while (iter.hasTop()) {
            keys.add(iter.getTopKey());
            iter.next();
        }
        return keys;
    }

    private static void next(SortedKeyValueIterator<Key,Value> iter, int times) throws IOException {
        for (int i = 0; i < times; i++) {
            iter.next();
        }
    }

    @Test
    void seekWithInclusiveStartKey() throws IOException {
        SortedMapIterator iter = new SortedMapIterator(data());
        iter.seek(new Range(ROW2_CQ1, true, null, false), NO_COLUMN_FAMILIES, false);
        assertEquals(ROW2_CQ1, iter.getTopKey());
        assertEquals(new Value("c"), iter.getTopValue());

        assertEquals(List.of(ROW2_CQ1, ROW2_CQ2, ROW3_CQ1), scan(data(), new Range(ROW2_CQ1, true, null, false)));
    }

    @Test
    void seekWithExclusiveStartKey() throws IOException {
        assertEquals(List.of(ROW2_CQ2, ROW3_CQ1), scan(data(), new Range(ROW2_CQ1, false, null, false)));

        // skipping an exclusive start key that is the last entry leaves nothing
        assertEquals(List.of(), scan(data(), new Range(ROW3_CQ1, false, null, false)));
    }

    @Test
    void seekWithPartialStartKey() throws IOException {
        Key row2 = new Key("row2");

        // a row-only key sorts before the rest of its row, so an exclusive start on it keeps the row
        assertEquals(List.of(ROW2_CQ1, ROW2_CQ2, ROW3_CQ1), scan(data(), new Range(row2, false, null, false)));

        // when the map holds the row-only key itself, an exclusive start skips just that entry
        SortedMap<Key,Value> data = data();
        data.put(row2, new Value("row2"));
        assertEquals(List.of(row2, ROW2_CQ1, ROW2_CQ2, ROW3_CQ1), scan(data, new Range(row2, true, null, false)));
        assertEquals(List.of(ROW2_CQ1, ROW2_CQ2, ROW3_CQ1), scan(data, new Range(row2, false, null, false)));

        // a row range with an exclusive start row skips the whole row
        assertEquals(List.of(ROW3_CQ1), scan(data, new Range(new Text("row2"), false, null, false)));
    }

    @Test
    void seekWithEndKey() throws IOException {
        assertEquals(List.of(ROW1_CQ2, ROW2_CQ1), scan(data(), new Range(ROW1_CQ2, true, ROW2_CQ2, false)));
        assertEquals(List.of(ROW1_CQ2, ROW2_CQ1, ROW2_CQ2), scan(data(), new Range(ROW1_CQ2, true, ROW2_CQ2, true)));

        // a row range includes all of its end row
        assertEquals(List.of(ROW1_CQ1, ROW1_CQ2, ROW2_CQ1, ROW2_CQ2), scan(data(), new Range(new Text("row1"), new Text("row2"))));
    }

    @Test
    void seekWithRangeThatMatchesNothing() throws IOException {
        // between two entries
        assertEquals(List.of(), scan(data(), new Range(new Key("row1", "cf", "cq3"), true, new Key("row1", "cf", "cq4"), true)));
        // after the last entry
        assertEquals(List.of(), scan(data(), new Range(new Text("row4"), null)));
        // an empty map
        assertEquals(List.of(), scan(new TreeMap<>(), new Range()));
    }

    @Test
    void seekWithInfiniteRange() throws IOException {
        // the empty key sorts before every other key
        SortedMap<Key,Value> data = data();
        data.put(new Key(), new Value("empty"));
        List<Key> expected = new ArrayList<>(ALL_KEYS);
        expected.add(0, new Key());

        assertEquals(expected, scan(data, new Range()));

        // an infinite start excludes nothing, even when the range marks its start as exclusive
        assertEquals(expected, scan(data, new Range((Key) null, false, (Key) null, false)));
    }

    @Test
    void seekIgnoresColumnFamilies() throws IOException {
        // like Accumulo's SortedMapIterator, this does not filter on the column families passed to seek
        SortedMapIterator iter = new SortedMapIterator(data());
        iter.seek(new Range(), Set.of(new ArrayByteSequence("other")), true);
        assertEquals(ROW1_CQ1, iter.getTopKey());
    }

    @Test
    void nextExhaustsTheRange() throws IOException {
        SortedMapIterator iter = new SortedMapIterator(data());

        // before a seek there is no entry to move on from
        assertThrows(IllegalStateException.class, iter::next);

        iter.seek(new Range(new Text("row1")), NO_COLUMN_FAMILIES, false);
        assertEquals(ROW1_CQ1, iter.getTopKey());
        iter.next();
        assertEquals(ROW1_CQ2, iter.getTopKey());
        iter.next();
        assertFalse(iter.hasTop());
        assertThrows(IllegalStateException.class, iter::next);
    }

    @Test
    void deepCopyIsPositionedIndependently() throws IOException {
        SortedMapIterator iter = new SortedMapIterator(data());
        iter.seek(new Range(ROW2_CQ1, true, null, false), NO_COLUMN_FAMILIES, false);

        SortedKeyValueIterator<Key,Value> copy = iter.deepCopy(null);

        // the copy starts unpositioned, and seeking it leaves the original where it was
        assertFalse(copy.hasTop());
        assertEquals(ALL_KEYS, readAll(copy, new Range()));
        assertEquals(ROW2_CQ1, iter.getTopKey());
        iter.next();
        assertEquals(ROW2_CQ2, iter.getTopKey());
    }

    @Test
    void seekThrowsWhenInterrupted() throws IOException {
        SortedMapIterator iter = new SortedMapIterator(data());
        AtomicBoolean flag = new AtomicBoolean(false);
        iter.setInterruptFlag(flag);
        iter.seek(new Range(), NO_COLUMN_FAMILIES, false);
        iter.next();

        // seek checks the flag on every call, before moving the iterator
        flag.set(true);
        assertThrows(IterationInterruptedException.class, () -> iter.seek(new Range(), NO_COLUMN_FAMILIES, false));
        assertThrows(IterationInterruptedException.class, () -> iter.seek(new Range(), NO_COLUMN_FAMILIES, false));
        assertEquals(ROW1_CQ2, iter.getTopKey());
    }

    @Test
    void nextChecksTheInterruptFlagEvery100thCallStartingWithTheFirst() throws IOException {
        SortedMapIterator iter = new SortedMapIterator(rows(300));
        AtomicBoolean flag = new AtomicBoolean(false);
        iter.setInterruptFlag(flag);
        iter.seek(new Range(), NO_COLUMN_FAMILIES, false);
        flag.set(true);

        // call 1 checks the flag, and throws without moving the iterator
        assertThrows(IterationInterruptedException.class, iter::next);
        assertEquals(row(0), iter.getTopKey());

        // calls 2-100 do not check it, call 101 does
        next(iter, 99);
        assertEquals(row(99), iter.getTopKey());
        assertThrows(IterationInterruptedException.class, iter::next);

        // calls 102-200 do not check it, call 201 does
        next(iter, 99);
        assertEquals(row(198), iter.getTopKey());
        assertThrows(IterationInterruptedException.class, iter::next);
        assertEquals(row(198), iter.getTopKey());

        // calls 202-301 run on once the flag is cleared
        flag.set(false);
        next(iter, 100);
        assertEquals(row(298), iter.getTopKey());
    }

    @Test
    void seekDoesNotResetTheInterruptCheckCount() throws IOException {
        SortedMapIterator iter = new SortedMapIterator(rows(300));
        AtomicBoolean flag = new AtomicBoolean(false);
        iter.setInterruptFlag(flag);

        // seek skips an exclusive start key by calling next, which is call 1
        iter.seek(new Range(row(0), false, null, false), NO_COLUMN_FAMILIES, false);
        assertEquals(row(1), iter.getTopKey());
        next(iter, 49); // calls 2-50

        // seeking again keeps the count, so calls 51-100 do not check the flag and call 101 does
        iter.seek(new Range(), NO_COLUMN_FAMILIES, false);
        flag.set(true);
        next(iter, 50);
        assertEquals(row(50), iter.getTopKey());
        assertThrows(IterationInterruptedException.class, iter::next);
    }

    @Test
    void deepCopySharesTheInterruptFlag() throws IOException {
        SortedMapIterator iter = new SortedMapIterator(data());
        AtomicBoolean flag = new AtomicBoolean(false);
        iter.setInterruptFlag(flag);
        iter.seek(new Range(), NO_COLUMN_FAMILIES, false);
        iter.next(); // call 1 for the original

        SortedKeyValueIterator<Key,Value> copy = iter.deepCopy(null);
        copy.seek(new Range(), NO_COLUMN_FAMILIES, false);
        flag.set(true);

        // the copy counts its own calls, so its call 1 checks the shared flag while the original's call 2 does not
        assertThrows(IterationInterruptedException.class, copy::next);
        iter.next();
        assertEquals(ROW2_CQ1, iter.getTopKey());
        assertThrows(IterationInterruptedException.class, () -> copy.seek(new Range(), NO_COLUMN_FAMILIES, false));
    }

    @Test
    void iterationInterruptedExceptionIsUnchecked() {
        RuntimeException e = new IterationInterruptedException("stopped");
        assertEquals("stopped", e.getMessage());
        assertNull(new IterationInterruptedException().getMessage());
    }

    @Test
    void initIsUnsupported() {
        SortedMapIterator iter = new SortedMapIterator(data());
        assertThrows(UnsupportedOperationException.class, () -> iter.init(null, Map.of(), null));
    }
}
