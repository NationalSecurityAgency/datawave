package datawave.core.iterators.compress.event;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.Test;

public class EventKeyComparatorTest {

    @Test
    public void testCompareSameRow() {
        Key left = new Key("row");
        Key right = new Key("row");
        assertCount(1, left, right);

        Key r1 = new Key("row-1");
        Key r2 = new Key("row-2");
        assertCount(2, r1, r2);
    }

    @Test
    public void testCompareSameRowColumnFamily() {
        Key left = new Key("row", "cf");
        Key right = new Key("row", "cf");
        assertCount(1, left, right);

        Key cf1 = new Key("row", "cf-1");
        Key cf2 = new Key("row", "cf-2");
        assertCount(2, cf1, cf2);
    }

    @Test
    public void testCompareSameRowColumnFamilyVisibility() {
        // the column qualifier is allowed to be different
        Key left = new Key("row", "cf", "FIELD_A\0value-a", "VIZ");
        Key right = new Key("row", "cf", "FIELD_B\0value-b", "VIZ");
        assertCount(1, left, right);

        Key viz1 = new Key("row", "cf", "FIELD_A\0value-a", "VIZ-A");
        Key viz2 = new Key("row", "cf", "FIELD_B\0value-b", "VIZ-B");
        assertCount(2, viz1, viz2);
    }

    @Test
    public void testCompareSameRowColumnFamilyVisibilityTimestamp() {
        // the column qualifier is allowed to be different
        Key left = new Key("row", "cf", "FIELD_A\0value-a", "VIZ", 10L);
        Key right = new Key("row", "cf", "FIELD_B\0value-b", "VIZ", 10L);
        assertCount(1, left, right);

        Key ts1 = new Key("row", "cf", "FIELD_A\0value-a", "VIZ-A", 10L);
        Key ts2 = new Key("row", "cf", "FIELD_B\0value-b", "VIZ-A", 11L);
        assertCount(2, ts1, ts2);
    }

    @Test
    public void testCompareSameRowColumnFamilyVisibilityTimestampDelete() {
        // the column qualifier is allowed to be different, have to use a different constructor to use the delete flat
        Key left = new Key("row".getBytes(), "cf".getBytes(), "FIELD_A\0value-a".getBytes(), "VIZ".getBytes(), 10L, true);
        Key right = new Key("row".getBytes(), "cf".getBytes(), "FIELD_B\0value-b".getBytes(), "VIZ".getBytes(), 10L, true);
        assertCount(1, left, right);

        Key delete1 = new Key("row".getBytes(), "cf".getBytes(), "FIELD_A\0value-a".getBytes(), "VIZ".getBytes(), 10L, true);
        Key delete2 = new Key("row".getBytes(), "cf".getBytes(), "FIELD_B\0value-b".getBytes(), "VIZ".getBytes(), 10L, false);
        assertCount(2, delete1, delete2);
    }

    @Test
    public void testSortedOrder() {
        Key k1 = new Key("row".getBytes(), "cf".getBytes(), "FIELD_A\0value-a".getBytes(), "VIZ-A".getBytes(), 11L, false);
        Key k2 = new Key("row".getBytes(), "cf".getBytes(), "FIELD_A\0value-a".getBytes(), "VIZ-B".getBytes(), 11L, false);
        Key k3 = new Key("row".getBytes(), "cf".getBytes(), "FIELD_A\0value-a".getBytes(), "VIZ-B".getBytes(), 10L, false);
        Key k4 = new Key("row".getBytes(), "cf".getBytes(), "FIELD_A\0value-a".getBytes(), "VIZ-B".getBytes(), 10L, true);

        SortedSet<Key> keys = new TreeSet<>(new EventKeyComparator());
        keys.addAll(List.of(k1, k2, k3, k4));

        Iterator<Key> iter = keys.iterator();
        assertEquals(k1, iter.next());
        assertEquals(k2, iter.next());
        assertEquals(k3, iter.next());
        assertEquals(k4, iter.next());
        assertFalse(iter.hasNext());
    }

    private void assertCount(int expected, Key... keys) {
        SortedSet<Key> set = new TreeSet<>(new EventKeyComparator());
        set.addAll(Arrays.asList(keys));
        assertEquals(expected, set.size(), "sorted set size did not match expectation");
    }
}
