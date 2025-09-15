package datawave.query.index.day;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;

public class BitSetIndexEntryTest {

    @Test
    public void testUniqueness() {
        Map<String,BitSet> firstMap = Collections.singletonMap("FOO == 'bar'", BitSetFactory.create(1, 2, 3));
        BitSetIndexEntry first = new BitSetIndexEntry("2023", firstMap);

        Map<String,BitSet> secondMap = Collections.singletonMap("FOO == 'bar'", BitSetFactory.create(1, 2, 3));
        BitSetIndexEntry second = new BitSetIndexEntry("2023", secondMap);

        assertEquals(first, second);

        Set<BitSetIndexEntry> set = new HashSet<>();
        set.add(first);
        set.add(second);
        assertEquals(1, set.size());
    }

    @Test
    public void testCompareToYear() {
        Map<String,BitSet> firstMap = Collections.singletonMap("FOO == 'bar'", BitSetFactory.create(1, 2, 3));
        BitSetIndexEntry first = new BitSetIndexEntry("2023", firstMap);

        Map<String,BitSet> secondMap = Collections.singletonMap("FOO == 'bar'", BitSetFactory.create(1, 2, 3));
        BitSetIndexEntry second = new BitSetIndexEntry("2024", secondMap);

        assertEquals(-1, first.compareTo(second));
        assertEquals(1, second.compareTo(first));
    }

    @Test
    public void testCompareToMapKey() {
        Map<String,BitSet> firstMap = Collections.singletonMap("FOO == 'bar'", BitSetFactory.create(1, 2, 3));
        BitSetIndexEntry first = new BitSetIndexEntry("2023", firstMap);

        Map<String,BitSet> secondMap = Collections.singletonMap("FOO == 'baz'", BitSetFactory.create(1, 2, 3));
        BitSetIndexEntry second = new BitSetIndexEntry("2023", secondMap);

        assertTrue(first.compareTo(second) < 0);
        assertTrue(second.compareTo(first) < 0);
    }

    @Test
    public void testCompareToBitSets() {
        Map<String,BitSet> firstMap = Collections.singletonMap("FOO == 'bar'", BitSetFactory.create(1, 2, 3));
        BitSetIndexEntry first = new BitSetIndexEntry("2023", firstMap);

        Map<String,BitSet> secondMap = Collections.singletonMap("FOO == 'bar'", BitSetFactory.create(4, 5, 6));
        BitSetIndexEntry second = new BitSetIndexEntry("2023", secondMap);

        assertTrue(first.compareTo(second) < 0);
        assertTrue(second.compareTo(first) < 0);
    }

    @Test
    public void testSortOrderBasedOnYear() {
        Map<String,BitSet> map = Collections.singletonMap("FOO == 'bar'", BitSetFactory.create(1, 2, 3));
        BitSetIndexEntry first = new BitSetIndexEntry("2023", map);
        BitSetIndexEntry second = new BitSetIndexEntry("2024", map);

        SortedSet<BitSetIndexEntry> set = new TreeSet<>();
        set.add(second);
        set.add(first);

        assertEquals("2023", set.first().getYearOrDay());
        assertEquals("2024", set.last().getYearOrDay());
    }

    @Test
    public void testSortOrderBasedOnMapKey() {
        Map<String,BitSet> firstMap = new HashMap<>();
        firstMap.put("FOO == 'bar'", BitSetFactory.create(1, 2, 3));

        Map<String,BitSet> secondMap = new HashMap<>();
        secondMap.put("FOO == 'baz'", BitSetFactory.create(1, 2, 3));

        BitSetIndexEntry first = new BitSetIndexEntry("2024", firstMap);
        BitSetIndexEntry second = new BitSetIndexEntry("2024", secondMap);

        SortedSet<BitSetIndexEntry> set = new TreeSet<>();
        set.add(second);
        set.add(first);

        assertEquals("FOO == 'bar'", set.first().getEntries().keySet().iterator().next());
        assertEquals("FOO == 'baz'", set.last().getEntries().keySet().iterator().next());
    }
}
