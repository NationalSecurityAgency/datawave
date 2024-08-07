package datawave.query.util.sortedmap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Test;

public class MergeSortIteratorTest {

    @Test
    public void testIteration() {
        SortedMap<Integer,Integer> set1 = new TreeMap<>();
        SortedMap<Integer,Integer> set2 = new TreeMap<>();
        SortedMap<Integer,Integer> set3 = new TreeMap<>();

        set1.put(1, 1);
        set1.put(3, 1);
        set1.put(4, 1);
        set1.put(5, 1);
        set1.put(6, 1);
        set1.put(10, 1);

        set2.put(1, 1);
        set2.put(2, 1);
        set2.put(5, 1);
        set2.put(20, 1);

        set3.put(2, 1);
        set3.put(5, 1);
        set3.put(6, 1);
        set3.put(30, 1);

        List<Integer> expected = new ArrayList<>();
        expected.add(1);
        expected.add(2);
        expected.add(3);
        expected.add(4);
        expected.add(5);
        expected.add(6);
        expected.add(10);
        expected.add(20);
        expected.add(30);

        List<SortedMap<Integer,Integer>> col = new ArrayList<>();
        col.add(set1);
        col.add(set2);
        col.add(set3);
        List<Integer> results = new ArrayList<>();

        Iterator<Integer> it = new MultiMapBackedSortedMap(col).keySet().iterator();
        try {
            it.remove();
            fail("Expected remove to fail");
        } catch (Exception e) {
            // expected
        }
        while (it.hasNext()) {
            try {
                it.remove();
                fail("Expected remove to fail");
            } catch (Exception e) {
                // expected
            }
            Integer next = it.next();
            results.add(next);
            assertTrue(set1.containsKey(next) || set2.containsKey(next) || set3.containsKey(next));
            it.remove();
            assertFalse(set1.containsKey(next) || set2.containsKey(next) || set3.containsKey(next));
            try {
                it.remove();
                fail("Expected remove to fail");
            } catch (Exception e) {
                // expected
            }
        }
        assertEquals(expected, results);
        assertTrue(set1.isEmpty() && set2.isEmpty() && set3.isEmpty());
    }

    @Test
    public void testIterationSansHasNext() {
        SortedMap<Integer,Integer> set1 = new TreeMap<>();
        SortedMap<Integer,Integer> set2 = new TreeMap<>();
        SortedMap<Integer,Integer> set3 = new TreeMap<>();

        set1.put(1, 1);
        set1.put(3, 1);
        set1.put(4, 1);
        set1.put(5, 1);
        set1.put(6, 1);
        set1.put(10, 1);

        set2.put(1, 1);
        set2.put(2, 1);
        set2.put(5, 1);
        set2.put(20, 1);

        set3.put(2, 1);
        set3.put(5, 1);
        set3.put(6, 1);
        set3.put(30, 1);

        List<Integer> expected = new ArrayList<>();
        expected.add(1);
        expected.add(2);
        expected.add(3);
        expected.add(4);
        expected.add(5);
        expected.add(6);
        expected.add(10);
        expected.add(20);
        expected.add(30);

        List<SortedMap<Integer,Integer>> col = new ArrayList<>();
        col.add(set1);
        col.add(set2);
        col.add(set3);
        List<Integer> results = new ArrayList<>();
        Iterator<Integer> it = new MultiMapBackedSortedMap(col).keySet().iterator();
        while (true) {
            try {
                it.remove();
                fail("Expected remove to fail");
            } catch (Exception e) {
                // expected
            }
            Integer next;
            try {
                next = it.next();
            } catch (NoSuchElementException nsee) {
                break;
            }
            results.add(next);
            assertTrue(set1.containsKey(next) || set2.containsKey(next) || set3.containsKey(next));
            it.remove();
            assertFalse(set1.containsKey(next) || set2.containsKey(next) || set3.containsKey(next));
            try {
                it.remove();
                fail("Expected remove to fail");
            } catch (Exception e) {
                // expected
            }
        }
        assertEquals(expected, results);
        assertTrue(set1.isEmpty() && set2.isEmpty() && set3.isEmpty());
    }

    @Test
    public void testIterationSansWithNulls() {
        Comparator<Integer> c = new Comparator<Integer>() {

            @Override
            public int compare(Integer o1, Integer o2) {
                if (o1 == null) {
                    return (o2 == null ? 0 : -1);
                } else {
                    return (o2 == null ? 1 : o1.compareTo(o2));
                }
            }
        };

        SortedMap<Integer,Integer> set1 = new TreeMap<>(c);
        SortedMap<Integer,Integer> set2 = new TreeMap<>(c);
        SortedMap<Integer,Integer> set3 = new TreeMap<>(c);

        set1.put(1, 1);
        set1.put(3, 1);
        set1.put(4, 1);
        set1.put(5, 1);
        set1.put(6, 1);
        set1.put(10, 1);

        set2.put(null, 1);
        set2.put(1, 1);
        set2.put(2, 1);
        set2.put(5, 1);
        set2.put(20, 1);

        set3.put(null, 1);
        set3.put(2, 1);
        set3.put(5, 1);
        set3.put(6, 1);
        set3.put(30, 1);

        List<Integer> expected = new ArrayList<>();
        expected.add(null);
        expected.add(1);
        expected.add(2);
        expected.add(3);
        expected.add(4);
        expected.add(5);
        expected.add(6);
        expected.add(10);
        expected.add(20);
        expected.add(30);

        List<SortedMap<Integer,Integer>> col = new ArrayList<>();
        col.add(set1);
        col.add(set2);
        col.add(set3);
        List<Integer> results = new ArrayList<>();
        Iterator<Integer> it = new MultiMapBackedSortedMap(col).keySet().iterator();
        try {
            it.remove();
            fail("Expected remove to fail");
        } catch (Exception e) {
            // expected
        }
        while (it.hasNext()) {
            try {
                it.remove();
                fail("Expected remove to fail");
            } catch (Exception e) {
                // expected
            }
            Integer next = it.next();
            results.add(next);
            assertTrue(set1.containsKey(next) || set2.containsKey(next) || set3.containsKey(next));
            it.remove();
            assertFalse(set1.containsKey(next) || set2.containsKey(next) || set3.containsKey(next));
            try {
                it.remove();
                fail("Expected remove to fail");
            } catch (Exception e) {
                // expected
            }
        }
        assertEquals(expected, results);
        assertTrue(set1.isEmpty() && set2.isEmpty() && set3.isEmpty());
    }
}
