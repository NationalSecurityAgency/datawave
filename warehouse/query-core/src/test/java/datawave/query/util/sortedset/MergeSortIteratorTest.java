package datawave.query.util.sortedset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Test;

public class MergeSortIteratorTest {

    @Test
    public void testIteration() {
        SortedSet<Integer> set1 = new TreeSet<>();
        SortedSet<Integer> set2 = new TreeSet<>();
        SortedSet<Integer> set3 = new TreeSet<>();

        set1.add(1);
        set1.add(3);
        set1.add(4);
        set1.add(5);
        set1.add(6);
        set1.add(10);

        set2.add(1);
        set2.add(2);
        set2.add(5);
        set2.add(20);

        set3.add(2);
        set3.add(5);
        set3.add(6);
        set3.add(30);

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

        List<SortedSet<Integer>> col = new ArrayList<>();
        col.add(set1);
        col.add(set2);
        col.add(set3);
        List<Integer> results = new ArrayList<>();
        MergeSortIterator<Integer> it = new MergeSortIterator<>(col);
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
            assertTrue(set1.contains(next) || set2.contains(next) || set3.contains(next));
            it.remove();
            assertFalse(set1.contains(next) || set2.contains(next) || set3.contains(next));
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
        SortedSet<Integer> set1 = new TreeSet<>();
        SortedSet<Integer> set2 = new TreeSet<>();
        SortedSet<Integer> set3 = new TreeSet<>();

        set1.add(1);
        set1.add(3);
        set1.add(4);
        set1.add(5);
        set1.add(6);
        set1.add(10);

        set2.add(1);
        set2.add(2);
        set2.add(5);
        set2.add(20);

        set3.add(2);
        set3.add(5);
        set3.add(6);
        set3.add(30);

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

        List<SortedSet<Integer>> col = new ArrayList<>();
        col.add(set1);
        col.add(set2);
        col.add(set3);
        List<Integer> results = new ArrayList<>();
        MergeSortIterator<Integer> it = new MergeSortIterator<>(col);
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
            assertTrue(set1.contains(next) || set2.contains(next) || set3.contains(next));
            it.remove();
            assertFalse(set1.contains(next) || set2.contains(next) || set3.contains(next));
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

        SortedSet<Integer> set1 = new TreeSet<>(c);
        SortedSet<Integer> set2 = new TreeSet<>(c);
        SortedSet<Integer> set3 = new TreeSet<>(c);

        set1.add(1);
        set1.add(3);
        set1.add(4);
        set1.add(5);
        set1.add(6);
        set1.add(10);

        set2.add(null);
        set2.add(1);
        set2.add(2);
        set2.add(5);
        set2.add(20);

        set3.add(null);
        set3.add(2);
        set3.add(5);
        set3.add(6);
        set3.add(30);

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

        List<SortedSet<Integer>> col = new ArrayList<>();
        col.add(set1);
        col.add(set2);
        col.add(set3);
        List<Integer> results = new ArrayList<>();
        MergeSortIterator<Integer> it = new MergeSortIterator<>(col);
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
            assertTrue(set1.contains(next) || set2.contains(next) || set3.contains(next));
            it.remove();
            assertFalse(set1.contains(next) || set2.contains(next) || set3.contains(next));
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
