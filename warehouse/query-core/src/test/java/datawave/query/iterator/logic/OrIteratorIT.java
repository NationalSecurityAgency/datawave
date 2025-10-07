package datawave.query.iterator.logic;

import static datawave.query.iterator.logic.TestUtil.randomUids;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException;
import org.junit.jupiter.api.Test;

import datawave.query.attributes.Document;
import datawave.query.iterator.NestedIterator;

class OrIteratorIT {

    private final SortedSet<String> uidsAll = new TreeSet<>(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "10"));
    private final SortedSet<String> uidsEven = new TreeSet<>(Arrays.asList("2", "4", "6", "8", "10"));
    private final SortedSet<String> uidsOdd = new TreeSet<>(Arrays.asList("1", "3", "5", "7", "9"));

    private static final Random rand = new Random();

    @Test
    void testSimpleUnion() {
        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsEven));
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_B", uidsOdd));
        OrIterator<?> itr = new OrIterator<>(includes);
        driveIterator(itr, new TreeSet<>(uidsAll));
    }

    @Test
    void testUnionWithIndexOnlyTerm() {
        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsEven));
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_B", uidsOdd, true));

        Map<String,Integer> indexOnlyCounts = new HashMap<>();
        indexOnlyCounts.put("FIELD_B", 5);

        OrIterator<?> itr = new OrIterator<>(includes);
        driveIterator(itr, uidsAll, indexOnlyCounts);
    }

    @Test
    void testUnionWithIndexOnlyTermThatIsInterrupted() {
        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsEven));
        includes.add(IndexIteratorBridgeTest.createInterruptibleIndexIteratorBridge("FIELD_B", uidsOdd, true, 4));

        Map<String,Integer> indexOnlyCounts = new HashMap<>();
        indexOnlyCounts.put("FIELD_B", 3);

        OrIterator<?> itr = new OrIterator<>(includes);
        assertThrows(IterationInterruptedException.class, () -> driveIterator(itr, uidsAll, indexOnlyCounts));
    }

    // A union with a negated term cannot be evaluated by itself
    @Test
    void testUnionWithNegatedIndexOnlyTermThatIsInterrupted() {
        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsEven));

        Set<NestedIterator<Key>> excludes = new HashSet<>();
        excludes.add(IndexIteratorBridgeTest.createInterruptibleIndexIteratorBridge("FIELD_B", uidsOdd, true, 4));

        Map<String,Integer> indexOnlyCounts = new HashMap<>();
        indexOnlyCounts.put("FIELD_B", 3);

        OrIterator<?> itr = new OrIterator<>(includes, excludes);
        assertThrows(IllegalStateException.class, () -> driveIterator(itr, uidsAll, indexOnlyCounts));
    }

    @Test
    void testUnionOfLowCardinalityIndexOnlyTerms() {
        int max = 100;
        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, 5);
            SortedSet<String> uidsB = randomUids(100, 10);
            SortedSet<String> uidsC = randomUids(100, 15);
            driveUnion(uidsA, uidsB, uidsC);
        }
    }

    @Test
    void testUnionOfHighCardinalityIndexOnlyTerms() {
        int max = 100;
        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, 50);
            SortedSet<String> uidsB = randomUids(100, 75);
            SortedSet<String> uidsC = randomUids(100, 85);
            driveUnion(uidsA, uidsB, uidsC);
        }
    }

    @Test
    void testUnionOfVariableCardinalityIndexOnlyTerms() {
        int max = 100;
        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, rand.nextInt(100));
            SortedSet<String> uidsB = randomUids(100, rand.nextInt(100));
            SortedSet<String> uidsC = randomUids(100, rand.nextInt(100));
            driveUnion(uidsA, uidsB, uidsC);
        }
    }

    @Test
    void testCase01() {
        SortedSet<String> uidsA = new TreeSet<>(Arrays.asList("20", "29", "83", "87", "99"));
        SortedSet<String> uidsB = new TreeSet<>(Arrays.asList("1", "30", "61", "8", "90"));
        SortedSet<String> uidsC = new TreeSet<>(Arrays.asList("31", "4", "52", "55", "79"));
        driveUnion(uidsA, uidsB, uidsC);
    }

    @Test
    void testCase02() {
        SortedSet<String> uidsA = new TreeSet<>(Arrays.asList("20", "29", "83", "87", "99"));
        SortedSet<String> uidsB = new TreeSet<>();
        SortedSet<String> uidsC = new TreeSet<>(Arrays.asList("31", "4", "52", "55", "79"));
        driveUnion(uidsA, uidsB, uidsC);
    }

    private void driveUnion(SortedSet<String> uidsA, SortedSet<String> uidsB, SortedSet<String> uidsC) {
        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsA, true));
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_B", uidsB, true));
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_C", uidsC, true));

        OrIterator<Key> orIterator = new OrIterator<>(includes);

        Map<String,Integer> indexOnlyCounts = new HashMap<>();
        if (!uidsA.isEmpty()) {
            indexOnlyCounts.put("FIELD_A", uidsA.size());
        }
        if (!uidsB.isEmpty()) {
            indexOnlyCounts.put("FIELD_B", uidsB.size());
        }
        if (!uidsC.isEmpty()) {
            indexOnlyCounts.put("FIELD_C", uidsC.size());
        }

        SortedSet<String> uids = new TreeSet<>();
        uids.addAll(uidsA);
        uids.addAll(uidsB);
        uids.addAll(uidsC);

        driveIterator(orIterator, uids, indexOnlyCounts);
    }

    /**
     * Drives the union given a set of expected uids
     *
     * @param itr
     *            the union
     * @param uids
     *            expected uids
     */
    private void driveIterator(OrIterator<?> itr, SortedSet<String> uids) {
        driveIterator(itr, uids, Collections.emptyMap());
    }

    /**
     * Drives the union given a set of expected uids and expected index only field counts
     *
     * @param itr
     *            the union
     * @param uids
     *            the expected uids
     * @param indexOnlyCounts
     *            the expected index only field counts
     */
    private void driveIterator(OrIterator<?> itr, SortedSet<String> uids, Map<String,Integer> indexOnlyCounts) {
        seekIterator(itr);
        itr.initialize();

        int count = 0;
        Map<String,Integer> counts = new HashMap<>();

        for (String uid : uids) {
            count++;
            assertTrue(itr.hasNext());

            Object o = itr.next();
            assertInstanceOf(Key.class, o);
            IndexIteratorTest.assertTopKey((Key) o, uid);

            if (!indexOnlyCounts.isEmpty()) {
                Document d = itr.document();

                for (String indexOnlyField : indexOnlyCounts.keySet()) {
                    if (d.containsKey(indexOnlyField)) {
                        int i = counts.getOrDefault(indexOnlyField, 0);
                        counts.put(indexOnlyField, ++i);
                    }
                }
            }
        }

        assertFalse(itr.hasNext(), "iterator had more elements");
        assertEquals(uids.size(), count, "expected next count did not match");
        assertEquals(indexOnlyCounts, counts);
    }

    private void seekIterator(NestedIterator<?> iterator) {
        try {
            iterator.seek(new Range(), Collections.emptyList(), false);
        } catch (IOException e) {
            fail("Failed to seek iterators", e);
        }
    }
}
