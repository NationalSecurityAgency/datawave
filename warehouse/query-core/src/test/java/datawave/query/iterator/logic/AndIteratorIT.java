package datawave.query.iterator.logic;

import static datawave.query.iterator.logic.TestUtil.randomUids;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

import datawave.query.attributes.Document;
import datawave.query.iterator.NestedIterator;

/**
 * Integration test for the {@link AndIterator}. IndexIterators are used with mocked out sources.
 */
class AndIteratorIT {

    // first five
    private final SortedSet<String> uidsA = new TreeSet<>(Arrays.asList("a", "b", "c", "d", "e"));
    // first five, odd
    private final SortedSet<String> uidsB = new TreeSet<>(Arrays.asList("a", "c", "e", "g", "i"));
    // last three
    private final SortedSet<String> uidsC = new TreeSet<>(Arrays.asList("x", "y", "z"));

    private static final Random rand = new Random();

    @Test
    void testIntersectionWithoutReduction() throws IOException {
        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsA));
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_B", uidsA));

        AndIterator itr = new AndIterator<>(includes);
        driveIterator(itr, new TreeSet<>(uidsA));
    }

    @Test
    void testIntersectionWithReduction() throws IOException {
        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsA));
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_B", uidsB));

        AndIterator itr = new AndIterator<>(includes);
        SortedSet<String> uids = intersectUids(uidsA, uidsB);
        driveIterator(itr, uids);
    }

    @Test
    void testNoIntersection() throws IOException {
        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsA));
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_C", uidsC));

        AndIterator itr = new AndIterator<>(includes);

        Set<String> fields = Sets.newHashSet("FIELD_A", "FIELD_C");
        SortedSet<String> uids = intersectUids(uidsA, uidsC);
        driveIterator(itr, uids, fields);
    }

    /**
     * Triggers the "Lookup of event field failed, precision of query reduced" warning
     *
     * @throws IOException
     *             on purpose
     */
    @Test
    void testIterationInterruptedOnInitialSeekOfEventField() throws IOException {
        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsA, false));
        includes.add(IndexIteratorBridgeTest.createInterruptibleIndexIteratorBridge("FIELD_B", uidsB, false, 1));

        AndIterator itr = new AndIterator<>(includes);

        // generates the following warning
        // "Lookup of event field failed, precision of query reduced."

        SortedSet<String> uids = new TreeSet<>(uidsA);
        assertThrows(IterationInterruptedException.class, () -> driveIterator(itr, uids));
    }

    /**
     * Triggers the "Lookup of index only term failed" warning
     *
     */
    @Test
    void testIterationInterruptedOnInitialSeekOfIndexOnlyField() {
        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsA, false));
        includes.add(IndexIteratorBridgeTest.createInterruptibleIndexIteratorBridge("FIELD_B", uidsB, true, 1));

        AndIterator itr = new AndIterator<>(includes);
        assertThrows(IterationInterruptedException.class, () -> itr.seek(new Range(), Collections.emptyList(), false));
    }

    // 1) handles the next() -> advanceIterators() path with an event field
    @Test
    void testIterationInterruptedOnNextCallEventField() throws IOException {
        SortedSet<String> uidsA = new TreeSet<>(Arrays.asList("a", "b", "c", "d", "e", "f", "g"));
        SortedSet<String> uidsB = new TreeSet<>(Arrays.asList("a", "b", "c", "d", "e", "f", "g"));
        SortedSet<String> uidsC = new TreeSet<>(Arrays.asList("a", "b", "c", "d", "e", "f", "g"));

        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsA, false)); // turning this off causes this test to fail when run as a
        includes.add(IndexIteratorBridgeTest.createInterruptibleIndexIteratorBridge("FIELD_B", uidsB, false, 4));
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_C", uidsC, false));

        AndIterator itr = new AndIterator<>(includes);
        assertThrows(IterationInterruptedException.class, () -> driveIterator(itr, intersectUids(uidsA, uidsB, uidsC)));
    }

    @Test
    void testIterationInterruptedOnNextCallAllIteratorsFail() {
        SortedSet<String> uidsA = new TreeSet<>(Arrays.asList("a", "b", "c", "d", "e", "f", "g"));
        SortedSet<String> uidsB = new TreeSet<>(Arrays.asList("a", "b", "c", "d", "e", "f", "g"));
        SortedSet<String> uidsC = new TreeSet<>(Arrays.asList("a", "b", "c", "d", "e", "f", "g"));

        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createInterruptibleIndexIteratorBridge("FIELD_A", uidsA, false, 4));
        includes.add(IndexIteratorBridgeTest.createInterruptibleIndexIteratorBridge("FIELD_B", uidsB, false, 5));
        includes.add(IndexIteratorBridgeTest.createInterruptibleIndexIteratorBridge("FIELD_C", uidsC, false, 6));

        AndIterator itr = new AndIterator<>(includes);
        SortedSet<String> uids = intersectUids(uidsA, uidsB, uidsC);
        assertThrows(IterationInterruptedException.class, () -> driveIterator(itr, uids));
    }

    // 1) handles the next() -> advanceIterators() path with an index only term
    @Test
    void testIterationInterruptedOnNextCallIndexOnlyField() {
        SortedSet<String> uidsA = new TreeSet<>(Arrays.asList("a", "b", "c", "d", "e"));
        SortedSet<String> uidsB = new TreeSet<>(Arrays.asList("a", "b", "c", "d", "e"));
        SortedSet<String> uidsC = new TreeSet<>(Arrays.asList("a", "b", "c", "d", "e"));

        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsA, true));
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_B", uidsB, true));
        includes.add(IndexIteratorBridgeTest.createInterruptibleIndexIteratorBridge("FIELD_C", uidsC, true, 4));

        AndIterator itr = new AndIterator<>(includes);

        Set<String> indexOnlyFields = Sets.newHashSet("FIELD_A", "FIELD_B", "FIELD_C");
        Set<String> droppedFields = Collections.singleton("FIELD_C");
        SortedSet<String> uids = intersectUids(uidsA, uidsB, uidsC);
        assertThrows(IterationInterruptedException.class, () -> driveIterator(itr, uids, indexOnlyFields, droppedFields));
    }

    // 2) handles the next() -> advanceIterators() path when a negation is in play
    @Test
    void testIterationInterruptedOnNextCallWithNegation() {
        SortedSet<String> uidsA = new TreeSet<>(Arrays.asList("a", "c", "e", "g", "i"));
        SortedSet<String> uidsB = new TreeSet<>(Arrays.asList("a", "c", "e", "g", "i"));
        SortedSet<String> uidsC = new TreeSet<>(Arrays.asList("b", "d", "f", "h", "j"));

        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsA, true));
        includes.add(IndexIteratorBridgeTest.createInterruptibleIndexIteratorBridge("FIELD_B", uidsB, true, 4));

        Set<NestedIterator<Key>> excludes = new HashSet<>();
        excludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_C", uidsC, false));

        AndIterator itr = new AndIterator<>(includes, excludes);

        Set<String> indexOnlyFields = Sets.newHashSet("FIELD_A", "FIELD_B");
        Set<String> droppedFields = Collections.singleton("FIELD_B");
        SortedSet<String> uids = intersectUids(uidsA, uidsB);
        assertThrows(IterationInterruptedException.class, () -> driveIterator(itr, uids, indexOnlyFields, droppedFields));
    }

    // 3) applyContextRequired -> contextIncludes are uneven and there's no high key
    @Test
    void testIterationExceptionDuringApplyContextRequired() throws IOException {
        SortedSet<String> uidsA = new TreeSet<>(Arrays.asList("a", "c", "e", "g", "i"));
        SortedSet<String> uidsB = new TreeSet<>(Arrays.asList("a", "e", "g", "i"));
        SortedSet<String> uidsC = new TreeSet<>(Arrays.asList("b", "d", "f", "h", "j"));

        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsA, true));
        includes.add(IndexIteratorBridgeTest.createInterruptibleIndexIteratorBridge("FIELD_B", uidsB, true, 4));

        Set<NestedIterator<Key>> excludes = new HashSet<>();
        excludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_C", uidsC, false));

        AndIterator itr = new AndIterator<>(includes, excludes);
        itr.setContext(new Key("20220314_17", "datatype\0a"));

        // manually initialize iterator
        itr.seek(new Range(), Collections.emptyList(), false);
        itr.initialize();

        // set context before advancing
        itr.setContext(new Key("20220314_17", "datatype\0c"));

        assertTrue(itr.hasNext());
        Object o = itr.next();
        assertTrue(o instanceof Key);
        IndexIteratorTest.assertTopKey((Key) o, "a");

        Set<String> indexOnlyFields = Sets.newHashSet("FIELD_A", "FIELD_B");
        Set<String> droppedFields = Collections.singleton("FIELD_B");
        SortedSet<String> uids = intersectUids(uidsA, uidsB);
        assertThrows(IterationInterruptedException.class, () -> driveIterator(itr, uids, indexOnlyFields, droppedFields));
    }

    @Test
    void testIntersectionOfLowCardinalityIndexOnlyTerms() throws IOException {
        int max = 100;
        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, 5);
            SortedSet<String> uidsB = randomUids(100, 10);
            SortedSet<String> uidsC = randomUids(100, 15);
            driveIntersection(uidsA, uidsB, uidsC);
        }
    }

    @Test
    void testIntersectionOfHighCardinalityIndexOnlyTerms() throws IOException {
        int max = 100;
        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, 50);
            SortedSet<String> uidsB = randomUids(100, 75);
            SortedSet<String> uidsC = randomUids(100, 85);
            driveIntersection(uidsA, uidsB, uidsC);
        }
    }

    @Test
    void testIntersectionOfVariableCardinalityIndexOnlyTerms() throws IOException {
        int max = 100;
        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, rand.nextInt(100));
            SortedSet<String> uidsB = randomUids(100, rand.nextInt(100));
            SortedSet<String> uidsC = randomUids(100, rand.nextInt(100));
            driveIntersection(uidsA, uidsB, uidsC);
        }
    }

    // === assert methods ===

    private void driveIntersection(SortedSet<String> uidsA, SortedSet<String> uidsB, SortedSet<String> uidsC) throws IOException {
        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsA, true));
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_B", uidsB, true));
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_C", uidsC, true));
        AndIterator<Key> andIterator = new AndIterator<>(includes);

        SortedSet<String> uids = intersectUids(uidsA, uidsB, uidsC);

        driveIterator(andIterator, uids);
    }

    private void driveIterator(AndIterator itr, SortedSet<String> uids) throws IOException {
        driveIterator(itr, uids, Collections.emptySet(), Collections.emptySet());
    }

    private void driveIterator(AndIterator itr, SortedSet<String> uids, Set<String> indexOnlyFields) throws IOException {
        driveIterator(itr, uids, indexOnlyFields, Collections.emptySet());
    }

    private void driveIterator(AndIterator itr, SortedSet<String> uids, Set<String> indexOnlyFields, Set<String> droppedFields) throws IOException {

        // seek iterators and initialize
        itr.seek(new Range(), Collections.emptyList(), false);
        itr.initialize();

        for (String uid : uids) {
            assertTrue(itr.hasNext());

            // assert top key
            Object o = itr.next();
            assertTrue(o instanceof Key);
            IndexIteratorTest.assertTopKey((Key) o, uid);

            // only assert fields if index only fields were present
            if (!indexOnlyFields.isEmpty()) {
                Document d = itr.document();
                IndexIteratorTest.assertDocumentUid(uid, d);

                for (String field : indexOnlyFields) {
                    // do not assert based on dropped fields
                    if (!droppedFields.contains(field)) {
                        IndexIteratorTest.assertDocumentField(field, d);
                    }
                }
            }
        }
        assertFalse(itr.hasNext());
    }

    // === helper methods ===

    private SortedSet<String> intersectUids(SortedSet<String>... uidSets) {
        Set<String> uids = null;
        for (SortedSet<String> uidSet : uidSets) {
            if (uids == null) {
                uids = Sets.newHashSet(uidSet);
            } else {
                uids = Sets.intersection(uids, uidSet);
            }
        }
        return new TreeSet<>(uids);
    }

}
