package datawave.query.iterator.logic;

import com.google.common.collect.Sets;
import datawave.query.iterator.NestedIterator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Set of integration tests for intersection-driven queries with nested unions
 */
class AndOrIteratorIT {

    private final SortedSet<String> uidsAll = new TreeSet<>(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9"));
    private final SortedSet<String> uidsEven = new TreeSet<>(Arrays.asList("2", "4", "6", "8"));
    private final SortedSet<String> uidsOdd = new TreeSet<>(Arrays.asList("1", "3", "5", "7", "9"));
    private final SortedSet<String> uidsPrime = new TreeSet<>(Arrays.asList("1", "2", "3", "5", "7"));

    private final Random rand = new Random();

    @Test
    void testSimpleNestedUnion() throws IOException {
        Set<NestedIterator<Key>> unionIncludes = new HashSet<>();
        unionIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_B", uidsEven));
        unionIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_C", uidsOdd));
        OrIterator union = new OrIterator(unionIncludes);

        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsAll));
        includes.add(union);

        // uids
        SortedSet<String> uids = union(uidsEven, uidsOdd);
        uids = intersect(uids, uidsAll);

        AndIterator itr = new AndIterator(includes);
        driveIterator(itr, uids);
    }

    @Test
    void testNestedOddAndPrime() throws IOException {
        Set<NestedIterator<Key>> unionIncludes = new HashSet<>();
        unionIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_B", uidsOdd));
        unionIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_C", uidsPrime));
        OrIterator union = new OrIterator(unionIncludes);

        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsAll));
        includes.add(union);

        // uids
        SortedSet<String> uids = union(uidsOdd, uidsPrime);
        uids = intersect(uids, uidsAll);

        AndIterator itr = new AndIterator(includes);
        driveIterator(itr, uids);
    }

    // A && (B || !C) ==> (A && B) || (A && !C)
    @Test
    void testNestedUnionWithNegatedTerm() throws IOException {
        NestedIterator<Key> include = IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_B", uidsAll);
        NestedIterator<Key> exclude = IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_C", uidsEven);
        OrIterator union = new OrIterator(Collections.singleton(include), Collections.singleton(exclude));

        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsPrime));
        includes.add(union);

        // uids built using DeMorgan's Law
        SortedSet<String> uids = union(intersect(uidsPrime, uidsAll), negate(uidsPrime, uidsEven));

        AndIterator itr = new AndIterator(includes);
        driveIterator(itr, uids);
    }

    // negated union
    // A && !(B || C) ==> (A && !B) || (A && !C)
    @Test
    void testNestedUnionOfNegatedTerms() throws IOException {
        Set<NestedIterator<Key>> excludes = new HashSet<>();
        excludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_B", uidsPrime));
        excludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_C", uidsOdd));
        OrIterator union = new OrIterator(excludes);

        OrIteratorIT.seekIterators(excludes);

        NestedIterator<Key> include = IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsAll);

        // uids built using DeMorgan's Law
        SortedSet<String> uids = union(negate(uidsAll, uidsPrime), negate(uidsAll, uidsOdd));

        AndIterator itr = new AndIterator(Collections.singleton(include), Collections.singleton(union));
        driveIterator(itr, uids);
    }

    // double nested union
    // (A && B) || (C && D)
    @Test
    void testSimpleDoubleNestedUnion() throws IOException {
        Set<NestedIterator<Key>> leftIncludes = new HashSet<>();
        leftIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_B", uidsEven));
        leftIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_C", uidsAll));
        OrIterator leftUnion = new OrIterator(leftIncludes);

        Set<NestedIterator<Key>> rightIncludes = new HashSet<>();
        rightIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_B", uidsOdd));
        rightIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_C", uidsAll));
        OrIterator rightUnion = new OrIterator(rightIncludes);

        // init unions
        OrIteratorIT.seekIterators(leftIncludes);
        OrIteratorIT.seekIterators(rightIncludes);

        AndIterator itr = new AndIterator(Sets.newHashSet(leftUnion, rightUnion));
        driveIterator(itr, uidsAll);
    }

    @Test
    void testAllRandomUids() throws IOException {
        SortedSet<String> uidsA = randomUids(50);
        SortedSet<String> uidsB = randomUids(50);
        SortedSet<String> uidsC = randomUids(50);

        Set<NestedIterator<Key>> unionIncludes = new HashSet<>();
        unionIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_B", uidsB));
        unionIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_C", uidsC));
        OrIterator union = new OrIterator(unionIncludes);

        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsA));
        includes.add(union);

        // uids
        SortedSet<String> uids = union(uidsB, uidsC);
        uids = intersect(uids, uidsA);

        AndIterator itr = new AndIterator(includes);
        driveIterator(itr, uids);
    }

    // === test interrupted exceptions ===

    // === test index only exceptions ===

    @Test
    void borge() {
        AndIterator intersection = null;
        OrIterator union = null;
    }

    // junction type - and, or
    // leaf type - junction, iterator
    // index state - indexed, index-only
    // negation state - binary
    // uid type - all, even, odd

    // === assert methods ===

    private void driveIterator(AndIterator itr, SortedSet<String> uids) throws IOException {
        driveIterator(itr, uids, Collections.emptySet(), Collections.emptySet());
    }

    private void driveIterator(AndIterator itr, SortedSet<String> uids, Set<String> indexOnlyFields) throws IOException {
        driveIterator(itr, uids, indexOnlyFields, Collections.emptySet());
    }

    private void driveIterator(AndIterator itr, SortedSet<String> uids, Set<String> indexOnlyFields, Set<String> droppedFields) throws IOException {
        itr.seek(new Range(), Collections.emptyList(), false);
        itr.initialize();

        for (String uid : uids) {

            assertTrue(itr.hasNext());

            Object o = itr.next();
            assertTrue(o instanceof Key);
            IndexIteratorTest.assertTopKey((Key) o, uid);

            // TODO -- document field assertions
        }

        assertFalse(itr.hasNext(), "Iterator had more elements: " + itr.next());
    }

    // === helper methods ===

    private SortedSet<String> intersect(SortedSet<String>... uidSets) {
        Iterator<SortedSet<String>> iter = Arrays.stream(uidSets).iterator();
        Set<String> intersected = iter.next();
        while (iter.hasNext()) {
            intersected = Sets.intersection(intersected, iter.next());
        }
        return new TreeSet<>(intersected);
    }

    private SortedSet<String> union(SortedSet<String>... uidSets) {
        SortedSet<String> allUids = new TreeSet<>();
        for (SortedSet<String> uidSet : uidSets) {
            allUids.addAll(uidSet);
        }
        return allUids;
    }

    private SortedSet<String> negate(SortedSet<String> uids, SortedSet<String> negatedUids) {
        uids.removeAll(negatedUids);
        return uids;
    }

    private SortedSet<String> randomUids(int numUids) {
        return randomUids(100, numUids);
    }

    private SortedSet<String> randomUids(int bound, int numUids) {
        SortedSet<String> uids = new TreeSet<>();

        while (uids.size() < numUids) {
            int i = 1 + rand.nextInt(bound);
            uids.add(String.valueOf(i));
        }

        return uids;
    }
}
