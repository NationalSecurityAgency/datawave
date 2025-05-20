package datawave.query.iterator.logic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.iterator.NestedIterator;

/**
 * Set of integration tests for intersection-driven queries with nested unions
 */
class AndOrIteratorIT {

    // elements will sort lexicographically, not numerically
    private final SortedSet<String> uidsAll = new TreeSet<>(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"));
    private final SortedSet<String> uidsEven = new TreeSet<>(Arrays.asList("2", "4", "6", "8", "10"));
    private final SortedSet<String> uidsOdd = new TreeSet<>(Arrays.asList("1", "3", "5", "7", "9", "11"));
    private final SortedSet<String> uidsPrime = new TreeSet<>(Arrays.asList("1", "2", "3", "5", "7", "11"));

    private final Random rand = new Random();

    private static final Logger log = Logger.getLogger(AndOrIteratorIT.class);

    @Test
    void testSimpleNestedUnion() throws IOException {
        Set<NestedIterator<Key>> unionIncludes = new HashSet<>();
        unionIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_B", uidsEven));
        unionIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_C", uidsOdd));
        OrIterator union = new OrIterator(unionIncludes);

        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsAll));
        includes.add(union);

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

    // A && (B || !C) ==> (A && B) || (A && !C)
    @Test
    void testNestedUnionWithNegatedTermBuildDocument() throws IOException {
        NestedIterator<Key> include = IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_B", uidsAll, true);
        NestedIterator<Key> exclude = IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_C", uidsEven, true);
        OrIterator union = new OrIterator(Collections.singleton(include), Collections.singleton(exclude));

        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsPrime, true));
        includes.add(union);

        // uids built using DeMorgan's Law
        final SortedSet<String> uids = union(intersect(uidsPrime, uidsAll), negate(uidsPrime, uidsEven));

        final Map<String,Integer> indexOnlyCounts = new HashMap<>();
        indexOnlyCounts.put("FIELD_A", 6);
        indexOnlyCounts.put("FIELD_B", 6);

        AndIterator itr = new AndIterator(includes);
        driveIterator(itr, uids, indexOnlyCounts);
    }

    // negated union
    // A && !(B || C) ==> (A && !B) || (A && !C)
    @Test
    void testNestedUnionOfNegatedTerms() throws IOException {
        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_B", uidsPrime));
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_C", uidsOdd));
        OrIterator union = new OrIterator(includes);

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

        AndIterator itr = new AndIterator(Sets.newHashSet(leftUnion, rightUnion));
        driveIterator(itr, uidsAll);
    }

    // A && (B || C)
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

        SortedSet<String> uids = union(uidsB, uidsC);
        uids = intersect(uids, uidsA);

        AndIterator itr = new AndIterator(includes);
        driveIterator(itr, uids);
    }

    // === test interrupted exceptions ===

    // A && (B_interrupted || C)
    @Test
    void testInterruptedExceptionDuringInitSubtree() {
        Set<NestedIterator<Key>> unionIncludes = new HashSet<>();
        unionIncludes.add(IndexIteratorBridgeTest.createInterruptibleIndexIteratorBridge("FIELD_B", uidsEven, 2));
        unionIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_C", uidsOdd));
        OrIterator union = new OrIterator(unionIncludes);

        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsAll));
        includes.add(union);

        final SortedSet<String> uids = intersect(union(uidsEven, uidsOdd), uidsAll);

        AndIterator itr = new AndIterator(includes);
        assertThrows(IterationInterruptedException.class, () -> driveIterator(itr, uids));
    }

    @Test
    void testInterruptedExceptionInNestedUnion() throws IOException {
        Set<NestedIterator<Key>> unionIncludes = new HashSet<>();
        unionIncludes.add(IndexIteratorBridgeTest.createInterruptibleIndexIteratorBridge("FIELD_B", uidsEven, 4));
        unionIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_C", uidsOdd));
        OrIterator union = new OrIterator(unionIncludes);

        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsAll));
        includes.add(union);

        final SortedSet<String> uids = intersect(union(uidsEven, uidsOdd), uidsAll);

        AndIterator itr = new AndIterator(includes);
        assertThrows(IterationInterruptedException.class, () -> driveIterator(itr, uids));
    }

    @Test
    void testInterruptedExceptionInAnchorTerm() throws IOException {
        Set<NestedIterator<Key>> unionIncludes = new HashSet<>();
        unionIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_B", uidsEven));
        unionIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_C", uidsOdd));
        OrIterator union = new OrIterator(unionIncludes);

        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createInterruptibleIndexIteratorBridge("FIELD_A", uidsAll, 3));
        includes.add(union);

        final SortedSet<String> uids = intersect(union(uidsEven, uidsOdd), uidsAll);

        AndIterator itr = new AndIterator(includes);
        assertThrows(IterationInterruptedException.class, () -> driveIterator(itr, uids));
    }

    // === test index only exceptions ===

    @Test
    void testIndexOnlyAnchorTermAssertions() throws IOException {
        Set<NestedIterator<Key>> unionIncludes = new HashSet<>();
        unionIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_B", uidsEven));
        unionIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_C", uidsOdd));
        OrIterator union = new OrIterator(unionIncludes);

        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsAll, true));
        includes.add(union);

        SortedSet<String> uids = union(uidsEven, uidsOdd);
        uids = intersect(uids, uidsAll);

        Map<String,Integer> indexOnlyCounts = new HashMap<>();
        indexOnlyCounts.put("FIELD_A", 11);

        AndIterator itr = new AndIterator(includes);
        driveIterator(itr, uids, indexOnlyCounts);
    }

    @Test
    void testIndexOnlyNestedUnionTermAssertions() throws IOException {
        Set<NestedIterator<Key>> unionIncludes = new HashSet<>();
        unionIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_B", uidsEven));
        unionIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_C", uidsOdd, true));
        OrIterator union = new OrIterator(unionIncludes);

        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsAll, true));
        includes.add(union);

        SortedSet<String> uids = union(uidsEven, uidsOdd);
        uids = intersect(uids, uidsAll);

        Map<String,Integer> indexOnlyCounts = new HashMap<>();
        indexOnlyCounts.put("FIELD_A", 11);
        indexOnlyCounts.put("FIELD_C", 6);

        AndIterator itr = new AndIterator(includes);
        driveIterator(itr, uids, indexOnlyCounts);
    }

    @Test
    void testIndexOnlyIterationExceptionInAnchorTerm() {
        Set<NestedIterator<Key>> unionIncludes = new HashSet<>();
        unionIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_B", uidsEven));
        unionIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_C", uidsOdd));
        OrIterator union = new OrIterator(unionIncludes);

        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createInterruptibleIndexIteratorBridge("FIELD_A", uidsAll, true, 4));
        includes.add(union);

        final SortedSet<String> uids = intersect(union(uidsEven, uidsOdd), uidsAll);

        AndIterator itr = new AndIterator(includes);
        assertThrows(IterationInterruptedException.class, () -> driveIterator(itr, uids));
    }

    @Test
    void testIndexOnlyIterationExceptionInNestedUnion() {
        Set<NestedIterator<Key>> unionIncludes = new HashSet<>();
        unionIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_B", uidsEven));
        unionIncludes.add(IndexIteratorBridgeTest.createInterruptibleIndexIteratorBridge("FIELD_C", uidsOdd, true, 4));
        OrIterator union = new OrIterator(unionIncludes);

        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsAll, true));
        includes.add(union);

        final SortedSet<String> uids = intersect(union(uidsEven, uidsOdd), uidsAll);

        Map<String,Integer> indexOnlyCounts = new HashMap<>();
        indexOnlyCounts.put("FIELD_A", 11);
        indexOnlyCounts.put("FIELD_C", 1); // compare to #testIndexOnlyNestedUnionTermAssertions

        AndIterator itr = new AndIterator(includes);
        assertThrows(IterationInterruptedException.class, () -> driveIterator(itr, uids, indexOnlyCounts));
    }

    @Test
    void testWholeUnionOfIndexOnlyTermsPrunedViaInterruptedExceptions() {
        Set<NestedIterator<Key>> unionIncludes = new HashSet<>();
        unionIncludes.add(IndexIteratorBridgeTest.createInterruptibleIndexIteratorBridge("FIELD_B", uidsEven, true, 5));
        unionIncludes.add(IndexIteratorBridgeTest.createInterruptibleIndexIteratorBridge("FIELD_C", uidsOdd, true, 4));
        OrIterator union = new OrIterator(unionIncludes);

        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsAll, true));
        includes.add(union);

        final SortedSet<String> uids = intersect(union(uidsEven, uidsOdd), uidsAll);

        Map<String,Integer> indexOnlyCounts = new HashMap<>();
        indexOnlyCounts.put("FIELD_A", 11);
        indexOnlyCounts.put("FIELD_B", 2);
        indexOnlyCounts.put("FIELD_C", 1);

        AndIterator itr = new AndIterator(includes);
        assertThrows(IterationInterruptedException.class, () -> driveIterator(itr, uids, indexOnlyCounts));
    }

    // nested union with negated term that is interrupted

    // A && (B || !C)
    @Test
    void testNestedUnionWithNegatedIndexOnlyTermIsInterrupted() {
        Set<NestedIterator<Key>> unionIncludes = new HashSet<>();
        unionIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_B", uidsAll, true));

        Set<NestedIterator<Key>> unionExcludes = new HashSet<>();
        unionExcludes.add(IndexIteratorBridgeTest.createInterruptibleIndexIteratorBridge("FIELD_C", uidsAll, true, 4));

        OrIterator union = new OrIterator(unionIncludes, unionExcludes);

        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsAll, true));
        includes.add(union);

        Map<String,Integer> indexOnlyCounts = new HashMap<>();
        indexOnlyCounts.put("FIELD_A", 1);

        AndIterator itr = new AndIterator(includes);
        assertThrows(IterationInterruptedException.class, () -> driveIterator(itr, uidsAll, indexOnlyCounts));
    }

    // nested union of negated terms which is interrupted
    // A && !(B || C)
    @Test
    void testNegatedNestedUnionOfIndexOnlyFieldsIsInterrupted() {
        Set<NestedIterator<Key>> unionIncludes = new HashSet<>();
        unionIncludes.add(IndexIteratorBridgeTest.createInterruptibleIndexIteratorBridge("FIELD_B", uidsOdd, true, 4));
        unionIncludes.add(IndexIteratorBridgeTest.createInterruptibleIndexIteratorBridge("FIELD_C", uidsOdd, true, 4));
        OrIterator union = new OrIterator(unionIncludes);

        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsAll, true));

        Map<String,Integer> indexOnlyCounts = new HashMap<>();
        indexOnlyCounts.put("FIELD_A", 5);

        AndIterator itr = new AndIterator(includes, Collections.singleton(union));
        assertThrows(IterationInterruptedException.class, () -> driveIterator(itr, uidsEven, indexOnlyCounts));
    }

    // === test by volume ===

    /**
     * For when you absolutely positively gotta make sure nothing slipped through the cracks
     */
    // A && (B || C)
    @Test
    void testByVolumeIntersectionWithNestedUnion() throws IOException {
        int max = 100;
        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(50);
            SortedSet<String> uidsB = randomUids(50);
            SortedSet<String> uidsC = randomUids(50);

            // build uids using De Morgans
            SortedSet<String> leftUids = intersect(uidsA, uidsB);
            SortedSet<String> rightUids = intersect(uidsA, uidsC);
            final SortedSet<String> expectedUids = union(leftUids, rightUids);

            Map<String,Integer> expectedFieldCounts = new HashMap<>();
            expectedFieldCounts.put("FIELD_A", uidsA.size());
            expectedFieldCounts.put("FIELD_B", leftUids.size());
            expectedFieldCounts.put("FIELD_C", rightUids.size());

            driveIntersectionWithSimpleNestedUnion(uidsA, uidsB, uidsC, expectedUids, expectedFieldCounts);
        }
    }

    // Several edge cases exists where the following query will skip documents and cause valid hits to be missed.
    // A fix will be made later, this test exists to document the error condition and validate any future fixes.
    // A && (B || !C)
    @Disabled
    @Test
    void testByVolumeIntersectionWithNestedUnionThatContainsANegation() throws IOException {
        int max = 100;
        for (int i = 0; i < max; i++) {
            // increase the bound and add some variety to the number of uids
            SortedSet<String> uidsA = randomUids(10, 5);
            SortedSet<String> uidsB = randomUids(10, 3);
            SortedSet<String> uidsC = randomUids(10, 3);

            SortedSet<String> countA = new TreeSet<>(uidsA);
            countA.removeAll(uidsC);
            countA = union(countA, intersect(uidsA, uidsB));

            SortedSet<String> countB = intersect(uidsA, uidsB);

            Map<String,Integer> expectedFieldCounts = new HashMap<>();
            expectedFieldCounts.put("FIELD_A", countA.size());
            expectedFieldCounts.put("FIELD_B", countB.size());

            log.info("uids a: " + uidsA);
            log.info("uids b: " + uidsB);
            log.info("uids c: " + uidsC);
            log.info("expected uids: " + countA);

            driveIntersectionWithSimpleNestedUnionWithNegatedTerm(uidsA, uidsB, uidsC, countA, expectedFieldCounts);
        }
    }

    // A && !(B || C)
    @Test
    void testByVolumeIntersectionWithNestedUnionOfNegatedTerms() throws IOException {
        int max = 100;
        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, 50);
            SortedSet<String> uidsB = randomUids(100, 30);
            SortedSet<String> uidsC = randomUids(100, 15);

            Set<String> uidsToRemove = union(uidsB, uidsC);
            SortedSet<String> uids = new TreeSet<>(uidsA);
            uids.removeAll(uidsToRemove);

            Map<String,Integer> expectedFieldCounts = new HashMap<>();
            expectedFieldCounts.put("FIELD_A", uids.size());

            driveIntersectionWithNestedUnionOfNegatedTerms(uidsA, uidsB, uidsC, uids, expectedFieldCounts);
        }
    }

    private void driveIntersectionWithSimpleNestedUnion(SortedSet<String> uidsA, SortedSet<String> uidsB, SortedSet<String> uidsC,
                    SortedSet<String> expectedUids, Map<String,Integer> expectedFieldCounts) throws IOException {
        Set<NestedIterator<Key>> unionIncludes = new HashSet<>();
        unionIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_B", uidsB, true));
        unionIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_C", uidsC, true));
        OrIterator union = new OrIterator(unionIncludes);

        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsA, true));
        includes.add(union);

        AndIterator itr = new AndIterator(includes);
        driveIterator(itr, expectedUids, expectedFieldCounts);
    }

    // A && (B || !C)
    private void driveIntersectionWithSimpleNestedUnionWithNegatedTerm(SortedSet<String> uidsA, SortedSet<String> uidsB, SortedSet<String> uidsC,
                    SortedSet<String> expectedUids, Map<String,Integer> expectedFieldCounts) throws IOException {
        Set<NestedIterator<Key>> unionIncludes = new HashSet<>();
        unionIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_B", uidsB, true));

        Set<NestedIterator<Key>> unionExcludes = new HashSet<>();
        unionExcludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_C", uidsC, true));

        OrIterator union = new OrIterator(unionIncludes, unionExcludes);

        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsA, true));
        includes.add(union);

        AndIterator itr = new AndIterator(includes);
        driveIterator(itr, expectedUids, expectedFieldCounts);
    }

    // A && !(B || C)
    private void driveIntersectionWithNestedUnionOfNegatedTerms(SortedSet<String> uidsA, SortedSet<String> uidsB, SortedSet<String> uidsC,
                    SortedSet<String> expectedUids, Map<String,Integer> expectedFieldCounts) throws IOException {
        Set<NestedIterator<Key>> unionIncludes = new HashSet<>();
        unionIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_B", uidsB, true));
        unionIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_C", uidsC, true));

        OrIterator union = new OrIterator(unionIncludes);
        Set<NestedIterator<Key>> excludes = Collections.singleton(union);

        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsA, true));

        AndIterator itr = new AndIterator(includes, excludes);
        driveIterator(itr, expectedUids, expectedFieldCounts);
    }

    // === test cases discovered via random tests ===

    // "4" is missing. "8" is correctly evaluated via context excludes, once the b-term include is exhausted. This is an initial state problem.
    // A && (B || !C)
    @Disabled
    @Test
    void testCase01() throws IOException {
        SortedSet<String> uidsA = new TreeSet<>(Arrays.asList("10", "4", "6", "7", "8"));
        SortedSet<String> uidsB = new TreeSet<>(Arrays.asList("1", "2", "7"));
        SortedSet<String> uidsC = new TreeSet<>(Arrays.asList("10", "6", "7"));

        SortedSet<String> countA = new TreeSet<>(uidsA);
        countA.removeAll(uidsC);
        countA = union(countA, intersect(uidsA, uidsB));

        SortedSet<String> countB = intersect(uidsA, uidsB);

        Map<String,Integer> fieldIndexCounts = new HashMap<>();
        fieldIndexCounts.put("FIELD_A", countA.size());
        fieldIndexCounts.put("FIELD_B", countB.size());

        log.info("uids a: " + uidsA);
        log.info("uids b: " + uidsB);
        log.info("uids c: " + uidsC);
        log.info("expected uids: " + countA);

        driveIntersectionWithSimpleNestedUnionWithNegatedTerm(uidsA, uidsB, uidsC, countA, fieldIndexCounts);
    }

    // expected is 1, 4, 8
    // term 1 was missed
    // A && (B || !C)
    @Test
    void testCase02() throws IOException {
        SortedSet<String> uidsA = new TreeSet<>(Arrays.asList("1", "4", "5", "7", "8"));
        SortedSet<String> uidsB = new TreeSet<>(Arrays.asList("2", "3", "4"));
        SortedSet<String> uidsC = new TreeSet<>(Arrays.asList("3", "5", "7"));

        Set<NestedIterator<Key>> unionIncludes = new HashSet<>();
        unionIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_B", uidsB, true));

        Set<NestedIterator<Key>> unionExcludes = new HashSet<>();
        unionExcludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_C", uidsC, true));

        OrIterator union = new OrIterator(unionIncludes, unionExcludes);

        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsA, true));
        includes.add(union);

        SortedSet<String> countA = new TreeSet<>(uidsA);
        countA.removeAll(uidsC);
        countA = union(countA, intersect(uidsA, uidsB));

        SortedSet<String> countB = intersect(uidsA, uidsB);

        Map<String,Integer> fieldIndexCounts = new HashMap<>();
        fieldIndexCounts.put("FIELD_A", countA.size());
        fieldIndexCounts.put("FIELD_B", countB.size());

        AndIterator itr = new AndIterator(includes);
        driveIterator(itr, countA, fieldIndexCounts);
    }

    // originally missed 3, 5
    @Disabled
    @Test
    void testCase04() throws IOException {
        SortedSet<String> uidsA = new TreeSet<>(Arrays.asList("1", "3", "5", "7", "9"));
        SortedSet<String> uidsB = new TreeSet<>(Arrays.asList("10", "6", "9"));
        SortedSet<String> uidsC = new TreeSet<>(Arrays.asList("1", "10", "2"));

        SortedSet<String> countA = new TreeSet<>(uidsA);
        countA.removeAll(uidsC);
        countA = union(countA, intersect(uidsA, uidsB)); // expected is 3, 5, 7, 9

        SortedSet<String> countB = intersect(uidsA, uidsB);

        Map<String,Integer> fieldIndexCounts = new HashMap<>();
        fieldIndexCounts.put("FIELD_A", countA.size());
        fieldIndexCounts.put("FIELD_B", countB.size());

        log.info("uids a: " + uidsA);
        log.info("uids b: " + uidsB);
        log.info("uids c: " + uidsC);
        log.info("expected uids: " + countA);

        driveIntersectionWithSimpleNestedUnionWithNegatedTerm(uidsA, uidsB, uidsC, countA, fieldIndexCounts);
    }

    // contrived case where the B term never intersects, the negated C term is exhausted early but should contribute to all future A-terms
    @Disabled
    @Test
    void testCase05() throws IOException {
        SortedSet<String> uidsA = new TreeSet<>(Arrays.asList("1", "3", "5", "7", "9"));
        SortedSet<String> uidsB = new TreeSet<>(Arrays.asList("2", "4", "6", "8"));
        SortedSet<String> uidsC = new TreeSet<>(Arrays.asList("1"));

        SortedSet<String> countA = new TreeSet<>(uidsA);
        countA.removeAll(uidsC);
        countA = union(countA, intersect(uidsA, uidsB));

        SortedSet<String> countB = intersect(uidsA, uidsB);

        Map<String,Integer> fieldIndexCounts = new HashMap<>();
        fieldIndexCounts.put("FIELD_A", countA.size());
        fieldIndexCounts.put("FIELD_B", countB.size());

        log.info("uids a: " + uidsA);
        log.info("uids b: " + uidsB);
        log.info("uids c: " + uidsC);
        log.info("expected uids: " + countA);

        driveIntersectionWithSimpleNestedUnionWithNegatedTerm(uidsA, uidsB, uidsC, countA, fieldIndexCounts);
    }

    // expected = 1,4,5,7
    // did not find 4, 7
    // A && (B || !C)
    @Disabled
    @Test
    void testCase06() throws IOException {
        SortedSet<String> uidsA = new TreeSet<>(Arrays.asList("1", "10", "4", "5", "7"));
        SortedSet<String> uidsB = new TreeSet<>(Arrays.asList("1", "5", "9"));
        SortedSet<String> uidsC = new TreeSet<>(Arrays.asList("10", "2", "9"));

        SortedSet<String> countA = new TreeSet<>(uidsA);
        countA.removeAll(uidsC);
        countA = union(countA, intersect(uidsA, uidsB));

        SortedSet<String> countB = intersect(uidsA, uidsB);

        Map<String,Integer> fieldIndexCounts = new HashMap<>();
        fieldIndexCounts.put("FIELD_A", countA.size());
        fieldIndexCounts.put("FIELD_B", countB.size());

        log.info("uids a: " + uidsA);
        log.info("uids b: " + uidsB);
        log.info("uids c: " + uidsC);
        log.info("expected uids: " + countA);

        driveIntersectionWithSimpleNestedUnionWithNegatedTerm(uidsA, uidsB, uidsC, countA, fieldIndexCounts);
    }

    // contrived case where the B-term exhausts early, the C-term never excludes any A-term
    @Disabled
    @Test
    void testCase08() throws IOException {
        SortedSet<String> uidsA = new TreeSet<>(Arrays.asList("1", "3", "5", "7", "9"));
        SortedSet<String> uidsB = new TreeSet<>(Arrays.asList("2", "4", "6", "8"));
        SortedSet<String> uidsC = new TreeSet<>(Arrays.asList("1"));

        SortedSet<String> countA = new TreeSet<>(uidsA);
        countA.removeAll(uidsC);
        countA = union(countA, intersect(uidsA, uidsB));

        SortedSet<String> countB = intersect(uidsA, uidsB);

        Map<String,Integer> fieldIndexCounts = new HashMap<>();
        fieldIndexCounts.put("FIELD_A", countA.size());
        fieldIndexCounts.put("FIELD_B", countB.size());

        log.info("uids a: " + uidsA);
        log.info("uids b: " + uidsB);
        log.info("uids c: " + uidsC);
        log.info("expected uids: " + countA);

        driveIntersectionWithSimpleNestedUnionWithNegatedTerm(uidsA, uidsB, uidsC, countA, fieldIndexCounts);
    }

    // This test should log a warning and continue because the non-event iterator is exhausted and no longer contributes
    // to the non-event state of the nested union. In order for this case to be properly exercised, exhausted iterators should
    // be removed from the original set of includes and context includes
    // A && (B || C || D)
    @Disabled
    @Test
    void testEdgeCaseThatShouldFlipAFatalExceptionToAWarning_exception() throws Exception {
        SortedSet<String> sortedNonEventUids = new TreeSet<>(List.of("1", "2"));
        Set<NestedIterator<Key>> orIncludes = new HashSet<>();
        orIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_B", sortedNonEventUids, true));
        orIncludes.add(IndexIteratorBridgeTest.createInterruptibleIndexIteratorBridge("FIELD_C", uidsAll, false, 5));
        orIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_D", uidsAll, false));

        OrIterator or = new OrIterator<>(orIncludes);

        Set<NestedIterator<Key>> andIncludes = new HashSet<>();
        andIncludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsAll));
        andIncludes.add(or);

        AndIterator and = new AndIterator<>(andIncludes);

        Map<String,Integer> counts = new HashMap<>();
        counts.put("FIELD_A", uidsAll.size());
        counts.put("FIELD_B", 2);
        counts.put("FIELD_C", 5);
        counts.put("FIELD_D", uidsAll.size());

        driveIterator(and, new TreeSet<>(uidsAll), counts);
    }

    // === assert methods ===

    private void driveIterator(AndIterator itr, SortedSet<String> uids) throws IOException {
        driveIterator(itr, uids, Collections.emptyMap());
    }

    private void driveIterator(AndIterator itr, SortedSet<String> uids, Map<String,Integer> indexOnlyCounts) throws IOException {

        if (uids.isEmpty()) {
            // random tests can generate non-intersecting data
            return;
        }

        itr.seek(new Range(), Collections.emptyList(), false);
        itr.initialize();

        Set<String> seenUids = new HashSet<>();
        Map<String,Integer> counts = new HashMap<>();
        IOException caughtException = null;

        try {
            while (itr.hasNext()) {
                Object o = itr.next();
                assertTrue(o instanceof Key);
                seenUids.add(uidFromKey((Key) o));

                if (!indexOnlyCounts.isEmpty()) {
                    Document d = itr.document();

                    for (String indexOnlyField : indexOnlyCounts.keySet()) {
                        if (d.containsKey(indexOnlyField)) {
                            int i = counts.getOrDefault(indexOnlyField, 0);
                            counts.put(indexOnlyField, ++i);
                        }
                    }

                    assertDocumentUids(d, uidFromKey((Key) o));
                }
            }
        } catch (Exception e) {
            if (e instanceof IOException) {
                caughtException = (IOException) e;
            } else {
                throw e;
            }
        }

        assertFalse(itr.hasNext(), "Iterator had more elements: " + itr.next());

        if (caughtException != null) {
            throw caughtException;
        }

        SortedSet<String> unexpectedUids = new TreeSet<>();
        for (String uid : seenUids) {
            if (!uids.remove(uid)) {
                unexpectedUids.add(uid);
            }
        }

        if (!uids.isEmpty()) {
            log.warn("expected uids were not found: " + uids);
        }

        if (!unexpectedUids.isEmpty()) {
            log.warn("unexpected uids were found: " + unexpectedUids);
        }

        assertTrue(uids.isEmpty(), "expected uids were not found: " + uids);
        assertTrue(unexpectedUids.isEmpty(), "unexpected uids found: " + unexpectedUids);
    }

    private void assertDocumentUids(Document d, String uid) {
        for (Map.Entry<String,Attribute<? extends Comparable<?>>> entry : d.entrySet()) {
            Attribute attr = entry.getValue();
            assertAttributeUid(attr, uid);
        }
    }

    private void assertAttributeUid(Attribute attr, String uid) {
        if (attr instanceof Attributes) {
            Attributes attributes = (Attributes) attr;
            for (Attribute attribute : attributes.getAttributes()) {
                assertAttributeUid(attribute, uid);
            }
        }

        String docUid = uidFromKey(attr.getMetadata());
        assertEquals(uid, docUid, "expected " + uid + " but found " + docUid);
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

    private String uidFromKey(Key key) {
        String cf = key.getColumnFamily().toString();
        return cf.split("\0")[1];
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
