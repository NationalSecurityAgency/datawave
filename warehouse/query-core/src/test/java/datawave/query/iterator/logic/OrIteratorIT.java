package datawave.query.iterator.logic;

import static datawave.query.iterator.logic.TestUtil.driveIterator;
import static datawave.query.iterator.logic.TestUtil.randomUids;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

import datawave.query.iterator.NestedIterator;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.IteratorBuildingVisitor;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.PushdownNegationVisitor;
import datawave.query.predicate.TimeFilter;

/**
 * Integration tests for the {@link OrIterator} using mocked out sources
 */
class OrIteratorIT {

    private static final Logger log = Logger.getLogger(OrIteratorIT.class);

    private final SortedSet<String> uidsAll = new TreeSet<>(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "10"));
    private final SortedSet<String> uidsEven = new TreeSet<>(Arrays.asList("2", "4", "6", "8", "10"));
    private final SortedSet<String> uidsOdd = new TreeSet<>(Arrays.asList("1", "3", "5", "7", "9"));

    private final Set<String> fields = Sets.newHashSet("FIELD_A", "FIELD_B", "FIELD_C", "CONTEXT");

    private final int max = 100;

    private final Random random = new Random();

    // (A || B)
    @Test
    void testUnionOfIncludes() {
        String query = "FIELD_A == 'value' || FIELD_B == 'value'";
        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, 5);
            SortedSet<String> uidsB = randomUids(100, 15);
            driveUnionOfIncludes(query, uidsA, uidsB);
        }

        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, 65);
            SortedSet<String> uidsB = randomUids(100, 75);
            driveUnionOfIncludes(query, uidsA, uidsB);
        }

        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, random.nextInt(100));
            SortedSet<String> uidsB = randomUids(100, random.nextInt(100));
            driveUnionOfIncludes(query, uidsA, uidsB);
        }
    }

    // (A || B || C)
    @Test
    void testLargeUnionOfIncludes() {
        String query = "FIELD_A == 'value' || FIELD_B == 'value' || FIELD_C == 'value'";
        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, 5);
            SortedSet<String> uidsB = randomUids(100, 10);
            SortedSet<String> uidsC = randomUids(100, 15);
            driveUnionOfIncludes(query, uidsA, uidsB, uidsC);
        }

        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, 50);
            SortedSet<String> uidsB = randomUids(100, 75);
            SortedSet<String> uidsC = randomUids(100, 85);
            driveUnionOfIncludes(query, uidsA, uidsB, uidsC);
        }

        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, random.nextInt(100));
            SortedSet<String> uidsB = randomUids(100, random.nextInt(100));
            SortedSet<String> uidsC = randomUids(100, random.nextInt(100));
            driveUnionOfIncludes(query, uidsA, uidsB, uidsC);
        }
    }

    // context && (A || !B)
    @Test
    void testUnionOfIncludesAndExcludes() {
        String query = "CONTEXT == 'value' && (FIELD_A == 'value' || !(FIELD_B == 'value'))";
        for (int i = 0; i < max; i++) {
            SortedSet<String> context = randomUids(10, 5);
            SortedSet<String> uidsA = randomUids(100, 10);
            SortedSet<String> uidsB = randomUids(100, 15);
            driveUnionOfIncludeAndExclude(query, context, uidsA, uidsB);
        }

        for (int i = 0; i < max; i++) {
            SortedSet<String> context = randomUids(100, 50);
            SortedSet<String> uidsA = randomUids(100, 65);
            SortedSet<String> uidsB = randomUids(100, 75);
            driveUnionOfIncludeAndExclude(query, context, uidsA, uidsB);
        }

        for (int i = 0; i < max; i++) {
            SortedSet<String> context = randomUids(100, random.nextInt(100));
            SortedSet<String> uidsA = randomUids(100, random.nextInt(100));
            SortedSet<String> uidsB = randomUids(100, random.nextInt(100));
            driveUnionOfIncludeAndExclude(query, context, uidsA, uidsB);
        }
    }

    // context && (!A || !B)
    @Test
    void testUnionOfExcludes() {
        String query = "CONTEXT == 'value' && (!(FIELD_A == 'value') || !(FIELD_B == 'value'))";
        for (int i = 0; i < max; i++) {
            SortedSet<String> context = randomUids(100, 5);
            SortedSet<String> uidsA = randomUids(100, 10);
            SortedSet<String> uidsB = randomUids(100, 15);
            driveUnionOfExcludes(query, context, uidsA, uidsB);
        }

        for (int i = 0; i < max; i++) {
            SortedSet<String> context = randomUids(100, 50);
            SortedSet<String> uidsA = randomUids(100, 65);
            SortedSet<String> uidsB = randomUids(100, 75);
            driveUnionOfExcludes(query, context, uidsA, uidsB);
        }

        for (int i = 0; i < max; i++) {
            SortedSet<String> context = randomUids(100, random.nextInt(100));
            SortedSet<String> uidsA = randomUids(100, random.nextInt(100));
            SortedSet<String> uidsB = randomUids(100, random.nextInt(100));
            driveUnionOfExcludes(query, context, uidsA, uidsB);
        }
    }

    @Test
    void testUnionOfExcludes_case01() {
        String query = "CONTEXT == 'value' && (!(FIELD_A == 'value') || !(FIELD_B == 'value'))";
        SortedSet<String> context = new TreeSet<>(List.of("1", "2", "7"));
        SortedSet<String> uidsA = new TreeSet<>(List.of("10", "7", "9"));
        SortedSet<String> uidsB = new TreeSet<>(List.of("6", "7", "9"));
        driveUnionOfExcludes(query, context, uidsA, uidsB); // expected [6, 9]
    }

    // union with nested intersection
    @Test
    void testUnionWithNestedIntersectionOfIncludes() {
        String query = "CONTEXT == 'value' && (FIELD_A == 'value' || (FIELD_B == 'value' && FIELD_C == 'value'))";
        for (int i = 0; i < max; i++) {
            SortedSet<String> context = randomUids(100, 10);
            SortedSet<String> uidsA = randomUids(100, 5);
            SortedSet<String> uidsB = randomUids(100, 10);
            SortedSet<String> uidsC = randomUids(100, 15);
            driveUnionWithNestedIntersectionOfIncludes(query, context, uidsA, uidsB, uidsC);
        }

        for (int i = 0; i < max; i++) {
            SortedSet<String> context = randomUids(100, 50);
            SortedSet<String> uidsA = randomUids(100, 55);
            SortedSet<String> uidsB = randomUids(100, 65);
            SortedSet<String> uidsC = randomUids(100, 75);
            driveUnionWithNestedIntersectionOfIncludes(query, context, uidsA, uidsB, uidsC);
        }

        for (int i = 0; i < max; i++) {
            SortedSet<String> context = randomUids(100, random.nextInt(100));
            SortedSet<String> uidsA = randomUids(100, random.nextInt(100));
            SortedSet<String> uidsB = randomUids(100, random.nextInt(100));
            SortedSet<String> uidsC = randomUids(100, random.nextInt(100));
            driveUnionWithNestedIntersectionOfIncludes(query, context, uidsA, uidsB, uidsC);
        }
    }

    @Test
    void testUnionWithNestedIntersectionOfIncludes_case01() {
        String query = "CONTEXT == 'value' && (FIELD_A == 'value' || (FIELD_B == 'value' && FIELD_C == 'value'))";
        SortedSet<String> context = new TreeSet<>(List.of("10", "8", "9"));
        SortedSet<String> uidsA = new TreeSet<>(List.of("3", "6", "9"));
        SortedSet<String> uidsB = new TreeSet<>(List.of("1", "10", "4"));
        SortedSet<String> uidsC = new TreeSet<>(List.of("1", "10", "3"));
        driveUnionWithNestedIntersectionOfIncludes(query, context, uidsA, uidsB, uidsC); // expected [10, 9]
    }

    @Test
    void testUnionWithNestedIntersectionOfIncludes_case02() {
        String query = "CONTEXT == 'value' && (FIELD_A == 'value' || (FIELD_B == 'value' && FIELD_C == 'value'))";
        SortedSet<String> context = new TreeSet<>(List.of("5", "6", "9"));
        SortedSet<String> uidsA = new TreeSet<>(List.of("10", "7", "8"));
        SortedSet<String> uidsB = new TreeSet<>(List.of("10", "4", "6"));
        SortedSet<String> uidsC = new TreeSet<>(List.of("10", "6", "9"));
        driveUnionWithNestedIntersectionOfIncludes(query, context, uidsA, uidsB, uidsC); // expected [6]
    }

    @Test
    void testUnionWithNestedIntersectionOfIncludes_case03() {
        String query = "CONTEXT == 'value' && (FIELD_A == 'value' || (FIELD_B == 'value' && FIELD_C == 'value'))";
        SortedSet<String> context = new TreeSet<>(List.of("3", "5", "9"));
        SortedSet<String> uidsA = new TreeSet<>(List.of("4", "6", "7"));
        SortedSet<String> uidsB = new TreeSet<>(List.of("2", "3", "7"));
        SortedSet<String> uidsC = new TreeSet<>(List.of("10", "2", "9"));
        driveUnionWithNestedIntersectionOfIncludes(query, context, uidsA, uidsB, uidsC); // expected []
    }

    // union with nested intersection with exclude

    @Test
    void testUnionWithNestedIntersectionOfIncludeExclude() {
        String query = "CONTEXT == 'value' && (FIELD_A == 'value' || (FIELD_B == 'value' && !(FIELD_C == 'value')))";
        for (int i = 0; i < max; i++) {
            SortedSet<String> context = randomUids(100, 10);
            SortedSet<String> uidsA = randomUids(100, 5);
            SortedSet<String> uidsB = randomUids(100, 10);
            SortedSet<String> uidsC = randomUids(100, 15);
            driveUnionWithNestedIntersectionOfIncludeAndExclude(query, context, uidsA, uidsB, uidsC);
        }

        for (int i = 0; i < max; i++) {
            SortedSet<String> context = randomUids(100, 10);
            SortedSet<String> uidsA = randomUids(100, 55);
            SortedSet<String> uidsB = randomUids(100, 65);
            SortedSet<String> uidsC = randomUids(100, 75);
            driveUnionWithNestedIntersectionOfIncludeAndExclude(query, context, uidsA, uidsB, uidsC);
        }

        for (int i = 0; i < max; i++) {
            SortedSet<String> context = randomUids(100, 10);
            SortedSet<String> uidsA = randomUids(100, random.nextInt(100));
            SortedSet<String> uidsB = randomUids(100, random.nextInt(100));
            SortedSet<String> uidsC = randomUids(100, random.nextInt(100));
            driveUnionWithNestedIntersectionOfIncludeAndExclude(query, context, uidsA, uidsB, uidsC);
        }
    }

    @Test
    void testUnionWithNestedIntersectionOfIncludeExclude_case01() {
        String query = "CONTEXT == 'value' && (FIELD_A == 'value' || (FIELD_B == 'value' && !(FIELD_C == 'value')))";
        SortedSet<String> context = new TreeSet<>(List.of("4", "6", "9"));
        SortedSet<String> uidsA = new TreeSet<>(List.of("1", "10", "5"));
        SortedSet<String> uidsB = new TreeSet<>(List.of("2", "6", "9"));
        SortedSet<String> uidsC = new TreeSet<>(List.of("3", "5", "6"));
        driveUnionWithNestedIntersectionOfIncludeAndExclude(query, context, uidsA, uidsB, uidsC); // expected [9]
    }

    @Test
    void testUnionWithNestedIntersectionOfIncludeExclude_case02() {
        String query = "CONTEXT == 'value' && (FIELD_A == 'value' || (FIELD_B == 'value' && !(FIELD_C == 'value')))";
        SortedSet<String> context = new TreeSet<>(List.of("4", "7", "8"));
        SortedSet<String> uidsA = new TreeSet<>(List.of("10", "2", "8"));
        SortedSet<String> uidsB = new TreeSet<>(List.of("10", "4", "7"));
        SortedSet<String> uidsC = new TreeSet<>(List.of("2", "4", "9"));
        driveUnionWithNestedIntersectionOfIncludeAndExclude(query, context, uidsA, uidsB, uidsC); // expected [7, 8]
    }

    @Test
    void testUnionWithNestedIntersectionOfIncludeExclude_case03() {
        String query = "CONTEXT == 'value' && (FIELD_A == 'value' || (FIELD_B == 'value' && !(FIELD_C == 'value')))";
        SortedSet<String> context = new TreeSet<>(List.of("1", "10", "8"));
        SortedSet<String> uidsA = new TreeSet<>(List.of("1", "7", "8"));
        SortedSet<String> uidsB = new TreeSet<>(List.of("5", "6", "9"));
        SortedSet<String> uidsC = new TreeSet<>(List.of("10", "7", "9"));
        driveUnionWithNestedIntersectionOfIncludeAndExclude(query, context, uidsA, uidsB, uidsC); // expected [1, 8]
    }

    @Test
    void testUnionWithNestedIntersectionOfIncludeExclude_case04() {
        String query = "CONTEXT == 'value' && (FIELD_A == 'value' || (FIELD_B == 'value' && !(FIELD_C == 'value')))";
        SortedSet<String> context = new TreeSet<>(List.of("3", "4", "8"));
        SortedSet<String> uidsA = new TreeSet<>(List.of("1", "10", "3"));
        SortedSet<String> uidsB = new TreeSet<>(List.of("10", "3", "4"));
        SortedSet<String> uidsC = new TreeSet<>(List.of("3", "5", "9"));
        driveUnionWithNestedIntersectionOfIncludeAndExclude(query, context, uidsA, uidsB, uidsC); // expected [3, 4]
    }

    @Test
    void testUnionWithNestedIntersectionOfIncludeExclude_case05() {
        String query = "CONTEXT == 'value' && (FIELD_A == 'value' || (FIELD_B == 'value' && !(FIELD_C == 'value')))";
        SortedSet<String> context = new TreeSet<>(List.of("3", "5", "9"));
        SortedSet<String> uidsA = new TreeSet<>(List.of("10", "4", "5"));
        SortedSet<String> uidsB = new TreeSet<>(List.of("1", "10", "2"));
        SortedSet<String> uidsC = new TreeSet<>(List.of("1", "4", "5"));
        driveUnionWithNestedIntersectionOfIncludeAndExclude(query, context, uidsA, uidsB, uidsC); // expected [5]
    }

    @Test
    void testUnionWithNestedIntersectionOfIncludeExclude_case06() {
        String query = "CONTEXT == 'value' && (FIELD_A == 'value' || (FIELD_B == 'value' && !(FIELD_C == 'value')))";
        SortedSet<String> context = new TreeSet<>(List.of("3", "5", "9"));
        SortedSet<String> uidsA = new TreeSet<>(List.of("1", "3", "9"));
        SortedSet<String> uidsB = new TreeSet<>(List.of("10", "8", "9"));
        SortedSet<String> uidsC = new TreeSet<>(List.of("1", "5", "8"));
        driveUnionWithNestedIntersectionOfIncludeAndExclude(query, context, uidsA, uidsB, uidsC); // expected [3, 9]
    }

    // union with nested intersection of excludes. requires context to evaluate
    // A || (!B && !C)
    @Test
    void testUnionWithNestedIntersectionOfExcludes() {
        String query = "CONTEXT == 'value' && (FIELD_A == 'value' || (!(FIELD_B == 'value') && !(FIELD_C == 'value')))";
        for (int i = 0; i < max; i++) {
            SortedSet<String> context = randomUids(100, 10);
            SortedSet<String> uidsA = randomUids(100, 5);
            SortedSet<String> uidsB = randomUids(100, 10);
            SortedSet<String> uidsC = randomUids(100, 15);
            driveUnionWithNestedIntersectionOfNegatedTerms(query, context, uidsA, uidsB, uidsC);
        }

        for (int i = 0; i < max; i++) {
            SortedSet<String> context = randomUids(100, 50);
            SortedSet<String> uidsA = randomUids(100, 55);
            SortedSet<String> uidsB = randomUids(100, 65);
            SortedSet<String> uidsC = randomUids(100, 75);
            driveUnionWithNestedIntersectionOfNegatedTerms(query, context, uidsA, uidsB, uidsC);
        }

        for (int i = 0; i < max; i++) {
            SortedSet<String> context = randomUids(100, random.nextInt(100));
            SortedSet<String> uidsA = randomUids(100, random.nextInt(100));
            SortedSet<String> uidsB = randomUids(100, random.nextInt(100));
            SortedSet<String> uidsC = randomUids(100, random.nextInt(100));
            driveUnionWithNestedIntersectionOfNegatedTerms(query, context, uidsA, uidsB, uidsC);
        }
    }

    @Test
    void testUnionWithNestedIntersectionOfExcludes_case01() {
        String query = "CONTEXT == 'value' && (FIELD_A == 'value' || (!(FIELD_B == 'value') && !(FIELD_C == 'value')))";
        SortedSet<String> context = new TreeSet<>(List.of("2", "4", "7"));
        SortedSet<String> uidsA = new TreeSet<>(List.of("1", "7", "8"));
        SortedSet<String> uidsB = new TreeSet<>(List.of("5", "8", "9"));
        SortedSet<String> uidsC = new TreeSet<>(List.of("1", "2", "3"));
        driveUnionWithNestedIntersectionOfNegatedTerms(query, context, uidsA, uidsB, uidsC); // expected [4, 7]
    }

    @Test
    void testUnionWithNestedIntersectionOfExcludes_case02() {
        String query = "CONTEXT == 'value' && (FIELD_A == 'value' || (!(FIELD_B == 'value') && !(FIELD_C == 'value')))";
        SortedSet<String> context = new TreeSet<>(List.of("3", "4", "8"));
        SortedSet<String> uidsA = new TreeSet<>(List.of("1", "3", "9"));
        SortedSet<String> uidsB = new TreeSet<>(List.of("2", "5", "9"));
        SortedSet<String> uidsC = new TreeSet<>(List.of("1", "2", "4"));
        driveUnionWithNestedIntersectionOfNegatedTerms(query, context, uidsA, uidsB, uidsC); // expected [3, 8]
    }

    // TODO -- union with both context includes and context excludes
    // CONTEXT && (A || (B && !C) || (!D && !E))

    // test iteration interrupted
    @Test
    void testUnionWithIndexOnlyTermThatIsInterrupted() {
        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsEven));
        includes.add(IndexIteratorBridgeTest.createInterruptibleIndexIteratorBridge("FIELD_B", uidsOdd, true, 4));

        Map<String,Set<String>> indexOnlyCounts = new HashMap<>();
        indexOnlyCounts.put("FIELD_B", new HashSet<>(Arrays.asList("2", "4", "6", "8")));

        OrIterator<Key> itr = new OrIterator<>(includes);
        assertThrows(IterationInterruptedException.class, () -> driveIterator(itr, uidsAll, indexOnlyCounts));
    }

    // A union with a negated term cannot be evaluated by itself
    @Test
    void testUnionWithNegatedIndexOnlyTermThatIsInterrupted() {
        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsEven));

        Set<NestedIterator<Key>> excludes = new HashSet<>();
        excludes.add(IndexIteratorBridgeTest.createInterruptibleIndexIteratorBridge("FIELD_B", uidsOdd, true, 4));

        OrIterator<Key> itr = new OrIterator<>(includes, excludes);
        SortedSet<String> uids = Collections.emptySortedSet();
        Map<String,Set<String>> counts = Collections.emptyMap();
        assertThrows(IllegalStateException.class, () -> driveIterator(itr, uids, counts));
    }

    // === assert methods ===

    /**
     * Drive a union of includes, i.e. <code>A || B</code>
     *
     * @param query
     *            the query
     * @param uidsA
     *            uids for term A
     * @param uidsB
     *            uids for term B
     */
    private void driveUnionOfIncludes(String query, SortedSet<String> uidsA, SortedSet<String> uidsB) {
        IteratorBuildingVisitorForTests visitor = getIteratorBuildingVisitor();
        visitor.putFieldUids("FIELD_A", uidsA);
        visitor.putFieldUids("FIELD_B", uidsB);

        NestedIterator<Key> iterator = visitor.getIterator(query);

        SortedSet<String> expected = new TreeSet<>(Sets.union(uidsA, uidsB));

        Map<String,Set<String>> counts = new HashMap<>();
        if (!uidsA.isEmpty()) {
            counts.put("FIELD_A", new TreeSet<>(uidsA));
        }
        if (!uidsB.isEmpty()) {
            counts.put("FIELD_B", new TreeSet<>(uidsB));
        }

        TestUtil.driveIterator(iterator, expected, counts);
    }

    /**
     * Drive a larger union of includes, i.e. <code>(A || B || C)</code>
     *
     * @param query
     *            the query
     * @param uidsA
     *            uids for term A
     * @param uidsB
     *            uids for term B
     * @param uidsC
     *            uids for term C
     */
    private void driveUnionOfIncludes(String query, SortedSet<String> uidsA, SortedSet<String> uidsB, SortedSet<String> uidsC) {
        IteratorBuildingVisitorForTests visitor = getIteratorBuildingVisitor();
        visitor.putFieldUids("FIELD_A", uidsA);
        visitor.putFieldUids("FIELD_B", uidsB);
        visitor.putFieldUids("FIELD_C", uidsC);

        NestedIterator<Key> iterator = visitor.getIterator(query);

        Map<String,Set<String>> indexOnlyCounts = new HashMap<>();
        if (!uidsA.isEmpty()) {
            indexOnlyCounts.put("FIELD_A", uidsA);
        }
        if (!uidsB.isEmpty()) {
            indexOnlyCounts.put("FIELD_B", uidsB);
        }
        if (!uidsC.isEmpty()) {
            indexOnlyCounts.put("FIELD_C", uidsC);
        }

        SortedSet<String> uids = new TreeSet<>();
        uids.addAll(uidsA);
        uids.addAll(uidsB);
        uids.addAll(uidsC);

        driveIterator(iterator, uids, indexOnlyCounts);
    }

    /**
     * Drive a union of an include and exclude, i.e. <code>A || !B</code>
     *
     * @param query
     *            the query
     * @param uidsA
     *            uids for the include term
     * @param uidsB
     *            uids for the exclude term
     */
    private void driveUnionOfIncludeAndExclude(String query, SortedSet<String> context, SortedSet<String> uidsA, SortedSet<String> uidsB) {
        IteratorBuildingVisitorForTests visitor = getIteratorBuildingVisitor();
        visitor.putFieldUids("FIELD_A", uidsA);
        visitor.putFieldUids("FIELD_B", uidsB);
        visitor.putFieldUids("CONTEXT", context);
        NestedIterator<Key> iterator = visitor.getIterator(query);

        SortedSet<String> expected = new TreeSet<>();
        for (String ctx : context) {
            if (uidsA.contains(ctx) || !uidsB.contains(ctx)) {
                expected.add(ctx);
            }
        }

        Map<String,Set<String>> counts = new HashMap<>();
        Set<String> termA = Sets.intersection(context, uidsA);
        if (!termA.isEmpty()) {
            counts.put("FIELD_A", Sets.intersection(context, uidsA));
        }

        driveIterator(iterator, expected, counts);
    }

    /**
     * Drive a union of excludes, i.e.
     * <p>
     * <code>CONTEXT && (!A || !B)</code>
     *
     * @param query
     *            the query
     * @param context
     *            the context uids
     * @param uidsA
     *            term A uids
     * @param uidsB
     *            term B uids
     */
    private void driveUnionOfExcludes(String query, SortedSet<String> context, SortedSet<String> uidsA, SortedSet<String> uidsB) {
        IteratorBuildingVisitorForTests visitor = getIteratorBuildingVisitor();
        visitor.putFieldUids("FIELD_A", uidsA);
        visitor.putFieldUids("FIELD_B", uidsB);
        visitor.putFieldUids("CONTEXT", context);
        NestedIterator<Key> iterator = visitor.getIterator(query);

        SortedSet<String> expected = new TreeSet<>();
        for (String ctx : context) {
            if (!uidsA.contains(ctx) || !uidsB.contains(ctx)) {
                expected.add(ctx);
            }
        }

        driveIterator(iterator, expected, Collections.emptyMap());
    }

    /**
     * Drive a union with a nested intersection of includes, i.e.,
     * <p>
     * <code>A || (B &amp;&amp; C)</code>
     *
     * @param query
     *            the query
     * @param uidsA
     *            uids used for the union
     * @param uidsB
     *            uids used for the nested intersection
     * @param uidsC
     *            uids used for the nested intersection
     */
    private void driveUnionWithNestedIntersectionOfIncludes(String query, SortedSet<String> context, SortedSet<String> uidsA, SortedSet<String> uidsB,
                    SortedSet<String> uidsC) {
        IteratorBuildingVisitorForTests visitor = getIteratorBuildingVisitor();
        visitor.putFieldUids("FIELD_A", uidsA);
        visitor.putFieldUids("FIELD_B", uidsB);
        visitor.putFieldUids("FIELD_C", uidsC);
        visitor.putFieldUids("CONTEXT", context);

        NestedIterator<Key> iterator = visitor.getIterator(query);

        Set<String> termA = new TreeSet<>();
        Set<String> termB = new TreeSet<>();
        Set<String> termC = new TreeSet<>();

        SortedSet<String> expected = new TreeSet<>();
        for (String ctx : context) {
            if (uidsA.contains(ctx)) {
                expected.add(ctx);
                termA.add(ctx);
            }
            if (uidsB.contains(ctx) && uidsC.contains(ctx)) {
                expected.add(ctx);
                termB.add(ctx);
                termC.add(ctx);
            }
        }

        Map<String,Set<String>> counts = new HashMap<>();
        if (!termA.isEmpty()) {
            counts.put("FIELD_A", termA);
        }
        if (!termB.isEmpty() && !termC.isEmpty()) {
            counts.put("FIELD_B", termB);
            counts.put("FIELD_C", termC);
        }

        driveIterator(iterator, expected, counts);
    }

    /**
     * Drive a union with a nested intersection of includes, i.e.,
     * <p>
     * <code>A || (B &amp;&amp; !C)</code>
     *
     * @param query
     *            the query
     * @param uidsA
     *            uids used for the union
     * @param uidsB
     *            uids used for the nested intersection
     * @param uidsC
     *            uids used for the nested intersection
     */
    private void driveUnionWithNestedIntersectionOfIncludeAndExclude(String query, SortedSet<String> context, SortedSet<String> uidsA, SortedSet<String> uidsB,
                    SortedSet<String> uidsC) {
        IteratorBuildingVisitorForTests visitor = getIteratorBuildingVisitor();
        visitor.putFieldUids("FIELD_A", uidsA);
        visitor.putFieldUids("FIELD_B", uidsB);
        visitor.putFieldUids("FIELD_C", uidsC);
        visitor.putFieldUids("CONTEXT", context);

        NestedIterator<Key> iterator = visitor.getIterator(query);

        Set<String> termA = new TreeSet<>();
        Set<String> termB = new TreeSet<>();

        SortedSet<String> expected = new TreeSet<>();
        for (String ctx : context) {
            if (uidsA.contains(ctx)) {
                expected.add(ctx);
                termA.add(ctx);
            }
            if (uidsB.contains(ctx) && !uidsC.contains(ctx)) {
                expected.add(ctx);
                termB.add(ctx);
            }
        }

        Map<String,Set<String>> counts = new HashMap<>();
        if (!termA.isEmpty()) {
            counts.put("FIELD_A", termA);
        }
        if (!termB.isEmpty()) {
            counts.put("FIELD_B", termB);
        }

        driveIterator(iterator, expected, counts);
    }

    /**
     * Drive a union with a nested intersection with a negated term. Context is required to evaluate.
     * <p>
     * <code>context && (A || (!B &amp;&amp; !C))</code>
     *
     * @param query
     *            the query
     * @param context
     *            uids for the context
     * @param uidsA
     *            uids used for the union
     * @param uidsB
     *            uids used for the nested intersection
     * @param uidsC
     *            uids used for the nested intersection
     */
    private void driveUnionWithNestedIntersectionOfNegatedTerms(String query, SortedSet<String> context, SortedSet<String> uidsA, SortedSet<String> uidsB,
                    SortedSet<String> uidsC) {
        IteratorBuildingVisitorForTests visitor = getIteratorBuildingVisitor();
        visitor.putFieldUids("FIELD_A", uidsA);
        visitor.putFieldUids("FIELD_B", uidsB);
        visitor.putFieldUids("FIELD_C", uidsC);
        visitor.putFieldUids("CONTEXT", context);

        NestedIterator<Key> iterator = visitor.getIterator(query);

        SortedSet<String> expected = new TreeSet<>();
        for (String ctx : context) {
            if (uidsA.contains(ctx)) {
                expected.add(ctx);
            }
            if (!uidsB.contains(ctx) && !uidsC.contains(ctx)) {
                expected.add(ctx);
            }
        }

        Map<String,Set<String>> counts = new HashMap<>();
        Set<String> termA = Sets.intersection(context, uidsA);
        if (!termA.isEmpty()) {
            counts.put("FIELD_A", Sets.intersection(context, uidsA));
        }

        driveIterator(iterator, expected, counts);
    }

    // === test utils ===

    public IteratorBuildingVisitorForTests getIteratorBuildingVisitor() {
        return new IteratorBuildingVisitorForTests(fields);
    }

    class IteratorBuildingVisitorForTests extends IteratorBuildingVisitor {

        Map<String,SortedSet<String>> fieldToUids = new HashMap<>();

        public IteratorBuildingVisitorForTests(Set<String> fields) {
            setTimeFilter(TimeFilter.alwaysTrue());
            setRange(new Range());

            setFieldsToAggregate(fields);
            setIndexOnlyFields(fields);
            setTermFrequencyFields(fields);
        }

        public NestedIterator<Key> getIterator(String query) {
            try {
                ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);

                // validate negation pushdown
                ASTJexlScript pushed = (ASTJexlScript) PushdownNegationVisitor.pushdownNegations(script);
                assertEquals(query, JexlStringBuildingVisitor.buildQuery(pushed), "negations were not pushed all the way down for query: " + query);

                script.jjtAccept(this, null);
                return root();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected SortedKeyValueIterator<Key,Value> getSourceIterator(final ASTEQNode node, boolean negation) {
            String field = JexlASTHelper.getIdentifier(node);
            return IndexIteratorTest.createSource(field, fieldToUids.get(field));
        }

        public void putFieldUids(String field, SortedSet<String> uids) {
            fieldToUids.put(field, uids);
        }
    }
}
