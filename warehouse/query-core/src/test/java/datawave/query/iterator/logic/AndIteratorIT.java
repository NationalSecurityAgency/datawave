package datawave.query.iterator.logic;

import static datawave.query.iterator.logic.TestUtil.assertDocumentUids;
import static datawave.query.iterator.logic.TestUtil.randomUids;
import static datawave.query.iterator.logic.TestUtil.uidFromKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
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

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

import datawave.query.attributes.Document;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.iterator.NestedIterator;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.IteratorBuildingVisitor;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.PushdownNegationVisitor;
import datawave.query.predicate.TimeFilter;

/**
 * Integration tests for the {@link AndIterator} with mocked out sources
 */
class AndIteratorIT {

    private static final Logger log = Logger.getLogger(AndIteratorIT.class);

    // first five
    private final SortedSet<String> uidsA = new TreeSet<>(Arrays.asList("a", "b", "c", "d", "e"));
    // first five, odd
    private final SortedSet<String> uidsB = new TreeSet<>(Arrays.asList("a", "c", "e", "g", "i"));

    private final Set<String> fields = Sets.newHashSet("FIELD_A", "FIELD_B", "FIELD_C", "CONTEXT");

    private final int max = 100;
    private final Random random = new Random();

    // (A && B)
    @Test
    void testIntersectionOfIncludes() {
        String query = "FIELD_A == 'value' && FIELD_B == 'value'";

        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, 5);
            SortedSet<String> uidsB = randomUids(100, 15);
            driveIntersectionOfIncludes(query, uidsA, uidsB);
        }

        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, 65);
            SortedSet<String> uidsB = randomUids(100, 75);
            driveIntersectionOfIncludes(query, uidsA, uidsB);
        }

        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, random.nextInt(100));
            SortedSet<String> uidsB = randomUids(100, random.nextInt(100));
            driveIntersectionOfIncludes(query, uidsA, uidsB);
        }
    }

    // (A && !B)
    @Test
    void testIntersectionOfIncludeAndExcludes() {
        String query = "FIELD_A == 'value' && !(FIELD_B == 'value')";

        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, 5);
            SortedSet<String> uidsB = randomUids(100, 15);
            driveIntersectionOfIncludeAndExclude(query, uidsA, uidsB);
        }

        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, 65);
            SortedSet<String> uidsB = randomUids(100, 75);
            driveIntersectionOfIncludeAndExclude(query, uidsA, uidsB);
        }

        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, random.nextInt(100));
            SortedSet<String> uidsB = randomUids(100, random.nextInt(100));
            driveIntersectionOfIncludeAndExclude(query, uidsA, uidsB);
        }
    }

    // context && !A && !B
    @Test
    void testIntersectionOfExcludes() {
        String query = "CONTEXT == 'value' && !(FIELD_A == 'value') && !(FIELD_B == 'value')";

        for (int i = 0; i < max; i++) {
            SortedSet<String> context = randomUids(100, 5);
            SortedSet<String> uidsA = randomUids(100, 10);
            SortedSet<String> uidsB = randomUids(100, 15);
            driveIntersectionOfExcludes(query, context, uidsA, uidsB);
        }

        for (int i = 0; i < max; i++) {
            SortedSet<String> context = randomUids(100, 50);
            SortedSet<String> uidsA = randomUids(100, 65);
            SortedSet<String> uidsB = randomUids(100, 75);
            driveIntersectionOfExcludes(query, context, uidsA, uidsB);
        }

        for (int i = 0; i < max; i++) {
            SortedSet<String> context = randomUids(100, random.nextInt(100));
            SortedSet<String> uidsA = randomUids(100, random.nextInt(100));
            SortedSet<String> uidsB = randomUids(100, random.nextInt(100));
            driveIntersectionOfExcludes(query, context, uidsA, uidsB);
        }
    }

    @Test
    void testIntersectionOfExcludes_case01() {
        String query = "CONTEXT == 'value' && !(FIELD_A == 'value') && !(FIELD_B == 'value')";
        SortedSet<String> context = new TreeSet<>(List.of("1", "6", "7"));
        SortedSet<String> uidsA = new TreeSet<>(List.of("1", "5", "7"));
        SortedSet<String> uidsB = new TreeSet<>(List.of("1", "3", "8"));
        driveIntersectionOfExcludes(query, context, uidsA, uidsB); // expected [6]
    }

    @Test
    void testIntersectionOfExcludes_case02() {
        String query = "CONTEXT == 'value' && !(FIELD_A == 'value') && !(FIELD_B == 'value')";
        SortedSet<String> context = new TreeSet<>(List.of("3", "6", "8"));
        SortedSet<String> uidsA = new TreeSet<>(List.of("1", "2", "6"));
        SortedSet<String> uidsB = new TreeSet<>(List.of("4", "6", "8"));
        driveIntersectionOfExcludes(query, context, uidsA, uidsB); // expected [3]
    }

    @Test
    void testIntersectionOfExcludes_case03() {
        String query = "CONTEXT == 'value' && !(FIELD_A == 'value') && !(FIELD_B == 'value')";
        SortedSet<String> context = new TreeSet<>(List.of("1", "2", "9"));
        SortedSet<String> uidsA = new TreeSet<>(List.of("2", "4", "6"));
        SortedSet<String> uidsB = new TreeSet<>(List.of("3", "4", "8"));
        driveIntersectionOfExcludes(query, context, uidsA, uidsB); // expected [1, 9]
    }

    @Test
    void testIntersectionOfExcludes_case04() {
        String query = "CONTEXT == 'value' && !(FIELD_A == 'value') && !(FIELD_B == 'value')";
        SortedSet<String> context = new TreeSet<>(List.of("1", "4", "6"));
        SortedSet<String> uidsA = new TreeSet<>(List.of("1", "4", "7"));
        SortedSet<String> uidsB = new TreeSet<>(List.of("1", "3", "6"));
        driveIntersectionOfExcludes(query, context, uidsA, uidsB); // expected [6]
    }

    @Test
    void testIntersectionOfExcludes_case05() {
        String query = "CONTEXT == 'value' && !(FIELD_A == 'value') && !(FIELD_B == 'value')";
        SortedSet<String> context = new TreeSet<>(List.of("4", "5", "8"));
        SortedSet<String> uidsA = new TreeSet<>(List.of("1", "4", "6"));
        SortedSet<String> uidsB = new TreeSet<>(List.of("5", "7", "9"));
        driveIntersectionOfExcludes(query, context, uidsA, uidsB); // expected [8]
    }

    /**
     * Triggers the "Lookup of event field failed, precision of query reduced" warning
     *
     */
    @Test
    void testIterationInterruptedOnInitialSeekOfEventField() {
        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsA, false));
        includes.add(IndexIteratorBridgeTest.createInterruptibleIndexIteratorBridge("FIELD_B", uidsB, false, 2));
        AndIterator<Key> itr = new AndIterator<>(includes);

        SortedSet<String> uids = new TreeSet<>(uidsA);

        // generates the following warning
        // "Lookup of event field failed, precision of query reduced."
        driveIterator(itr, uids);
    }

    /**
     * Triggers the "Lookup of index only term failed" warning
     *
     */
    @Test
    void testIterationInterruptedOnInitialSeekOfIndexOnlyField() {
        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsA, false));
        includes.add(IndexIteratorBridgeTest.createInterruptibleIndexIteratorBridge("FIELD_B", uidsB, true, 0));

        AndIterator<Key> itr = new AndIterator<>(includes);

        Range range = new Range();
        List<ByteSequence> columnFamilies = Collections.emptyList();
        assertThrows(DatawaveFatalQueryException.class, () -> itr.seek(range, columnFamilies, false));
    }

    @Test
    void testIterationInterruptedOnNextCallEventField() {
        SortedSet<String> uidsA = new TreeSet<>(Arrays.asList("a", "b", "c", "d", "e", "f", "g"));
        SortedSet<String> uidsB = new TreeSet<>(Arrays.asList("a", "b", "c", "d", "e", "f", "g"));
        SortedSet<String> uidsC = new TreeSet<>(Arrays.asList("a", "b", "c", "d", "e", "f", "g"));

        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsA, false));
        includes.add(IndexIteratorBridgeTest.createInterruptibleIndexIteratorBridge("FIELD_B", uidsB, false, 4));
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_C", uidsC, false));

        AndIterator<Key> itr = new AndIterator<>(includes);
        driveIterator(itr, intersectUids(uidsA, uidsB, uidsC));
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

        AndIterator<Key> itr = new AndIterator<>(includes);
        SortedSet<String> uids = intersectUids(uidsA, uidsB, uidsC);
        assertThrows(DatawaveFatalQueryException.class, () -> driveIterator(itr, uids));
    }

    @Test
    void testIterationInterruptedOnNextCallIndexOnlyField() {
        SortedSet<String> uidsA = new TreeSet<>(Arrays.asList("a", "b", "c", "d", "e"));
        SortedSet<String> uidsB = new TreeSet<>(Arrays.asList("a", "b", "c", "d", "e"));
        SortedSet<String> uidsC = new TreeSet<>(Arrays.asList("a", "b", "c", "d", "e"));

        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsA, true));
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_B", uidsB, true));
        includes.add(IndexIteratorBridgeTest.createInterruptibleIndexIteratorBridge("FIELD_C", uidsC, true, 4));
        AndIterator<Key> andIterator = new AndIterator<>(includes);

        SortedSet<String> uids = intersectUids(uidsA, uidsB, uidsC);

        Map<String,Set<String>> fieldCounts = new HashMap<>();
        fieldCounts.put("FIELD_A", uids);
        fieldCounts.put("FIELD_B", uids);
        fieldCounts.put("FIELD_C", uids);

        assertThrows(DatawaveFatalQueryException.class, () -> TestUtil.driveIterator(andIterator, uids, fieldCounts));
    }

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

        AndIterator<Key> andIterator = new AndIterator<>(includes, excludes);

        SortedSet<String> uids = intersectUids(uidsA, uidsB);

        Map<String,Set<String>> fieldCounts = new HashMap<>();
        fieldCounts.put("FIELD_A", uids);
        fieldCounts.put("FIELD_B", uids);
        fieldCounts.put("FIELD_C", uids);

        assertThrows(DatawaveFatalQueryException.class, () -> TestUtil.driveIterator(andIterator, uids, fieldCounts));
    }

    @Test
    void testIterationExceptionDuringApplyContextRequired() {
        SortedSet<String> uidsA = new TreeSet<>(Arrays.asList("a", "c", "e", "g", "i"));
        SortedSet<String> uidsB = new TreeSet<>(Arrays.asList("a", "e", "g", "i"));
        SortedSet<String> uidsC = new TreeSet<>(Arrays.asList("b", "d", "f", "h", "j"));

        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsA, true));
        includes.add(IndexIteratorBridgeTest.createInterruptibleIndexIteratorBridge("FIELD_B", uidsB, true, 3));

        Set<NestedIterator<Key>> excludes = new HashSet<>();
        excludes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_C", uidsC, false));

        AndIterator<Key> itr = new AndIterator<>(includes, excludes);

        SortedSet<String> uids = intersectUids(uidsA, uidsB);

        Map<String,Set<String>> fieldCounts = new HashMap<>();
        fieldCounts.put("FIELD_A", uids);
        fieldCounts.put("FIELD_B", uids);

        assertThrows(DatawaveFatalQueryException.class, () -> TestUtil.driveIterator(itr, uids, fieldCounts));
    }

    // (A && B && C)
    @Test
    void testLargerIntersection() {
        String query = "FIELD_A == 'value' && FIELD_B == 'value' && FIELD_C == 'value'";
        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, 5);
            SortedSet<String> uidsB = randomUids(100, 10);
            SortedSet<String> uidsC = randomUids(100, 15);
            driveIntersection(query, uidsA, uidsB, uidsC);
        }

        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, 50);
            SortedSet<String> uidsB = randomUids(100, 75);
            SortedSet<String> uidsC = randomUids(100, 85);
            driveIntersection(query, uidsA, uidsB, uidsC);
        }

        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, random.nextInt(100));
            SortedSet<String> uidsB = randomUids(100, random.nextInt(100));
            SortedSet<String> uidsC = randomUids(100, random.nextInt(100));
            driveIntersection(query, uidsA, uidsB, uidsC);
        }
    }

    @Test
    void testLargerIntersection_case01() {
        String query = "FIELD_A == 'value' && FIELD_B == 'value' && FIELD_C == 'value'";
        SortedSet<String> uidsA = new TreeSet<>(Arrays.asList("3", "6", "8"));
        SortedSet<String> uidsB = new TreeSet<>(Arrays.asList("5", "8", "9"));
        SortedSet<String> uidsC = new TreeSet<>(Arrays.asList("2", "5", "8"));
        driveIntersection(query, uidsA, uidsB, uidsC);
    }

    @Test
    void testLargerIntersection_case02() {
        String query = "FIELD_A == 'value' && FIELD_B == 'value' && FIELD_C == 'value'";
        SortedSet<String> uidsA = new TreeSet<>(Arrays.asList("2", "6", "9"));
        SortedSet<String> uidsB = new TreeSet<>(Arrays.asList("1", "2", "3"));
        SortedSet<String> uidsC = new TreeSet<>(Arrays.asList("2", "8", "9"));
        driveIntersection(query, uidsA, uidsB, uidsC);
    }

    @Test
    void testLargerIntersection_case03() {
        String query = "FIELD_A == 'value' && FIELD_B == 'value' && FIELD_C == 'value'";
        SortedSet<String> uidsA = new TreeSet<>(Arrays.asList("1", "4", "7"));
        SortedSet<String> uidsB = new TreeSet<>(Arrays.asList("1", "6", "7"));
        SortedSet<String> uidsC = new TreeSet<>(Arrays.asList("1", "7", "9"));
        driveIntersection(query, uidsA, uidsB, uidsC);
    }

    // with nested union

    // (A && (B || C)
    @Test
    void testIntersectionWithNestedUnion() {
        String query = "FIELD_A == 'value' && (FIELD_B == 'value' || FIELD_C == 'value')";
        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, 5);
            SortedSet<String> uidsB = randomUids(100, 10);
            SortedSet<String> uidsC = randomUids(100, 15);
            driveIntersectionWithNestedUnion(query, uidsA, uidsB, uidsC);
        }

        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, 50);
            SortedSet<String> uidsB = randomUids(100, 75);
            SortedSet<String> uidsC = randomUids(100, 85);
            driveIntersectionWithNestedUnion(query, uidsA, uidsB, uidsC);
        }

        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, random.nextInt(100));
            SortedSet<String> uidsB = randomUids(100, random.nextInt(100));
            SortedSet<String> uidsC = randomUids(100, random.nextInt(100));
            driveIntersectionWithNestedUnion(query, uidsA, uidsB, uidsC);
        }
    }

    @Test
    void testIntersectionWithNestedUnion_case01() {
        String query = "FIELD_A == 'value' && (FIELD_B == 'value' || FIELD_C == 'value')";
        SortedSet<String> uidsA = new TreeSet<>(List.of("1", "3", "8"));
        SortedSet<String> uidsB = new TreeSet<>(List.of("2", "5", "7"));
        SortedSet<String> uidsC = new TreeSet<>(List.of("1", "4", "8"));
        driveIntersectionWithNestedUnion(query, uidsA, uidsB, uidsC);
    }

    // with context includes

    // (A && (B || !C))
    @Test
    void testIntersectionWithContextIncludes() {
        String query = "FIELD_A == 'value' && (FIELD_B == 'value' || !(FIELD_C == 'value'))";
        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, 5);
            SortedSet<String> uidsB = randomUids(100, 10);
            SortedSet<String> uidsC = randomUids(100, 15);
            driveIntersectionWithContextIncludes(query, uidsA, uidsB, uidsC);
        }

        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, 50);
            SortedSet<String> uidsB = randomUids(100, 75);
            SortedSet<String> uidsC = randomUids(100, 85);
            driveIntersectionWithContextIncludes(query, uidsA, uidsB, uidsC);
        }

        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, random.nextInt(100));
            SortedSet<String> uidsB = randomUids(100, random.nextInt(100));
            SortedSet<String> uidsC = randomUids(100, random.nextInt(100));
            driveIntersectionWithContextIncludes(query, uidsA, uidsB, uidsC);
        }
    }

    // (A && (B || !C))
    @Test
    void testIntersectionWithContextIncludes_case01() {
        String query = "FIELD_A == 'value' && (FIELD_B == 'value' || !(FIELD_C == 'value'))";
        SortedSet<String> uidsA = new TreeSet<>(List.of("3", "5", "8"));
        SortedSet<String> uidsB = new TreeSet<>(List.of("10", "4", "5"));
        SortedSet<String> uidsC = new TreeSet<>(List.of("5", "6", "8"));
        driveIntersectionWithContextIncludes(query, uidsA, uidsB, uidsC); // expected [3, 5]
    }

    // (A && (B || !C))
    @Test
    void testIntersectionWithContextIncludes_case02() {
        String query = "FIELD_A == 'value' && (FIELD_B == 'value' || !(FIELD_C == 'value'))";
        SortedSet<String> uidsA = new TreeSet<>(List.of("4", "5", "9"));
        SortedSet<String> uidsB = new TreeSet<>(List.of("3", "8", "9"));
        SortedSet<String> uidsC = new TreeSet<>(List.of("3", "4", "8"));
        driveIntersectionWithContextIncludes(query, uidsA, uidsB, uidsC); // expected [5, 9]
    }

    // with context excludes
    // (A && (!B || !C))
    @Test
    void testIntersectionWithContextExcludes() {
        String query = "FIELD_A == 'value' && (!(FIELD_B == 'value') || !(FIELD_C == 'value'))";
        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, 5);
            SortedSet<String> uidsB = randomUids(100, 10);
            SortedSet<String> uidsC = randomUids(100, 15);
            driveIntersectionWithContextExcludes(query, uidsA, uidsB, uidsC);
        }

        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, 50);
            SortedSet<String> uidsB = randomUids(100, 75);
            SortedSet<String> uidsC = randomUids(100, 85);
            driveIntersectionWithContextExcludes(query, uidsA, uidsB, uidsC);
        }

        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, random.nextInt(100));
            SortedSet<String> uidsB = randomUids(100, random.nextInt(100));
            SortedSet<String> uidsC = randomUids(100, random.nextInt(100));
            driveIntersectionWithContextExcludes(query, uidsA, uidsB, uidsC);
        }
    }

    // A && (!B || !C)
    @Test
    void testIntersectionWithContextExcludes_case01() {
        String query = "FIELD_A == 'value' && (!(FIELD_B == 'value') || !(FIELD_C == 'value'))";
        SortedSet<String> uidsA = new TreeSet<>(List.of("5", "6", "8"));
        SortedSet<String> uidsB = new TreeSet<>(List.of("1", "3", "8"));
        SortedSet<String> uidsC = new TreeSet<>(List.of("4", "7", "8"));
        driveIntersectionWithContextExcludes(query, uidsA, uidsB, uidsC); // expected [5, 6]
    }

    // with both context includes and context excludes
    // A && (B || !C) || !(D || E)
    @Disabled
    @Test
    void testIntersectionWithContextIncludesAndContextExcludes() {
        fail("test not implemented yet");
        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, 5);
            SortedSet<String> uidsB = randomUids(100, 10);
            SortedSet<String> uidsC = randomUids(100, 15);
            // driveIntersection(uidsA, uidsB, uidsC);
        }

        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, 50);
            SortedSet<String> uidsB = randomUids(100, 75);
            SortedSet<String> uidsC = randomUids(100, 85);
            // driveIntersection(uidsA, uidsB, uidsC);
        }

        for (int i = 0; i < max; i++) {
            SortedSet<String> uidsA = randomUids(100, random.nextInt(100));
            SortedSet<String> uidsB = randomUids(100, random.nextInt(100));
            SortedSet<String> uidsC = randomUids(100, random.nextInt(100));
            // driveIntersection(uidsA, uidsB, uidsC);
        }
    }

    // === assert methods ===

    private void driveIntersectionOfIncludes(String query, SortedSet<String> uidsA, SortedSet<String> uidsB) {
        IteratorBuildingVisitorForTests visitor = getIteratorBuildingVisitor();
        visitor.putFieldUids("FIELD_A", uidsA);
        visitor.putFieldUids("FIELD_B", uidsB);

        NestedIterator<Key> iterator = visitor.getIterator(query);

        SortedSet<String> expected = new TreeSet<>(Sets.intersection(uidsA, uidsB));

        Map<String,Set<String>> counts = new HashMap<>();
        if (!expected.isEmpty()) {
            counts.put("FIELD_A", new TreeSet<>(expected));
            counts.put("FIELD_B", new TreeSet<>(expected));
        }

        TestUtil.driveIterator(iterator, expected, counts);
    }

    private void driveIntersectionOfIncludeAndExclude(String query, SortedSet<String> uidsA, SortedSet<String> uidsB) {
        IteratorBuildingVisitorForTests visitor = getIteratorBuildingVisitor();
        visitor.putFieldUids("FIELD_A", uidsA);
        visitor.putFieldUids("FIELD_B", uidsB);

        NestedIterator<Key> iterator = visitor.getIterator(query);

        SortedSet<String> expected = new TreeSet<>(Sets.difference(uidsA, uidsB));

        Map<String,Set<String>> counts = new HashMap<>();
        if (!expected.isEmpty()) {
            counts.put("FIELD_A", new TreeSet<>(expected));
        }

        TestUtil.driveIterator(iterator, expected, counts);
    }

    private void driveIntersectionOfExcludes(String query, SortedSet<String> context, SortedSet<String> uidsA, SortedSet<String> uidsB) {
        IteratorBuildingVisitorForTests visitor = getIteratorBuildingVisitor();
        visitor.putFieldUids("FIELD_A", uidsA);
        visitor.putFieldUids("FIELD_B", uidsB);
        visitor.putFieldUids("CONTEXT", context);

        NestedIterator<Key> iterator = visitor.getIterator(query);

        SortedSet<String> expected = new TreeSet<>();
        for (String ctx : context) {
            if (!uidsA.contains(ctx) && !uidsB.contains(ctx)) {
                expected.add(ctx);
            }
        }

        TestUtil.driveIterator(iterator, expected, Collections.emptyMap());
    }

    private void driveIntersection(String query, SortedSet<String> uidsA, SortedSet<String> uidsB, SortedSet<String> uidsC) {
        IteratorBuildingVisitorForTests visitor = getIteratorBuildingVisitor();
        visitor.putFieldUids("FIELD_A", uidsA);
        visitor.putFieldUids("FIELD_B", uidsB);
        visitor.putFieldUids("FIELD_C", uidsC);

        NestedIterator<Key> iterator = visitor.getIterator(query);

        SortedSet<String> uids = intersectUids(uidsA, uidsB, uidsC);

        Map<String,Set<String>> counts = new HashMap<>();
        if (!uids.isEmpty()) {
            counts.put("FIELD_A", new HashSet<>(uids));
            counts.put("FIELD_B", new HashSet<>(uids));
            counts.put("FIELD_C", new HashSet<>(uids));
        }

        TestUtil.driveIterator(iterator, uids, counts);
    }

    // (A && (B || C)
    private void driveIntersectionWithNestedUnion(String query, SortedSet<String> uidsA, SortedSet<String> uidsB, SortedSet<String> uidsC) {
        IteratorBuildingVisitorForTests visitor = getIteratorBuildingVisitor();
        visitor.putFieldUids("FIELD_A", uidsA);
        visitor.putFieldUids("FIELD_B", uidsB);
        visitor.putFieldUids("FIELD_C", uidsC);

        NestedIterator<Key> iterator = visitor.getIterator(query);

        Set<String> termA = Sets.union(Sets.intersection(uidsA, uidsB), Sets.intersection(uidsA, uidsC));
        Set<String> termB = Sets.intersection(uidsA, uidsB);
        Set<String> termC = Sets.intersection(uidsA, uidsC);

        SortedSet<String> expected = new TreeSet<>(termA);

        Map<String,Set<String>> counts = new HashMap<>();
        if (!termA.isEmpty()) {
            counts.put("FIELD_A", termA);
        }
        if (!termB.isEmpty()) {
            counts.put("FIELD_B", termB);
        }
        if (!termC.isEmpty()) {
            counts.put("FIELD_C", termC);
        }

        TestUtil.driveIterator(iterator, expected, counts);
    }

    // (A && (B || !C))
    private void driveIntersectionWithContextIncludes(String query, SortedSet<String> uidsA, SortedSet<String> uidsB, SortedSet<String> uidsC) {
        IteratorBuildingVisitorForTests visitor = getIteratorBuildingVisitor();
        visitor.putFieldUids("FIELD_A", uidsA);
        visitor.putFieldUids("FIELD_B", uidsB);
        visitor.putFieldUids("FIELD_C", uidsC);

        NestedIterator<Key> iterator = visitor.getIterator(query);

        SortedSet<String> termA = new TreeSet<>();
        SortedSet<String> termB = new TreeSet<>();
        SortedSet<String> expected = new TreeSet<>();
        for (String uid : uidsA) {
            if (uidsB.contains(uid)) {
                termA.add(uid);
                expected.add(uid);
            }
            if (!uidsC.contains(uid)) {
                termA.add(uid);
                expected.add(uid);
            }
        }

        Map<String,Set<String>> counts = new HashMap<>();
        if (!termA.isEmpty()) {
            counts.put("FIELD_A", termA);
        }
        if (!termB.isEmpty()) {
            counts.put("FIELD_B", termB);
        }

        TestUtil.driveIterator(iterator, expected, counts);
    }

    // (A && (!B || !C))
    private void driveIntersectionWithContextExcludes(String query, SortedSet<String> uidsA, SortedSet<String> uidsB, SortedSet<String> uidsC) {
        IteratorBuildingVisitorForTests visitor = getIteratorBuildingVisitor();
        visitor.putFieldUids("FIELD_A", uidsA);
        visitor.putFieldUids("FIELD_B", uidsB);
        visitor.putFieldUids("FIELD_C", uidsC);

        NestedIterator<Key> iterator = visitor.getIterator(query);

        SortedSet<String> expected = new TreeSet<>();
        for (String uid : uidsA) {
            if (!uidsB.contains(uid) || !uidsC.contains(uid)) {
                expected.add(uid);
            }
        }

        Map<String,Set<String>> counts = new HashMap<>();
        if (!expected.isEmpty()) {
            counts.put("FIELD_A", expected);
        }

        TestUtil.driveIterator(iterator, expected, counts);
    }

    private void driveIterator(AndIterator<Key> itr, SortedSet<String> uids) {
        driveIterator(itr, uids, Collections.emptyMap());
    }

    private void driveIterator(AndIterator<Key> itr, SortedSet<String> uids, Map<String,Set<String>> counts) {
        try {
            itr.seek(new Range(), Collections.emptyList(), false);
        } catch (IOException e) {
            fail("Failed to seek AndIterator during test setup");
        }

        Set<String> foundUids = new TreeSet<>();
        Map<String,Set<String>> fieldCounts = new HashMap<>();

        while (itr.hasNext()) {
            // assert top key
            Key key = itr.next();
            String uid = uidFromKey(key);
            foundUids.add(uid);

            // only assert fields if index only fields were present
            if (!counts.isEmpty()) {
                Document document = itr.document();
                assertDocumentUids(uid, document);

                for (String indexOnlyField : counts.keySet()) {
                    if (document.containsKey(indexOnlyField)) {
                        Set<String> fieldUids = fieldCounts.getOrDefault(indexOnlyField, new HashSet<>());
                        fieldUids.add(uid);
                        fieldCounts.put(indexOnlyField, fieldUids);
                    }
                }
            }
        }

        assertFalse(itr.hasNext(), "iterator had more elements");

        SortedSet<String> unexpectedUids = new TreeSet<>();
        for (String uid : foundUids) {
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

        assertTrue(uids.isEmpty(), "expected uids were not found");
        assertTrue(unexpectedUids.isEmpty(), "unexpected uids found");

        assertEquals(counts, fieldCounts, "indexOnly field counts did not match");
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

        if (uids == null) {
            return Collections.emptySortedSet();
        } else {
            return new TreeSet<>(uids);
        }
    }

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
