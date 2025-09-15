package datawave.query.index.day;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;

public class DayIndexQueryBuilderTest {

    private final int max = 1000; // max iterations for random input tests

    private final BitSet even = BitSetFactory.create(0, 2, 4, 6, 8);
    private final BitSet odd = BitSetFactory.create(1, 3, 5, 7, 9);
    private final BitSet prime = BitSetFactory.create(2, 3, 5, 7);

    private Map<String,BitSet> shards;

    private String query;

    private final DayIndexQueryBuilder builder = new DayIndexQueryBuilder();

    private final Map<Integer,String> expected = new HashMap<>();

    private final Random rand = new Random();

    @BeforeEach
    public void before() {
        shards = new HashMap<>();
        shards.put("A == 'even'", even);
        shards.put("B == 'odd'", odd);
        shards.put("C == 'prime'", prime);
    }

    @AfterEach
    public void testAfter() {
        teardown();
    }

    private void teardown() {
        query = null;
        expected.clear();
    }

    @Test
    public void testEven() {
        setQuery("A == 'even'");
        assertQueryAtIndex(0, "A == 'even'");
        assertQueryAtIndex(2, "A == 'even'");
        assertQueryAtIndex(4, "A == 'even'");
        assertQueryAtIndex(6, "A == 'even'");
        assertQueryAtIndex(8, "A == 'even'");
    }

    @Test
    public void testOdd() {
        setQuery("B == 'odd'");
        assertQueryAtIndex(1, "B == 'odd'");
        assertQueryAtIndex(3, "B == 'odd'");
        assertQueryAtIndex(5, "B == 'odd'");
        assertQueryAtIndex(7, "B == 'odd'");
        assertQueryAtIndex(9, "B == 'odd'");
    }

    @Test
    public void testPrime() {
        setQuery("C == 'prime'");
        addExpected(2, "C == 'prime'");
        addExpected(3, "C == 'prime'");
        addExpected(5, "C == 'prime'");
        addExpected(7, "C == 'prime'");
        test();
    }

    @Test
    public void testEvenAndOdd() {
        setQuery("A == 'even' && B == 'odd'");
        // expect no intersection
        test();
    }

    @Test
    public void testEvenAndPrime() {
        setQuery("A == 'even' && C == 'prime'");
        addExpected(2, "(A == 'even' && C == 'prime')");
        test();
    }

    @Test
    public void testOddAndPrime() {
        setQuery("B == 'odd' && C == 'prime'");
        addExpected(3, "(B == 'odd' && C == 'prime')");
        addExpected(5, "(B == 'odd' && C == 'prime')");
        addExpected(7, "(B == 'odd' && C == 'prime')");
        test();
    }

    @Test
    public void testUnionEvenAndOdd() {
        setQuery("A == 'even' || B == 'odd'");
        addExpected(0, "A == 'even'");
        addExpected(1, "B == 'odd'");
        addExpected(2, "A == 'even'");
        addExpected(3, "B == 'odd'");
        addExpected(4, "A == 'even'");
        addExpected(5, "B == 'odd'");
        addExpected(6, "A == 'even'");
        addExpected(7, "B == 'odd'");
        addExpected(8, "A == 'even'");
        addExpected(9, "B == 'odd'");
        test();
    }

    @Test
    public void testUnionEvenAndPrime() {
        setQuery("A == 'even' || C == 'prime'");
        addExpected(0, "A == 'even'");
        addExpected(2, "(A == 'even' || C == 'prime')");
        addExpected(3, "C == 'prime'");
        addExpected(4, "A == 'even'");
        addExpected(5, "C == 'prime'");
        addExpected(6, "A == 'even'");
        addExpected(7, "C == 'prime'");
        addExpected(8, "A == 'even'");
        test();
    }

    @Test
    public void testUnionOddAndPrime() {
        setQuery("B == 'odd' || C == 'prime'");
        addExpected(1, "B == 'odd'");
        addExpected(2, "C == 'prime'");
        addExpected(3, "(B == 'odd' || C == 'prime')");
        addExpected(5, "(B == 'odd' || C == 'prime')");
        addExpected(7, "(B == 'odd' || C == 'prime')");
        addExpected(9, "B == 'odd'");
        test();
    }

    @Test
    public void testNestedUnion() {
        setQuery("A == 'even' && (B == 'odd' || C == 'prime')");
        addExpected(2, "(A == 'even' && C == 'prime')");
        test();
    }

    @Test
    public void testRandomIntersections() {
        for (int i = 0; i < max; i++) {
            createRandomShards("A == 'rand'", "B == 'rand'");
            setQuery("A == 'rand' && B == 'rand'");

            // set expectations
            BitSet left = shards.get("A == 'rand'");
            BitSet right = shards.get("B == 'rand'");

            int len = Math.max(left.length(), right.length());
            for (int j = 0; j < len; j++) {
                if (left.get(j) && right.get(j)) {
                    addExpected(j, "(A == 'rand' && B == 'rand')");
                }
            }

            test();
            teardown();
        }
    }

    @Test
    public void testRandomUnions() {
        for (int i = 0; i < max; i++) {
            createRandomShards("A == 'rand'", "B == 'rand'");
            setQuery("A == 'rand' || B == 'rand'");

            // set expectations
            BitSet left = shards.get("A == 'rand'");
            BitSet right = shards.get("B == 'rand'");

            int len = Math.max(left.length(), right.length());
            for (int j = 0; j < len; j++) {
                if (left.get(j) && right.get(j)) {
                    addExpected(j, "(A == 'rand' || B == 'rand')");
                } else if (left.get(j)) {
                    addExpected(j, "A == 'rand'");
                } else if (right.get(j)) {
                    addExpected(j, "B == 'rand'");
                }
            }

            test();
            teardown();
        }
    }

    // ===== some tests that verify non-executable nodes are persisted =====

    @Test
    public void testPersistNotNode() {
        setQuery("C == 'prime' && !(B == 'odd')");
        addExpectedForPrime("(C == 'prime' && !(B == 'odd'))");
        test();

        teardown();
        setQuery("C == 'prime' || !(B == 'odd')");
        addExpectedForPrime("C == 'prime'", "!(B == 'odd')");
        test();
    }

    @Test
    public void testPersistNotEqualsNode() {
        setQuery("C == 'prime' && B != 'odd'");
        addExpectedForPrime("(C == 'prime' && B != 'odd')");
        test();

        teardown();
        setQuery("C == 'prime' || B != 'odd'");
        addExpectedForPrime("C == 'prime'", "B != 'odd'");
        test();
    }

    @Test
    public void testPersistRegexEqualsNode() {
        setQuery("C == 'prime' && B =~ 'od.*'");
        addExpectedForPrime("(C == 'prime' && B =~ 'od.*')");
        test();

        teardown();
        setQuery("C == 'prime' || B =~ 'od.*'");
        addExpectedForPrime("C == 'prime'", "B =~ 'od.*'");
        test();
    }

    @Test
    public void testPersistRegexNotEqualsNode() {
        setQuery("C == 'prime' && B !~ 'od.*'");
        addExpectedForPrime("(C == 'prime' && B !~ 'od.*')");
        test();

        teardown();
        setQuery("C == 'prime' || B !~ 'od.*'");
        addExpectedForPrime("C == 'prime'", "B !~ 'od.*'");
        test();
    }

    @Test
    public void testPersistLessThanNode() {
        setQuery("C == 'prime' && B < '5'");
        addExpectedForPrime("(C == 'prime' && B < '5')");
        test();

        teardown();
        setQuery("C == 'prime' || B < '5'");
        addExpectedForPrime("C == 'prime'", "B < '5'");
        test();
    }

    @Test
    public void testPersistLessThanEqualsNode() {
        setQuery("C == 'prime' && B <= '5'");
        addExpectedForPrime("(C == 'prime' && B <= '5')");
        test();

        teardown();
        setQuery("C == 'prime' || B <= '5'");
        addExpectedForPrime("C == 'prime'", "B <= '5'");
        test();
    }

    @Test
    public void testPersistGreaterThanNode() {
        setQuery("C == 'prime' && B > '5'");
        addExpectedForPrime("(C == 'prime' && B > '5')");
        test();

        teardown();
        setQuery("C == 'prime' || B > '5'");
        addExpectedForPrime("C == 'prime'", "B > '5'");
        test();
    }

    @Test
    public void testPersistGreaterThanEqualsNode() {
        setQuery("C == 'prime' && B >= '5'");
        addExpectedForPrime("(C == 'prime' && B >= '5')");
        test();

        teardown();
        setQuery("C == 'prime' || B >= '5'");
        addExpectedForPrime("C == 'prime'", "B >= '5'");
        test();
    }

    @Test
    public void testPersistBoundedRange() {
        setQuery("C == 'prime' && ((_Bounded_ = true) && (B > 2 && B < 4))");
        addExpectedForPrime("(C == 'prime' && ((_Bounded_ = true) && (B > 2 && B < 4)))");
        test();

        teardown();
        setQuery("C == 'prime' || ((_Bounded_ = true) && (B > 2 && B < 4))");
        addExpectedForPrime("C == 'prime'", "((_Bounded_ = true) && (B > 2 && B < 4))");
        test();
    }

    @Test
    public void testPersistEvaluationOnly() {
        setQuery("C == 'prime' && ((_Eval_ = true) && B == 'odd')");
        addExpectedForPrime("(C == 'prime' && ((_Eval_ = true) && B == 'odd'))");
        test();

        teardown();
        setQuery("C == 'prime' || ((_Eval_ = true) && B == 'odd')");
        addExpectedForPrime("C == 'prime'", "((_Eval_ = true) && B == 'odd')");
        test();
    }

    @Test
    public void testPersistDropMarker() {
        setQuery("C == 'prime' && ((_Drop_ = true) && B == 'odd')");
        addExpectedForPrime("(C == 'prime' && ((_Drop_ = true) && B == 'odd'))");
        test();

        teardown();
        setQuery("C == 'prime' || ((_Drop_ = true) && B == 'odd')");
        addExpectedForPrime("C == 'prime'", "((_Drop_ = true) && B == 'odd')");
        test();
    }

    @Test
    public void testPersistExceededValue() {
        setQuery("C == 'prime' && ((_Value_ = true) && B == 'odd')");
        addExpectedForPrime("(C == 'prime' && ((_Value_ = true) && B == 'odd'))");
        test();

        teardown();
        setQuery("C == 'prime' || ((_Value_ = true) && B == 'odd')");
        addExpectedForPrime("C == 'prime'", "((_Value_ = true) && B == 'odd')");
        test();
    }

    @Test
    public void testPersistExceededTerm() {
        setQuery("C == 'prime' && ((_Term_ = true) && B == 'odd')");
        addExpectedForPrime("(C == 'prime' && ((_Term_ = true) && B == 'odd'))");
        test();

        teardown();
        setQuery("C == 'prime' || ((_Term_ = true) && B == 'odd')");
        addExpectedForPrime("C == 'prime'", "((_Term_ = true) && B == 'odd')");
        test();
    }

    @Test
    public void testPersistDelayedMarker() {
        setQuery("C == 'prime' && ((_Delayed_ = true) && B == 'odd')");
        addExpectedForPrime("(C == 'prime' && ((_Delayed_ = true) && B == 'odd'))");
        test();

        teardown();
        setQuery("C == 'prime' || ((_Delayed_ = true) && B == 'odd')");
        addExpectedForPrime("C == 'prime'", "((_Delayed_ = true) && B == 'odd')");
        test();
    }

    @Test
    public void testPersistFalseNode() {
        setQuery("C == 'prime' && false");
        // anything and false evaluates to false
        test();

        teardown();
        setQuery("C == 'prime' || false");
        addExpectedForPrime("C == 'prime'");
        test();
    }

    @Test
    public void testPersistTrueNode() {
        setQuery("C == 'prime' && true");
        addExpectedForPrime("(C == 'prime' && true)");
        test();

        teardown();
        setQuery("C == 'prime' || true");
        addExpectedForPrime("C == 'prime'", "true");
        test();
    }

    @Test
    public void testPersistFunctionNode() {
        setQuery("C == 'prime' && content:phrase(B, termOffsetMap, 'a', 'b')");
        addExpectedForPrime("(C == 'prime' && content:phrase(B, termOffsetMap, 'a', 'b'))");
        test();

        teardown();
        setQuery("C == 'prime' || content:phrase(B, termOffsetMap, 'a', 'b')");
        addExpectedForPrime("C == 'prime'", "content:phrase(B, termOffsetMap, 'a', 'b')");
        test();
    }

    @Test
    public void testPersistNullLiteral() {
        setQuery("(C == 'prime' || C == 'prime' || C == 'prime') && NULL1 == null");
        addExpectedForPrime("((C == 'prime' || C == 'prime' || C == 'prime') && NULL1 == null)");
        test();
    }

    @Test
    public void testPersistArithmetic() {
        setQuery("C == 'prime' && 1 + 1 == 2");
        addExpectedForPrime("(C == 'prime' && 1 + 1 == 2)");
        test();
    }

    /**
     * Helper method for quickly setting expectations given a prime anchor term when the query does not change
     *
     * @param query
     *            the query
     */
    private void addExpectedForPrime(String query) {
        addExpected(2, query);
        addExpected(3, query);
        addExpected(5, query);
        addExpected(7, query);
    }

    /**
     * Helper method for quickly setting expectations given a prime anchor term when the query does not change
     *
     * @param left
     *            the left predicate
     * @param right
     *            the right predicate
     */
    private void addExpectedForPrime(String left, String right) {
        addExpected(0, right);
        addExpected(1, right);
        addExpected(2, "(" + left + " || " + right + ")");
        addExpected(3, "(" + left + " || " + right + ")");
        addExpected(4, right);
        addExpected(5, "(" + left + " || " + right + ")");
        addExpected(6, right);
        addExpected(7, "(" + left + " || " + right + ")");
        addExpected(8, right);
        addExpected(9, right);
    }

    private void test() {
        assertNotNull(query);
        for (int i = 0; i < 10; i++) {
            assertQueryAtIndex(i, expected.get(i));
        }
    }

    private void assertQueryAtIndex(int index, String expected) {
        ASTJexlScript script = parse(query);
        JexlNode result = builder.buildQuery(script, shards, index);

        if (expected == null) {
            // expect a null node
            assertNull(result, "Unexpected value at index: " + index);
        } else {
            assertNotNull(result, "Expected not null at index: " + index);
            String built = JexlStringBuildingVisitor.buildQuery(result);
            assertEquals(expected, built, "Assertion failed at index: " + index);
        }
    }

    private void setQuery(String query) {
        this.query = query;
    }

    private ASTJexlScript parse(String query) {
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (ParseException e) {
            fail("Failed to parse query: " + query);
            throw new RuntimeException(e);
        }
    }

    private void addExpected(int index, String query) {
        expected.put(index, query);
    }

    private BitSet createRandomBitSet() {
        return createRandomBitSet(1 + rand.nextInt(9));
    }

    private BitSet createRandomBitSet(int num) {
        Set<Integer> integers = new HashSet<>();
        while (integers.size() < num) {
            integers.add(rand.nextInt(10));
        }

        BitSet bitSet = new BitSet();
        for (int integer : integers) {
            bitSet.set(integer);
        }
        return bitSet;
    }

    private void createRandomShards(String... terms) {
        shards.clear();
        for (String term : terms) {
            shards.put(term, createRandomBitSet());
        }
    }
}
