package datawave.experimental.intersect;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Sets;

import datawave.query.jexl.JexlASTHelper;

public class UidIntersectionTest {

    private static Map<String,Set<String>> nodesToUids;

    // first five
    private static final Set<String> uids0 = Sets.newHashSet("uid0", "uid1", "uid2", "uid3", "uid4");
    // evens
    private static final Set<String> uids1 = Sets.newHashSet("uid0", "uid2", "uid4", "uid6", "uid8");
    // odds
    private static final Set<String> uids2 = Sets.newHashSet("uid1", "uid3", "uid5", "uid7", "uid9");
    // primes
    private static final Set<String> uids3 = Sets.newHashSet("uid2", "uid3", "uid5", "uid7");
    // last five
    private static final Set<String> uids4 = Sets.newHashSet("uid5", "uid6", "uid7", "uid8", "uid9");

    private static final Set<String> allUids = Sets.newHashSet("uid0", "uid1", "uid2", "uid3", "uid4", "uid5", "uid6", "uid7", "uid8", "uid9");

    private static final Set<String> ltUids = Sets.newHashSet("uid0", "uid1", "uid2", "uid3", "uid4", "uid5", "uid6");
    private static final Set<String> leUids = Sets.newHashSet("uid0", "uid1", "uid2", "uid3", "uid4", "uid5", "uid6", "uid7");
    private static final Set<String> gtUids = Sets.newHashSet("uid4", "uid5", "uid6", "uid7", "uid8", "uid9");
    private static final Set<String> geUids = Sets.newHashSet("uid3", "uid4", "uid5", "uid6", "uid7", "uid8", "uid9");

    private static final Set<String> gtltUids = Sets.newHashSet("uid4", "uid5", "uid6");

    @BeforeClass
    public static void setup() {
        nodesToUids = new HashMap<>();
        nodesToUids.put("FOO == 'bar'", uids0); // first five uids
        nodesToUids.put("FOO == 'baz'", uids1); // all even uids
        nodesToUids.put("FOO =~ 'ba.*'", uids2); // all odd uids
        nodesToUids.put("FOO == 'prime'", uids3); // prime uids
        nodesToUids.put("FOO == 'absent'", Collections.emptySet()); // no uids
        nodesToUids.put("FOO == 'last'", uids4); // last five uids

        // uids for basic operators
        nodesToUids.put("FOO != 'baz'", uids1); // NotEquals
        nodesToUids.put("FOO < '7'", ltUids); // LessThan
        nodesToUids.put("FOO > '3'", gtUids); // GreaterThan
        nodesToUids.put("FOO <= '7'", leUids); // LessThanEqual
        nodesToUids.put("FOO >= '3'", geUids); // GreaterThanEqual
        nodesToUids.put("FOO =~ 'abc.*'", allUids); // RegexEqual
        nodesToUids.put("FOO !~ 'abc.*'", uids0); // RegexNotEqual, first five

        // bounded ranges
        nodesToUids.put("(_Bounded_ = true) && (FOO > '3' && FOO < '7')", gtltUids); // bounded range (GT LT)

        // filter functions
        nodesToUids.put("filter:includeRegex(FOO,'.*abc.*')", allUids);
    }

    // test all basic operators

    @Test
    public void testEQ() {
        String query = "FOO == 'bar'";
        test(query, uids0);
    }

    @Test
    public void testNE() {
        String query = "FOO != 'baz'";
        test(query, uids1);
    }

    @Test
    public void testLT() {
        String query = "FOO < '7'";
        test(query, ltUids);
    }

    @Test
    public void testGT() {
        String query = "FOO > '3'";
        test(query, gtUids);
    }

    @Test
    public void testLE() {
        String query = "FOO <= '7'";
        test(query, leUids);
    }

    @Test
    public void testGE() {
        String query = "FOO >= '3'";
        test(query, geUids);
    }

    @Test
    public void testER() {
        String query = "FOO =~ 'abc.*'";
        test(query, allUids);
    }

    @Test
    public void testNR() {
        String query = "FOO !~ 'abc.*'";
        test(query, uids0);
    }

    // test marker nodes

    @Test
    public void testEvaluationOnly() {
        // (FOO == 'bar') is the first five uids
        // (FOO =~ 'ba.*') is all prime uids
        // evaluation only terms do not scan the field index, only take the left term uids
        String query = "FOO == 'bar' && ((_Eval_ = true) && (FOO =~ 'ba.*'))";
        test(query, uids0);
    }

    @Test
    public void testBoundedRange() {
        String query = "((_Bounded_ = true) && (FOO > '3' && FOO < '7'))";
        test(query, gtltUids);
    }

    @Test
    public void testBoundedRangeAndTerm() {
        String query = "FOO == 'bar' && ((_Bounded_ = true) && (FOO > '3' && FOO < '7'))";
        test(query, Collections.singleton("uid4"));
    }

    // test simple intersections

    @Test
    public void testEqAndEq() {
        // first five and all even
        String query = "FOO == 'bar' && FOO == 'baz'";
        test(query, Sets.newHashSet("uid0", "uid2", "uid4"));
    }

    @Test
    public void testEqAndLt() {
        // first five and all less than 7
        String query = "FOO == 'bar' && FOO < '7'";
        test(query, uids0);
    }

    @Test
    public void testEqAndGt() {
        // first five and all greater than 3
        String query = "FOO == 'bar' && FOO > '3'";
        test(query, Sets.intersection(uids0, gtUids)); // uid4
    }

    @Test
    public void testIntersectionWithEmptySide() {
        String query = "FOO == 'bar' && FOO == 'absent'";
        test(query, Collections.emptySet());
    }

    // A && !B
    @Test
    public void testIntersectionWithNegation() {
        // first five and not evens
        String query = "FOO == 'bar' && !(FOO == 'baz')";
        test(query, Sets.newHashSet("uid1", "uid3"));
    }

    // test simple unions

    // A || B
    @Test
    public void testEqOrGt() {
        // first five or all greater than 3
        String query = "FOO == 'bar' || FOO == 'baz'";
        test(query, Sets.union(uids0, uids1));
    }

    @Test
    public void testUnionWithEmptySide() {
        // first five or nothing
        String query = "FOO == 'bar' || FOO == 'absent'";
        test(query, uids0);
    }

    // A || !B
    @Ignore
    @Test
    public void testUnionWithNegation() {
        // primes or not first five
        String query = "FOO == 'bar' || !(FOO == 'prime')";
        test(query, uids0);
        // this really needs to be an IT
    }

    // test complex nested intersections

    // A && (B || C)
    @Test
    public void testIntersectionOfTermAndNestedUnion() {
        // first five and (even or odd)
        String query = "FOO == 'bar' && (FOO == 'baz' || FOO =~ 'ba.*')";
        test(query, uids0);
    }

    // A && (B || !C)
    @Test
    public void testIntersectionOfTermAndNestedUnionWithNegatedTerm() {
        // first five and (even or odd)
        String query = "FOO == 'bar' && (FOO == 'baz' || !(FOO =~ 'ba.*'))";
        test(query, Sets.newHashSet("uid0", "uid2", "uid4"));
    }

    // A && !(B || C)
    @Test
    public void testIntersectionOfTermAndNegatedNestedUnion() {
        // first five and (even or not odd)
        String query = "FOO == 'bar' && !(FOO == 'baz' || FOO == 'prime')";
        test(query, Collections.singleton("uid1"));
    }

    // A && !(B || C)
    @Test
    public void testIntersectionOfTermAndNegatedNestedUnionThatPrunesAway() {
        // first five and not (even or odd)
        String query = "FOO == 'bar' && !(FOO == 'baz' || FOO =~ 'ba.*')";
        test(query, Collections.emptySet());
    }

    // test complex nested unions

    // A || (B && C)
    @Test
    public void testUnionOfTermAndNestedIntersection() {
        String query = "FOO == 'bar' || (FOO =~ 'ba.*' && FOO == 'prime')";
        test(query, Sets.union(uids0, uids3));
    }

    // A || (B && C)
    @Test
    public void testUnionOfTermAndNestedIntersectionPrunedAway() {
        String query = "FOO == 'bar' || (FOO == 'baz' && FOO == 'absent')";
        test(query, uids0);
    }

    // A || (B && !C)
    @Test
    public void testUnionOfTermAndNestedIntersectionWithNegatedTerm() {
        String query = "FOO == 'bar' || (FOO > '3' && !(FOO == 'prime'))";
        test(query, Sets.union(uids0, Sets.newHashSet("uid6", "uid8", "uid9")));
    }

    // A || !(B && C)
    @Test
    public void testUnionOfTermAndNegatedNestedIntersection() {
        String query = "FOO == 'bar' || !(FOO > '3' && FOO < '7')";
        test(query, Sets.newHashSet("uid0", "uid1", "uid2", "uid3"));
    }

    // (A && B) || (C && D)
    @Test
    public void testUnionOfNestedIntersections() {
        // left is first five even uids, right is last five odd uids
        String query = "((FOO == 'bar' && FOO == 'baz') || (FOO == 'last' && FOO =~ 'ba.*'))";
        test(query, Sets.newHashSet("uid0", "uid2", "uid4", "uid5", "uid7", "uid9"));
    }

    // negation tests
    @Test
    public void testEqAndNotEq() {
        // first five and not even uids
        String query = "FOO == 'bar' && !(FOO == 'baz')";
        test(query, Sets.newHashSet("uid1", "uid3"));
    }

    @Test
    public void testEqAndNotEr() {
        // first five and not odd uids
        String query = "FOO == 'bar' && !(FOO =~ 'ba.*')";
        test(query, Sets.newHashSet("uid0", "uid2", "uid4"));
    }

    // test malformed query trees that contain negations

    @Test
    public void testEqAndNe() {
        // first five and not even uids
        String query = "FOO == 'bar' && FOO != 'baz'";
        test(query, Sets.newHashSet("uid1", "uid3"));
    }

    @Test
    public void testEqAndNr() {
        // all even and not first five
        String query = "FOO == 'baz' && FOO !~ 'abc.*'";
        test(query, Sets.newHashSet("uid6", "uid8"));
    }

    @Test
    public void testIntersection() {
        // intersect even uids and first five uids
        String query = "FOO == 'bar' && FOO == 'baz'";
        test(query, Sets.newHashSet("uid0", "uid2", "uid4"));
    }

    @Test
    public void testIntersection2() {
        // intersect even and odd uids
        String query = "FOO == 'baz' && FOO =~ 'ba.*'";
        test(query, Collections.emptySet());
    }

    @Test
    public void testIntersectAndNotNull() {
        // intersect uids and not empty set
        String query = "FOO == 'bar' && !(FOO == 'absent')";
        test(query, uids0);
    }

    @Test
    public void testNestedUnionOfNotNulls() {
        // intersect uids and not empty set
        String query = "FOO == 'bar' && (!(FOO == null) || !(FOO2 == null))";
        test(query, uids0);
    }

    @Test
    public void testNestedUnionOfIncludeRegex() {
        // intersect uids and not empty set
        String query = "FOO == 'bar' && (filter:includeRegex(FOO,'.*abc.*') || filter:includeRegex(FOO,'.*abc.*'))";
        test(query, uids0);
    }

    @Test
    public void testNestedUnionOfNotNullsWithEventOnly() {
        String query = "FOO == 'bar' && (!(FOO == null) || !(FOO2 == null) || EVENT_ONLY == 'ajax')";
        test(query, uids0);
    }

    private void test(String query, Set<String> expected) {
        try {
            ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);

            // regardless of what strategy is used, the final set of uids should remain the same
            UidIntersection strategy = new UidIntersection();

            Set<String> uids = strategy.intersect(script, nodesToUids);

            assertEquals(expected, uids);
        } catch (ParseException e) {
            fail("Failed to parse query: " + query);
        }
    }
}
