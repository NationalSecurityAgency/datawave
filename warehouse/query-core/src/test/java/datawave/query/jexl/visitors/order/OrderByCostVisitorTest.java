package datawave.query.jexl.visitors.order;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.Test;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.TreeEqualityVisitor;

public class OrderByCostVisitorTest {

    private Map<String,Long> fieldCounts;
    private Map<String,Long> termCounts;

    @Test
    public void testSimpleQuery() throws ParseException {
        String query = "A == '1'";
        testDefaultOrdering(query, query);
        testFieldOrdering(query, query);
        testTermOrdering(query, query);
    }

    @Test
    public void testUnionOfEqs_noChange() throws ParseException {
        String query = "A == '1' || B == '2'";
        testDefaultOrdering(query, query);
        testFieldOrdering(query, "B == '2' || A == '1'");
        testTermOrdering(query, "B == '2' || A == '1'");
    }

    @Test
    public void testIntersectionOfEqs_noChange() throws ParseException {
        String query = "A == '1' && B == '2'";
        testDefaultOrdering(query, query);
        testFieldOrdering(query, "B == '2' && A == '1'");
        testTermOrdering(query, "B == '2' && A == '1'");
    }

    @Test
    public void testNegativeCase() throws ParseException {
        String query = "A == '1' && B != '2'";
        testDefaultOrdering(query, query);
        testFieldOrdering(query, query);
        testTermOrdering(query, query);

        query = "B != '2' && A == '1'";
        String expected = "A == '1' && B != '2'";
        testDefaultOrdering(query, expected);
        testFieldOrdering(query, expected);
        testTermOrdering(query, expected);
    }

    @Test
    public void testAvoidFunctions() throws ParseException {
        String query = "A == '1' && content:phrase((B || C), termOffsetMap, 'quick', 'brown', 'fox')";
        testDefaultOrdering(query, query);
        testFieldOrdering(query, query);
        testTermOrdering(query, query);
    }

    @Test
    public void testNegatedFunctionFirst() throws ParseException {
        // even though we do not descend into function nodes (yet), a negated union of functions should get pushed to the right
        String query = "!(content:phrase(A, termOffsetMap, '1', '2') || content:phrase(B, termOffsetMap, '3', '4')) && C == '1'";
        String expected = "C == '1' && !(content:phrase(A, termOffsetMap, '1', '2') || content:phrase(B, termOffsetMap, '3', '4'))";
        testDefaultOrdering(query, expected);
        testFieldOrdering(query, expected);
        testTermOrdering(query, expected);
    }

    @Test
    public void testOrderingOfUnionSubtrees() throws ParseException {
        // For unions with ordered subtrees, assert that the order of the unions changes
        String query = "(A =~ '1' && B =~ '2') || (C == '3' && D == '4')";
        String expected = "(C == '3' && D == '4') || (A =~ '1' && B =~ '2')";
        // Regex node causes first union to move
        testDefaultOrdering(query, expected);
        testFieldOrdering(query, "(D == '4' && C == '3') || (B =~ '2' && A =~ '1')");
        testTermOrdering(query, "(D == '4' && C == '3') || (A =~ '1' && B =~ '2')");

        // Now assert that given ordered unions, order of subtrees changes
        query = "(A =~ '1' && B == '2') || (content:phrase(C, termOffsetMap, 'star', 'fox') && D == '4')";
        expected = "(B == '2' && A =~ '1') || (D == '4' && content:phrase(C, termOffsetMap, 'star', 'fox'))";
        // Regex in left intersection causes move, content function in right intersection causes move
        testDefaultOrdering(query, expected);
        testFieldOrdering(query, "(D == '4' && content:phrase(C, termOffsetMap, 'star', 'fox')) || (B == '2' && A =~ '1')");
        testTermOrdering(query, "(D == '4' && content:phrase(C, termOffsetMap, 'star', 'fox')) || (B == '2' && A =~ '1')");

        // Assert that both order of unions and subtrees changes
        query = "(A !~ '1' && B =~ '2') || (content:phrase(C, termOffsetMap, 'star', 'fox') && D == '4')";
        expected = "(D == '4' && content:phrase(C, termOffsetMap, 'star', 'fox')) || (B =~ '2' && A !~ '1')";
        testDefaultOrdering(query, expected);
        testFieldOrdering(query, expected);
        testTermOrdering(query, expected);
    }

    @Test
    public void testOrderingOfIntersectionSubtrees() throws ParseException {
        // For intersections with ordered subtrees, assert that the order of the intersections changes
        String query = "(A =~ '1' || B =~ '2') && (C == '3' || D == '4')";
        String expected = "(C == '3' || D == '4') && (A =~ '1' || B =~ '2')";
        // Regexes in left subtree causes move
        testDefaultOrdering(query, expected);
        testFieldOrdering(query, "(D == '4' || C == '3') && (B =~ '2' || A =~ '1')"); // field counts in reverse order
        testTermOrdering(query, "(D == '4' || C == '3') && (A =~ '1' || B =~ '2')");

        // Now assert that given ordered intersections, order of subtrees changes
        query = "(A != '1' || B == '2') && (C != '3' || D == '4')";
        expected = "(B == '2' || A != '1') && (D == '4' || C != '3')";
        testDefaultOrdering(query, expected);
        testFieldOrdering(query, "(D == '4' || C != '3') && (B == '2' || A != '1')");
        testTermOrdering(query, "(D == '4' || C != '3') && (B == '2' || A != '1')");

        // Assert that both order of intersections and subtrees changes
        query = "(A !~ '1' || B =~ '2') && (C != '3' || D == '4')";
        expected = "(D == '4' || C != '3') && (B =~ '2' || A !~ '1')";
        // EQ before NE, ER before NR
        testDefaultOrdering(query, expected);
        testFieldOrdering(query, expected);
        testTermOrdering(query, expected);
    }

    @Test
    public void testOrderingOfNestedJunctions() throws ParseException {
        // certain unions should sort before others given the sum of the field or term counts
        String query = "(A == '1' || B == '2' || C == '3') && (A == '1' || B == '2' || D == '4') && (A == '1' || B == '2' || E == '5')";
        testDefaultOrdering(query, query);
        testFieldOrdering(query, "(E == '5' || B == '2' || A == '1') && (D == '4' || B == '2' || A == '1') && (C == '3' || B == '2' || A == '1')");
        testTermOrdering(query, "(E == '5' || B == '2' || A == '1') && (D == '4' || B == '2' || A == '1') && (C == '3' || B == '2' || A == '1')");

        // certain intersections should sort before others given the lowest field or term count
        query = "(A == '1' && B == '2' && C == '3') || (A == '1' && B == '2' && D == '4') || (A == '1' && B == '2' && E == '5')";
        testDefaultOrdering(query, query);
        testFieldOrdering(query, "(E == '5' && B == '2' && A == '1') || (D == '4' && B == '2' && A == '1') || (C == '3' && B == '2' && A == '1')");
        testTermOrdering(query, "(E == '5' && B == '2' && A == '1') || (D == '4' && B == '2' && A == '1') || (C == '3' && B == '2' && A == '1')");
    }

    @Test
    public void testMarkerNodeSorts() throws ParseException {
        // value
        String query = "A == '1' && B == '2' && ((_Value_ = true) && (A =~ 'ba.*'))";
        testDefaultOrdering(query, query);
        testFieldOrdering(query, "B == '2' && A == '1' && ((_Value_ = true) && (A =~ 'ba.*'))");
        testTermOrdering(query, "B == '2' && A == '1' && ((_Value_ = true) && (A =~ 'ba.*'))");

        // list

        // term
        query = "A == '1' && B == '2' && ((_Term_ = true) && (_ANYFIELD_ =~ 'ba.*'))";
        testDefaultOrdering(query, query);
        testFieldOrdering(query, "B == '2' && A == '1' && ((_Term_ = true) && (_ANYFIELD_ =~ 'ba.*'))");
        testTermOrdering(query, "B == '2' && A == '1' && ((_Term_ = true) && (_ANYFIELD_ =~ 'ba.*'))");

        // bounded
        query = "A == '1' && B == '2' && ((_Bounded_ = true) && (A > '1' && A < '3'))";
        testDefaultOrdering(query, query);
        testFieldOrdering(query, "B == '2' && A == '1' && ((_Bounded_ = true) && (A > '1' && A < '3'))");
        testTermOrdering(query, "B == '2' && A == '1' && ((_Bounded_ = true) && (A > '1' && A < '3'))");

        // evaluation
        query = "A == '1' && B == '2' && ((_Eval_ = true) && (A == '1'))";
        testDefaultOrdering(query, query);
        testFieldOrdering(query, "B == '2' && A == '1' && ((_Eval_ = true) && (A == '1'))");
        testTermOrdering(query, "B == '2' && A == '1' && ((_Eval_ = true) && (A == '1'))");
    }

    // Given a set of nodes added to a query in random order, assert proper order every time
    @Test
    public void randomOrderTest() throws ParseException {
        List<JexlNode> nodes = new ArrayList<>();
        nodes.add(JexlNodeFactory.buildEQNode("A", "1"));
        nodes.add(JexlNodeFactory.buildNode((ASTNENode) null, "B", "2"));
        nodes.add(JexlNodeFactory.buildNode((ASTERNode) null, "C", "3.*"));
        nodes.add(JexlNodeFactory.buildNode((ASTNRNode) null, "D", "4.*"));
        nodes.add(JexlNodeFactory.buildNode((ASTGENode) null, "E", "5"));
        nodes.add(JexlNodeFactory.buildNode((ASTLENode) null, "F", "6"));
        nodes.add(JexlNodeFactory.buildNode((ASTGTNode) null, "G", "7"));
        nodes.add(JexlNodeFactory.buildNode((ASTLTNode) null, "H", "8"));
        nodes.add(JexlNodeFactory.buildFunctionNode("content", "phrase", "Q", "termOffsetMap", "Bond", "James", "Bond"));

        String expectedOr = "A == '1' || H < '8' || G > '7' || F <= '6' || E >= '5' || C =~ '3.*' || content:phrase(Q, 'termOffsetMap', 'Bond', 'James', 'Bond') || B != '2' || D !~ '4.*'";
        String expectedAnd = "A == '1' && H < '8' && G > '7' && F <= '6' && E >= '5' && C =~ '3.*' && content:phrase(Q, 'termOffsetMap', 'Bond', 'James', 'Bond') && B != '2' && D !~ '4.*'";

        String query;
        int numPerturbs = 100;
        for (int i = 0; i < numPerturbs; i++) {
            query = buildRandomQuery(nodes, " || ");
            testDefaultOrdering(query, expectedOr);
            testFieldOrdering(query,
                            "H < '8' || G > '7' || F <= '6' || E >= '5' || C =~ '3.*' || A == '1' || content:phrase(Q, 'termOffsetMap', 'Bond', 'James', 'Bond') || B != '2' || D !~ '4.*'");
            testTermOrdering(query, expectedOr);

            query = buildRandomQuery(nodes, " && ");
            testDefaultOrdering(query, expectedAnd);
            testFieldOrdering(query,
                            "H < '8' && G > '7' && F <= '6' && E >= '5' && C =~ '3.*' && A == '1' && content:phrase(Q, 'termOffsetMap', 'Bond', 'James', 'Bond') && B != '2' && D !~ '4.*'");
            testTermOrdering(query, expectedAnd);
        }
    }

    private String buildRandomQuery(List<JexlNode> nodes, String joiner) {
        Collections.shuffle(nodes);
        StringBuilder sb = new StringBuilder();
        Iterator<JexlNode> nodeIter = nodes.iterator();
        while (nodeIter.hasNext()) {
            sb.append(JexlStringBuildingVisitor.buildQueryWithoutParse(nodeIter.next()));
            if (nodeIter.hasNext()) {
                sb.append(joiner);
            }
        }
        return sb.toString();
    }

    @Test
    public void testModelExpansion() throws Exception {
        // F == '1' || F == '2', where F maps to A, B
        String query = "A == '1' || B == '1' || A == '2' || B == '2'";
        String expected = "A == '1' || A == '2' || B == '1' || B == '2'";
        testDefaultOrdering(query, expected);
    }

    @Test
    public void testSortingByNodeTypeWithinSameField() throws ParseException {
        String query = "A == '1' || B == '1' || A =~ 'ba.*' || B =~ 'ba.*'";
        String expected = "B == '1' || B =~ 'ba.*' || A == '1' || A =~ 'ba.*'";
        testFieldOrdering(query, expected);

        // with marker
        query = "A == '1' || B == '1' || ((_Delayed_ = true) && (A =~ 'ba.*')) || A =~ 'ba.*' || B =~ 'ba.*'";
        expected = "B == '1' || B =~ 'ba.*' || A == '1' || A =~ 'ba.*' || ((_Delayed_ = true) && (A =~ 'ba.*'))";
        testFieldOrdering(query, expected);
    }

    @Test
    public void testSortSameFieldTermAndMarker() throws ParseException {
        String query = "((_Delayed_ = true) && (A =~ 'ba.*')) || A == '1'";
        String expected = "A == '1' || ((_Delayed_ = true) && (A =~ 'ba.*'))";
        testFieldOrdering(query, expected);
    }

    @Test
    public void testSortDuplicateTerms() throws ParseException {
        String query = "A == '1' && B == '2' && A == '1' && B == '2'";
        testDefaultOrdering(query, "A == '1' && A == '1' && B == '2' && B == '2'");
        testFieldOrdering(query, "B == '2' && B == '2' && A == '1' && A == '1'");
    }

    private void testDefaultOrdering(String query, String expected) throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        ASTJexlScript ordered = OrderByCostVisitor.order(script);
        // built queries should be functionally equivalent (no lost nodes)
        assertTrue(TreeEqualityVisitor.isEqual(script, ordered));
        // ordered query string should match expected query string
        String orderedString = JexlStringBuildingVisitor.buildQueryWithoutParse(ordered);
        assertEquals(expected, orderedString);
    }

    private void testFieldOrdering(String query, String expected) throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        ASTJexlScript ordered = OrderByCostVisitor.orderByFieldCount(script, getFieldCounts());
        // built queries should be functionally equivalent (no lost nodes)
        assertTrue(TreeEqualityVisitor.isEqual(script, ordered));
        // ordered query string should match expected query string
        String orderedString = JexlStringBuildingVisitor.buildQueryWithoutParse(ordered);
        assertEquals(expected, orderedString);
    }

    private void testTermOrdering(String query, String expected) throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        ASTJexlScript ordered = OrderByCostVisitor.orderByTermCount(script, getTermCounts());
        // built queries should be functionally equivalent (no lost nodes)
        assertTrue(TreeEqualityVisitor.isEqual(script, ordered));
        // ordered query string should match expected query string
        String orderedString = JexlStringBuildingVisitor.buildQueryWithoutParse(ordered);
        assertEquals(expected, orderedString);
    }

    private Map<String,Long> getFieldCounts() {
        if (fieldCounts == null) {
            fieldCounts = getFieldCountMap();
        }
        return fieldCounts;
    }

    private Map<String,Long> getFieldCountMap() {
        Map<String,Long> counts = new HashMap<>();
        counts.put("A", 9L);
        counts.put("B", 8L);
        counts.put("C", 7L);
        counts.put("D", 6L);
        counts.put("E", 5L);
        counts.put("F", 4L); // same counts for E and F
        counts.put("G", 3L);
        counts.put("H", 2L);
        return counts;
    }

    private Map<String,Long> getTermCounts() {
        if (termCounts == null) {
            termCounts = getTermCountMap();
        }
        return termCounts;
    }

    private Map<String,Long> getTermCountMap() {
        Map<String,Long> counts = new HashMap<>();
        counts.put("A == '1'", 9L);
        counts.put("B == '2'", 8L);
        counts.put("C == '3'", 7L);
        counts.put("D == '4'", 6L);
        counts.put("E == '5'", 5L);
        counts.put("F == '6'", 5L); // same counts for E and F
        return counts;
    }

    @Test
    public void testCase() throws Exception {
        Map<String,Long> counts = new HashMap<>();
        counts.put("FIELD_A", 23L);
        counts.put("FIELD_B", 34L);
        counts.put("FIELD_C", 45L);

        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery("FIELD_C == 'v' || FIELD_B == 'v' || FIELD_A == 'v'");

        OrderByCostVisitor.orderByFieldCount(script, counts);

        String ordered = JexlStringBuildingVisitor.buildQuery(script);
        assertEquals("FIELD_A == 'v' || FIELD_B == 'v' || FIELD_C == 'v'", ordered);
    }
}
