package datawave.query.jexl.visitors.order;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

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
import datawave.query.jexl.visitors.order.OrderByCostVisitor;

public class OrderByCostVisitorTest {

    @Test
    public void testSimpleQuery() throws ParseException {
        String query = "A == '1'";
        test(query, query);
    }

    @Test
    public void testUnionOfEqs_noChange() throws ParseException {
        String query = "A == '1' || B == '2'";
        test(query, query);
    }

    @Test
    public void testIntersectionOfEqs_noChange() throws ParseException {
        String query = "A == '1' && B == '2'";
        test(query, query);
    }

    @Test
    public void testNegativeCase() throws ParseException {
        String query = "A == '1' && B != '2'";
        test(query, query);

        query = "B != '2' && A == '1'";
        String expected = "A == '1' && B != '2'";
        test(query, expected);
    }

    @Test
    public void testAvoidFunctions() throws ParseException {
        String query = "A == '1' && content:phrase((B || C), termOffsetMap, 'quick', 'brown', 'fox')";
        test(query, query);
    }

    @Test
    public void testNegatedFunctionFirst() throws ParseException {
        String query = "!(content:phrase(A, termOffsetMap, '1', '2') || content:phrase(B, termOffsetMap, '3', '4')) && C == '1'";
        String expected = "C == '1' && !(content:phrase(A, termOffsetMap, '1', '2') || content:phrase(B, termOffsetMap, '3', '4'))";
        test(query, expected);
    }

    @Test
    public void testOrderingOfUnionSubtrees() throws ParseException {
        // For unions with ordered subtrees, assert that the order of the unions changes
        String query = "(A =~ '1' && B =~ '2') || (C == '3' && D == '4')";
        String expected = "(C == '3' && D == '4') || (A =~ '1' && B =~ '2')";
        // Regex node causes first union to move
        test(query, expected);

        // Now assert that given ordered unions, order of subtrees changes
        query = "(A =~ '1' && B == '2') || (content:phrase(C, termOffsetMap, 'star', 'fox') && D == '4')";
        expected = "(B == '2' && A =~ '1') || (D == '4' && content:phrase(C, termOffsetMap, 'star', 'fox'))";
        // Regex in left intersection causes move, content function in right intersection causes move
        test(query, expected);

        // Assert that both order of unions and subtrees changes
        query = "(A !~ '1' && B =~ '2') || (content:phrase(C, termOffsetMap, 'star', 'fox') && D == '4')";
        expected = "(D == '4' && content:phrase(C, termOffsetMap, 'star', 'fox')) || (B =~ '2' && A !~ '1')";
        test(query, expected);
    }

    @Test
    public void testOrderingOfIntersectionSubtrees() throws ParseException {
        // For intersections with ordered subtrees, assert that the order of the intersections changes
        String query = "(A =~ '1' || B =~ '2') && (C == '3' || D == '4')";
        String expected = "(C == '3' || D == '4') && (A =~ '1' || B =~ '2')";
        // Regexes in left subtree causes move
        test(query, expected);

        // Now assert that given ordered intersections, order of subtrees changes
        query = "(A != '1' || B == '2') && (C != '3' || D == '4')";
        expected = "(B == '2' || A != '1') && (D == '4' || C != '3')";
        test(query, expected);

        // Assert that both order of intersections and subtrees changes
        query = "(A !~ '1' || B =~ '2') && (C != '3' || D == '4')";
        expected = "(D == '4' || C != '3') && (B =~ '2' || A !~ '1')";
        // EQ before NE, ER before NR
        test(query, expected);
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

        String expectedOr = "A == '1' || B != '2' || H < '8' || G > '7' || F <= '6' || E >= '5' || C =~ '3.*' || D !~ '4.*' || content:phrase(Q, 'termOffsetMap', 'Bond', 'James', 'Bond')";
        String expectedAnd = "A == '1' && B != '2' && H < '8' && G > '7' && F <= '6' && E >= '5' && C =~ '3.*' && D !~ '4.*' && content:phrase(Q, 'termOffsetMap', 'Bond', 'James', 'Bond')";

        String query;
        int numPerturbs = 100;
        for (int i = 0; i < numPerturbs; i++) {
            query = buildRandomQuery(nodes, " || ");
            test(query, expectedOr);

            query = buildRandomQuery(nodes, " && ");
            test(query, expectedAnd);
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

    private void test(String query, String expected) throws ParseException {

        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);

        ASTJexlScript ordered = OrderByCostVisitor.order(script);

        // Built queries should be functionally equivalent
        assertTrue(TreeEqualityVisitor.isEqual(script, ordered));

        // Built query string should match the expected query string
        String orderedString = JexlStringBuildingVisitor.buildQueryWithoutParse(ordered);
        assertEquals(expected, orderedString);
    }
}
