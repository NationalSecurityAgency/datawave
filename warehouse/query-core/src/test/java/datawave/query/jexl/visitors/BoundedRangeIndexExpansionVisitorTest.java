package datawave.query.jexl.visitors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.Test;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.visitors.BaseIndexExpansionVisitor.FutureJexlNode;

/**
 * Validate different ways the query tree is rewritten after expansion
 */
public class BoundedRangeIndexExpansionVisitorTest {

    @Test
    public void testBoundedRangeExpandsIntoSingleTerm() {
        String query = "((_Bounded_ = true) && (F >= '2' && F <= '3'))";
        ASTJexlScript script = parse(query);

        JexlNode first = script.jjtGetChild(0);
        assertNode(first, "((_Bounded_ = true) && (F >= '2' && F <= '3'))");

        JexlNode second = first.jjtGetChild(0);
        assertNode(second, "(_Bounded_ = true) && (F >= '2' && F <= '3')");

        JexlNode expanded = JexlNodeFactory.buildEQNode("F", "2.4");
        FutureJexlNode futureJexlNode = createFutureJexlNode(second, expanded);
        connectNode(futureJexlNode);

        assertNode(script, "(F == '2.4')");
    }

    @Test
    public void testBoundedRangeExpandsIntoMultipleTerms() {
        String query = "((_Bounded_ = true) && (F >= '2' && F <= '3'))";
        ASTJexlScript script = parse(query);

        JexlNode first = script.jjtGetChild(0);
        assertNode(first, "((_Bounded_ = true) && (F >= '2' && F <= '3'))");

        JexlNode second = first.jjtGetChild(0);
        assertNode(second, "(_Bounded_ = true) && (F >= '2' && F <= '3')");

        JexlNode expanded = parse("F == '2.4' || F == '2.6'").jjtGetChild(0);
        assertInstanceOf(ASTOrNode.class, expanded);
        FutureJexlNode futureJexlNode = createFutureJexlNode(second, expanded);
        connectNode(futureJexlNode);

        assertNode(script, "(F == '2.4' || F == '2.6')");
    }

    @Test
    public void testIntersectionWithBoundedRangeExpandsIntoSingleTerm() {
        String query = "F == 'a' && ((_Bounded_ = true) && (F >= '2' && F <= '3'))";
        ASTJexlScript script = parse(query);

        JexlNode first = script.jjtGetChild(0);
        assertNode(first, "F == 'a' && ((_Bounded_ = true) && (F >= '2' && F <= '3'))");

        JexlNode second = first.jjtGetChild(1);
        assertNode(second, "((_Bounded_ = true) && (F >= '2' && F <= '3'))");

        JexlNode third = second.jjtGetChild(0);
        assertNode(third, "(_Bounded_ = true) && (F >= '2' && F <= '3')");

        JexlNode expanded = JexlNodeFactory.buildEQNode("F", "2.4");
        FutureJexlNode futureJexlNode = createFutureJexlNode(third, expanded);
        connectNode(futureJexlNode);

        assertNode(script, "F == 'a' && (F == '2.4')");
    }

    @Test
    public void testUnionWithBoundedRangeExpandsIntoSingleTerm() {
        String query = "F == 'a' || ((_Bounded_ = true) && (F >= '2' && F <= '3'))";
        ASTJexlScript script = parse(query);

        JexlNode first = script.jjtGetChild(0);
        assertNode(first, "F == 'a' || ((_Bounded_ = true) && (F >= '2' && F <= '3'))");

        JexlNode second = first.jjtGetChild(1);
        assertNode(second, "((_Bounded_ = true) && (F >= '2' && F <= '3'))");

        JexlNode third = second.jjtGetChild(0);
        assertNode(third, "(_Bounded_ = true) && (F >= '2' && F <= '3')");

        JexlNode expanded = JexlNodeFactory.buildEQNode("F", "2.4");
        FutureJexlNode futureJexlNode = createFutureJexlNode(third, expanded);
        connectNode(futureJexlNode);

        assertNode(script, "F == 'a' || (F == '2.4')");
    }

    @Test
    public void testIntersectionWithBoundedRangeExpandsIntoMultipleTerms() {
        String query = "F == 'a' && ((_Bounded_ = true) && (F >= '2' && F <= '3'))";
        ASTJexlScript script = parse(query);

        JexlNode first = script.jjtGetChild(0);
        assertNode(first, "F == 'a' && ((_Bounded_ = true) && (F >= '2' && F <= '3'))");

        JexlNode second = first.jjtGetChild(1);
        assertNode(second, "((_Bounded_ = true) && (F >= '2' && F <= '3'))");

        JexlNode third = second.jjtGetChild(0);
        assertNode(third, "(_Bounded_ = true) && (F >= '2' && F <= '3')");

        JexlNode expanded = parse("F == '2.4' || F == '2.6'").jjtGetChild(0);
        assertInstanceOf(ASTOrNode.class, expanded);
        FutureJexlNode futureJexlNode = createFutureJexlNode(third, expanded);
        connectNode(futureJexlNode);

        assertNode(script, "F == 'a' && (F == '2.4' || F == '2.6')");
    }

    @Test
    public void testUnionWithBoundedRangeExpandsIntoMultipleTerms() {
        String query = "F == 'a' || ((_Bounded_ = true) && (F >= '2' && F <= '3'))";
        ASTJexlScript script = parse(query);

        JexlNode first = script.jjtGetChild(0);
        assertNode(first, "F == 'a' || ((_Bounded_ = true) && (F >= '2' && F <= '3'))");

        JexlNode second = first.jjtGetChild(1);
        assertNode(second, "((_Bounded_ = true) && (F >= '2' && F <= '3'))");

        JexlNode third = second.jjtGetChild(0);
        assertNode(third, "(_Bounded_ = true) && (F >= '2' && F <= '3')");

        JexlNode expanded = parse("F == '2.4' || F == '2.6'").jjtGetChild(0);
        assertInstanceOf(ASTOrNode.class, expanded);
        FutureJexlNode futureJexlNode = createFutureJexlNode(third, expanded);
        connectNode(futureJexlNode);

        assertNode(script, "F == 'a' || (F == '2.4' || F == '2.6')");
    }

    private ASTJexlScript parse(String query) {
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (ParseException e) {
            fail("Failed to parse query: " + query, e);
            throw new RuntimeException(e);
        }
    }

    private FutureJexlNode createFutureJexlNode(JexlNode orig, JexlNode expanded) {
        FutureJexlNode futureNode = new FutureJexlNode(orig, null, false, true);
        futureNode.jjtSetParent(orig.jjtGetParent());
        futureNode.setRebuiltNode(expanded);
        return futureNode;
    }

    private void assertNode(JexlNode node, String expected) {
        String query = JexlStringBuildingVisitor.buildQuery(node);
        assertEquals(expected, query);
    }

    private void connectNode(FutureJexlNode futureNode) {
        JexlNode node = futureNode.getOrigNode();
        JexlNodes.replaceChild(node.jjtGetParent(), node, futureNode.getRebuiltNode());
    }
}
