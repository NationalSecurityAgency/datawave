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

/**
 * Validate different ways the query tree is rewritten after expansion
 */
public class RegexIndexExpansionVisitorTest {

    @Test
    public void testRegexExpandsIntoSingleTerm() {
        String query = "F =~ 'a.*'";
        ASTJexlScript script = parse(query);

        JexlNode first = script.jjtGetChild(0);
        assertNode(first, "F =~ 'a.*'");

        JexlNode expanded = JexlNodeFactory.buildEQNode("F", "ab");
        BaseIndexExpansionVisitor.FutureJexlNode futureJexlNode = createFutureJexlNode(first, expanded);
        connectNode(futureJexlNode);

        assertNode(script, "F == 'ab'");
    }

    @Test
    public void testRegexExpandsIntoMultipleTerms() {
        String query = "F =~ 'a.*'";
        ASTJexlScript script = parse(query);

        JexlNode first = script.jjtGetChild(0);
        assertNode(first, "F =~ 'a.*'");

        JexlNode expanded = parse("F == 'ab' || F == 'ac'").jjtGetChild(0);
        assertInstanceOf(ASTOrNode.class, expanded);
        BaseIndexExpansionVisitor.FutureJexlNode futureJexlNode = createFutureJexlNode(first, expanded);
        connectNode(futureJexlNode);

        assertNode(script, "F == 'ab' || F == 'ac'");
    }

    @Test
    public void testIntersectionWithRegexExpandsIntoSingleTerm() {
        String query = "F == 'a' && F =~ 'a.*'";
        ASTJexlScript script = parse(query);

        JexlNode first = script.jjtGetChild(0);
        assertNode(first, "F == 'a' && F =~ 'a.*'");

        JexlNode second = first.jjtGetChild(1);
        assertNode(second, "F =~ 'a.*'");

        JexlNode expanded = JexlNodeFactory.buildEQNode("F", "ab");
        BaseIndexExpansionVisitor.FutureJexlNode futureJexlNode = createFutureJexlNode(second, expanded);
        connectNode(futureJexlNode);

        assertNode(script, "F == 'a' && F == 'ab'");
    }

    @Test
    public void testUnionWithRegexExpandsIntoSingleTerm() {
        String query = "F == 'a' || F =~ 'a.*'";
        ASTJexlScript script = parse(query);

        JexlNode first = script.jjtGetChild(0);
        assertNode(first, "F == 'a' || F =~ 'a.*'");

        JexlNode second = first.jjtGetChild(1);
        assertNode(second, "F =~ 'a.*'");

        JexlNode expanded = JexlNodeFactory.buildEQNode("F", "ab");
        BaseIndexExpansionVisitor.FutureJexlNode futureJexlNode = createFutureJexlNode(second, expanded);
        connectNode(futureJexlNode);

        assertNode(script, "F == 'a' || F == 'ab'");
    }

    @Test
    public void testIntersectionWithRegexExpandsIntoMultipleTerms() {
        String query = "F == 'a' && F =~ 'a.*'";
        ASTJexlScript script = parse(query);

        JexlNode first = script.jjtGetChild(0);
        assertNode(first, "F == 'a' && F =~ 'a.*'");

        JexlNode second = first.jjtGetChild(1);
        assertNode(second, "F =~ 'a.*'");

        JexlNode expanded = parse("F == 'ab' || F == 'ac'").jjtGetChild(0);
        assertInstanceOf(ASTOrNode.class, expanded);
        BaseIndexExpansionVisitor.FutureJexlNode futureJexlNode = createFutureJexlNode(second, expanded);
        connectNode(futureJexlNode);

        assertNode(script, "F == 'a' && (F == 'ab' || F == 'ac')");
    }

    @Test
    public void testUnionWithRegexExpandsIntoMultipleTerms() {
        String query = "F == 'a' || F =~ 'a.*'";
        ASTJexlScript script = parse(query);

        JexlNode first = script.jjtGetChild(0);
        assertNode(first, "F == 'a' || F =~ 'a.*'");

        JexlNode second = first.jjtGetChild(1);
        assertNode(second, "F =~ 'a.*'");

        JexlNode expanded = parse("F == 'ab' || F == 'ac'").jjtGetChild(0);
        assertInstanceOf(ASTOrNode.class, expanded);
        BaseIndexExpansionVisitor.FutureJexlNode futureJexlNode = createFutureJexlNode(second, expanded);
        connectNode(futureJexlNode);

        assertNode(script, "F == 'a' || F == 'ab' || F == 'ac'");
    }

    private ASTJexlScript parse(String query) {
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (ParseException e) {
            fail("Failed to parse query: " + query, e);
            throw new RuntimeException(e);
        }
    }

    private BaseIndexExpansionVisitor.FutureJexlNode createFutureJexlNode(JexlNode orig, JexlNode expanded) {
        BaseIndexExpansionVisitor.FutureJexlNode futureNode = new BaseIndexExpansionVisitor.FutureJexlNode(orig, null, false, true);
        futureNode.jjtSetParent(orig.jjtGetParent());
        futureNode.setRebuiltNode(expanded);
        return futureNode;
    }

    private void assertNode(JexlNode node, String expected) {
        String query = JexlStringBuildingVisitor.buildQuery(node);
        assertEquals(expected, query);
    }

    private void connectNode(BaseIndexExpansionVisitor.FutureJexlNode futureNode) {
        JexlNode node = futureNode.getOrigNode();
        JexlNodes.replaceChild(node.jjtGetParent(), node, futureNode.getRebuiltNode());
    }
}
