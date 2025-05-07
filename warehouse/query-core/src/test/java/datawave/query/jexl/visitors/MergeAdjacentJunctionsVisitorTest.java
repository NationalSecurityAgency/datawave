package datawave.query.jexl.visitors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.Test;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;

public class MergeAdjacentJunctionsVisitorTest {

    @Test
    public void testSimulatedPrune() {
        String intersection = "A == '1' && REPLACEMENT == '2'";
        String nest = "(B == '2' && C == '3' && D == '4')";

        ASTJexlScript andScript = parse(intersection);
        ASTJexlScript nestScript = parse(nest);
        JexlNode andRoot = andScript.jjtGetChild(0);
        JexlNode nestRoot = nestScript.jjtGetChild(0);

        assertInstanceOf(ASTAndNode.class, andRoot);
        assertInstanceOf(ASTReferenceExpression.class, nestRoot);
        assertInstanceOf(ASTAndNode.class, nestRoot.jjtGetChild(0));

        JexlNodes.swap(andRoot, andRoot.jjtGetChild(1), nestRoot);

        assertInstanceOf(ASTEQNode.class, andRoot.jjtGetChild(0));
        assertInstanceOf(ASTReferenceExpression.class, andRoot.jjtGetChild(1));

        String expectedInput = "A == '1' && (B == '2' && C == '3' && D == '4')";
        String expectedOutput = "A == '1' && B == '2' && C == '3' && D == '4'";

        String inputString = JexlStringBuildingVisitor.buildQuery(andScript);
        assertEquals(expectedInput, inputString);

        JexlNode result = MergeAdjacentJunctionsVisitor.merge(andScript);
        String resultString = JexlStringBuildingVisitor.buildQuery(result);
        assertEquals(expectedOutput, resultString);
    }

    @Test
    public void testNodeOrderPreservedAfterMerge() {
        String intersection = "A == '1' && REPLACEMENT == '2' && D == '4'";
        String nest = "(B == '2' && C == '3')";

        ASTJexlScript andScript = parse(intersection);
        ASTJexlScript nestScript = parse(nest);
        JexlNode andRoot = andScript.jjtGetChild(0);
        JexlNode nestRoot = nestScript.jjtGetChild(0);

        assertInstanceOf(ASTAndNode.class, andRoot);
        assertInstanceOf(ASTReferenceExpression.class, nestRoot);
        assertInstanceOf(ASTAndNode.class, nestRoot.jjtGetChild(0));

        JexlNodes.swap(andRoot, andRoot.jjtGetChild(1), nestRoot);

        assertInstanceOf(ASTEQNode.class, andRoot.jjtGetChild(0));
        assertInstanceOf(ASTReferenceExpression.class, andRoot.jjtGetChild(1));
        assertInstanceOf(ASTEQNode.class, andRoot.jjtGetChild(2));

        String expectedInput = "A == '1' && (B == '2' && C == '3') && D == '4'";
        String expectedOutput = "A == '1' && B == '2' && C == '3' && D == '4'";

        String inputString = JexlStringBuildingVisitor.buildQuery(andScript);
        assertEquals(expectedInput, inputString);

        JexlNode result = MergeAdjacentJunctionsVisitor.merge(andScript);
        String resultString = JexlStringBuildingVisitor.buildQuery(result);
        assertEquals(expectedOutput, resultString);
    }

    @Test
    public void testMergeIntersectionWithSingleChild() {
        String expected = "F == 'a'";
        JexlNode eq = JexlNodeFactory.buildEQNode("F", "a");
        JexlNode intersection = JexlNodeFactory.createAndNode(List.of(eq));
        ASTJexlScript script = JexlNodeFactory.createScript(intersection);

        // initial state
        assertInstanceOf(ASTJexlScript.class, script);
        assertInstanceOf(ASTAndNode.class, script.jjtGetChild(0));
        assertEquals(1, script.jjtGetChild(0).jjtGetNumChildren());
        assertInstanceOf(ASTEQNode.class, script.jjtGetChild(0).jjtGetChild(0));
        assertEquals(expected, JexlStringBuildingVisitor.buildQuery(script));

        JexlNode result = MergeAdjacentJunctionsVisitor.merge(script);

        // no state changes after visit
        assertInstanceOf(ASTJexlScript.class, script);
        assertInstanceOf(ASTEQNode.class, script.jjtGetChild(0));
        assertEquals(expected, JexlStringBuildingVisitor.buildQuery(result));
    }

    private ASTJexlScript parse(String query) {
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (ParseException e) {
            fail("Failed to parse query: " + query, e);
            throw new RuntimeException(e);
        }
    }

}
