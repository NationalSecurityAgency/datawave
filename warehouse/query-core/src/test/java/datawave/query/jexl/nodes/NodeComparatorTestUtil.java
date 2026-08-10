package datawave.query.jexl.nodes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.ParseException;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;

/**
 * Common test code for node comparator tests
 */
public class NodeComparatorTestUtil {

    /**
     * Assumes the provided queries are either a union or an intersection
     *
     * @param query
     *            the input query
     * @param expected
     *            the expected query
     */
    public void drive(String query, String expected, JexlNodeComparator comparator) {
        JexlNode[] queryChildren = parse(query);
        Arrays.sort(queryChildren, comparator);

        JexlNode[] expectedChildren = parse(expected);

        assertEquals(expectedChildren.length, queryChildren.length);
        for (int i = 0; i < expectedChildren.length; i++) {
            String expectedChild = JexlStringBuildingVisitor.buildQuery(expectedChildren[i]);
            String queryChild = JexlStringBuildingVisitor.buildQuery(queryChildren[i]);
            assertEquals(expectedChild, queryChild);
        }
    }

    private JexlNode[] parse(String query) {
        try {
            ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
            JexlNode node = script.jjtGetChild(0);
            assertTrue(node instanceof ASTAndNode || node instanceof ASTOrNode);
            return JexlNodes.getChildren(node);
        } catch (ParseException e) {
            fail("Failed test: " + query);
            throw new RuntimeException("Failed test: " + query);
        }
    }
}
