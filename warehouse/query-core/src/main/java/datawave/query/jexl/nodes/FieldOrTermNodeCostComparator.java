package datawave.query.jexl.nodes;

import java.util.Map;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.ParserTreeConstants;

import datawave.core.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.JexlASTHelper;

/**
 * Orders nodes based on field or term counts
 */
public class FieldOrTermNodeCostComparator extends NodeCostComparator {

    private final boolean isFieldCount;
    private static final long NODE_ID_MULTIPLIER = 5000;
    private final Map<String,Long> counts;

    public FieldOrTermNodeCostComparator(Map<String,Long> counts, boolean isFieldCount) {
        this.counts = counts;
        this.isFieldCount = isFieldCount;
    }

    @Override
    int getCostIndex(JexlNode node) {
        if (node.jjtGetNumChildren() == 1 && (node instanceof ASTReference || node instanceof ASTReferenceExpression)) {
            return getCostIndex(node.jjtGetChild(0));
        } else if (node instanceof ASTOrNode) {
            int sum = 0;
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                sum += getCostIndex(node.jjtGetChild(i));
            }
            return sum;
        } else if (node instanceof ASTAndNode) {
            int lowest = Integer.MAX_VALUE;
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                int cost = getCostIndex(node.jjtGetChild(i));
                if (cost < lowest)
                    lowest = cost;
            }
            return lowest;
        } else {
            return getCostForLeaf(node);
        }
    }

    /**
     * Get the cost for a leaf according to the count map.
     * <p>
     * The extra code to handle integer overflows is due to term counts in the global index being a Long but Java's {@link Comparable#compareTo(Object)} returns
     * an integer.
     *
     * @param node
     *            a JexlNode
     * @return an integer used to compare nodes
     */
    private int getCostForLeaf(JexlNode node) {
        String key = getNodeKey(node);
        long value = counts.getOrDefault(key, getNodeScore(node));
        if (value > Integer.MAX_VALUE) {
            value = Integer.MAX_VALUE;
        }
        return (int) value;
    }

    /**
     * Generate a key for the count map. It's either the field, or the whole node.
     *
     * @param node
     *            a JexlNode
     * @return a node key
     */
    private String getNodeKey(JexlNode node) {
        if (node instanceof ASTNotNode || node instanceof ASTNENode || node instanceof ASTNRNode || node instanceof ASTFunctionNode) {
            return "NO_KEY";
        } else if (isFieldCount) {
            return JexlASTHelper.getIdentifier(node);
        } else {
            return JexlStringBuildingVisitor.buildQueryWithoutParse(node);
        }
    }

    /**
     * Wrapper around {@link JexlNodes#id(JexlNode)} so that we can boost the score of negated terms
     *
     * @param node
     *            any JexlNode
     * @return a score for the node
     */
    private long getNodeScore(JexlNode node) {
        int id = JexlNodes.id(node);
        switch (id) {
            case ParserTreeConstants.JJTNENODE:
                return Integer.MAX_VALUE - 3L;
            case ParserTreeConstants.JJTNRNODE:
                return Integer.MAX_VALUE - 2L;
            case ParserTreeConstants.JJTNOTNODE:
                return Integer.MAX_VALUE - 1L;
            default:
                return id * NODE_ID_MULTIPLIER;
        }
    }
}
