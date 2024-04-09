package datawave.query.jexl.nodes;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.ParserTreeConstants;

import datawave.core.query.jexl.nodes.QueryPropertyMarker;

/**
 * Provides default node cost calculations based on the Jexl node id
 */
public class DefaultNodeCostComparator extends NodeCostComparator {

    /**
     *
     * @param node
     *            an arbitrary JexlNode
     * @return the node cost
     */
    @Override
    protected int getCostIndex(JexlNode node) {
        if (node.jjtGetNumChildren() == 1 && (node instanceof ASTReference || node instanceof ASTReferenceExpression)) {
            QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(node);
            if (instance.isAnyType()) {
                return Integer.MAX_VALUE - 4;
            }
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
            return getNodeScore(node);
        }
    }

    /**
     * Wrapper around {@link JexlNodes#id(JexlNode)} so that we can boost the score of negated terms
     *
     * @param node
     *            any JexlNode
     * @return a score for the node
     */
    private int getNodeScore(JexlNode node) {
        int id = JexlNodes.id(node);
        switch (id) {
            case ParserTreeConstants.JJTNENODE:
                return Integer.MAX_VALUE - 3;
            case ParserTreeConstants.JJTNRNODE:
                return Integer.MAX_VALUE - 2;
            case ParserTreeConstants.JJTNOTNODE:
                return Integer.MAX_VALUE - 1;
            default:
                return id;
        }
    }
}
