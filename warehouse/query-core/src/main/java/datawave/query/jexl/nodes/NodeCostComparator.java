package datawave.query.jexl.nodes;

import java.util.Comparator;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;

/**
 * Compare nodes based on arbitrary cost.
 *
 * EQ < ER < Functions
 */
public class NodeCostComparator implements Comparator<JexlNode> {

    @Override
    public int compare(JexlNode left, JexlNode right) {
        int leftCost = getCostIndex(left);
        int rightCost = getCostIndex(right);
        return Integer.compare(leftCost, rightCost);
    }

    // Evaluate OR nodes last, then And nodes, then nodes by node id
    private int getCostIndex(JexlNode node) {
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
            // TODO -- recurse NOT nodes?
            return JexlNodes.id(node);
        }
    }
}
