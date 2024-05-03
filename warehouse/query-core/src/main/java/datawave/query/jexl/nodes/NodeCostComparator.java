package datawave.query.jexl.nodes;

import java.util.Comparator;

import org.apache.commons.jexl3.parser.JexlNode;

/**
 * Compare nodes based on arbitrary cost.
 * <p>
 * EQ &lt; ER &lt; Functions
 */
public abstract class NodeCostComparator implements Comparator<JexlNode> {

    @Override
    public int compare(JexlNode left, JexlNode right) {
        int leftCost = getCostIndex(left);
        int rightCost = getCostIndex(right);
        return Integer.compare(leftCost, rightCost);
    }

    // Evaluate OR nodes last, then And nodes, then nodes by node id

    /**
     * Calculates a cost for the provided node
     *
     * @param node
     *            an arbitrary JexlNode
     * @return the integer cost
     */
    abstract int getCostIndex(JexlNode node);
}
