package datawave.query.jexl.nodes;

import java.util.Comparator;

import org.apache.commons.jexl3.parser.JexlNode;

import datawave.core.query.jexl.visitors.JexlStringBuildingVisitor;

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

        int result = Integer.compare(leftCost, rightCost);
        if (result == 0) {
            // if comparing by field cost (same field) provide an opportunity to sort alphabetically
            result = JexlStringBuildingVisitor.buildQuery(left).compareTo(JexlStringBuildingVisitor.buildQuery(right));
        }

        return result;
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
