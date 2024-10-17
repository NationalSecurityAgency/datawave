package datawave.query.jexl.nodes;

import java.util.Comparator;

import org.apache.commons.jexl3.parser.JexlNode;

import datawave.query.jexl.JexlASTHelper;

/**
 * Comparator for JexlNodes.
 * <p>
 * Implementing classes may prioritize different features for sorting. For example, sorting leaves before junctions, EQ nodes before ER nodes, or sorting
 * lexicographically by field and value
 * <p>
 * EQ &lt; ER &lt; Functions
 */
public abstract class JexlNodeComparator implements Comparator<JexlNode> {

    @Override
    public int compare(JexlNode left, JexlNode right) {
        int leftCost = getCostIndex(JexlASTHelper.dereference(left));
        int rightCost = getCostIndex(JexlASTHelper.dereference(right));

        return Integer.compare(leftCost, rightCost);
    }

    /**
     * Calculates a cost for the provided node
     *
     * @param node
     *            an arbitrary JexlNode
     * @return the integer cost
     */
    abstract int getCostIndex(JexlNode node);

    /**
     * Get the cost for a union by summing the cost of each child
     *
     * @param node
     *            the union
     * @return the cost
     */
    protected int getCostForUnion(JexlNode node) {
        int cost = 0;
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            cost += getCostIndex(node.jjtGetChild(i));
            // check for overflows
            if (cost == Integer.MAX_VALUE || cost < 0) {
                return Integer.MAX_VALUE;
            }
        }
        return cost;
    }

    /**
     * Get the cost for an intersection by taking the lowest cost of all children
     *
     * @param node
     *            the intersection
     * @return the cost
     */
    protected int getCostForIntersection(JexlNode node) {
        int cost = Integer.MAX_VALUE;
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            int childCost = getCostIndex(node.jjtGetChild(i));
            if (childCost < cost) {
                cost = childCost;
            }
        }
        return cost;
    }
}
