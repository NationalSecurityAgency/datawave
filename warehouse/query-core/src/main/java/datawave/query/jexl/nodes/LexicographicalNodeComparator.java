package datawave.query.jexl.nodes;

import org.apache.commons.jexl3.parser.JexlNode;

import datawave.query.jexl.visitors.JexlStringBuildingVisitor;

/**
 * Sorts nodes according to the node string.
 * <p>
 * Note: this comparator is intended to break ties between nodes of similar type or cost. Running this comparator in isolation will produce unexpected results.
 */
public class LexicographicalNodeComparator extends JexlNodeComparator {

    @Override
    public int compare(JexlNode left, JexlNode right) {
        String leftQuery = JexlStringBuildingVisitor.buildQuery(left);
        String rightQuery = JexlStringBuildingVisitor.buildQuery(right);
        return leftQuery.compareTo(rightQuery);
    }

    @Override
    public int getCostIndex(JexlNode node) {
        throw new IllegalStateException("Not implemented");
    }
}
