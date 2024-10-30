package datawave.query.jexl.nodes;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.JexlNode;

/**
 * Comparator that pushes single leaf nodes to the left and junctions to the right
 * <p>
 * Note: should only be used to break ties in other comparators.
 */
public class JunctionComparator extends JexlNodeComparator {

    @Override
    public int getCostIndex(JexlNode node) {
        if (node instanceof ASTAndNode && !QueryPropertyMarker.findInstance(node).isAnyType()) {
            return 3;
        } else if (node instanceof ASTOrNode) {
            return 2;
        } else {
            return 1;
        }
    }
}
