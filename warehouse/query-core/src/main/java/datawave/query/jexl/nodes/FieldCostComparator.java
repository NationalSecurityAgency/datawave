package datawave.query.jexl.nodes;

import java.util.Map;

import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.JexlNode;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.count.CountMap;

/**
 * Comparator that operates on field cardinality
 */
public class FieldCostComparator extends AbstractNodeCostComparator {

    /**
     * Constructor that accepts a {@link CountMap}
     *
     * @param counts
     *            the count map
     */
    public FieldCostComparator(CountMap counts) {
        this(counts.getCounts());
    }

    /**
     * Constructor that accepts a {@link Map} of counts
     *
     * @param counts
     *            the count map
     */
    public FieldCostComparator(Map<String,Long> counts) {
        super(counts);
    }

    /**
     * The {@link FieldCostComparator} uses a node's identifier to calculate cost
     *
     * @param node
     *            a JexlNode
     * @return the node key
     */
    @Override
    public String getNodeKey(JexlNode node) {
        if (node instanceof ASTNotNode || node instanceof ASTNENode || node instanceof ASTNRNode || node instanceof ASTFunctionNode) {
            // certain node types are always kicked out
            return null;
        }
        return JexlASTHelper.getIdentifier(node);
    }

}
