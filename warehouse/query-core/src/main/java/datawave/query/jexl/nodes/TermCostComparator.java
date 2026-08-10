package datawave.query.jexl.nodes;

import java.util.Map;

import org.apache.commons.jexl3.parser.JexlNode;

import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.util.count.CountMap;

/**
 * Comparator that operates on term cardinality
 */
public class TermCostComparator extends AbstractNodeCostComparator {

    /**
     * Constructor that accepts a {@link CountMap}
     *
     * @param counts
     *            the count map
     */
    public TermCostComparator(CountMap counts) {
        this(counts.getCounts());
    }

    /**
     * Constructor that accepts a {@link Map} of counts
     *
     * @param counts
     *            the count map
     */
    public TermCostComparator(Map<String,Long> counts) {
        super(counts);
    }

    /**
     * The {@link TermCostComparator} uses the whole node string to calculate cost
     *
     * @param node
     *            a JexlNode
     * @return the node key
     */
    public String getNodeKey(JexlNode node) {
        return JexlStringBuildingVisitor.buildQuery(node);
    }

}
