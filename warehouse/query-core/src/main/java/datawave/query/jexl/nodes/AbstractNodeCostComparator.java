package datawave.query.jexl.nodes;

import java.util.Map;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.ParserTreeConstants;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.count.CountMap;

/**
 * Class that contains core logic for field and term comparators
 */
public abstract class AbstractNodeCostComparator extends JexlNodeComparator {
    private static final long NODE_ID_MULTIPLIER = 5000L;
    private static final int SEGMENT = Integer.MAX_VALUE / 48;

    private final DefaultJexlNodeComparator comparator = new DefaultJexlNodeComparator();

    private final Map<String,Long> counts;

    /**
     * Constructor that accepts a {@link CountMap}
     *
     * @param counts
     *            the count map
     */
    protected AbstractNodeCostComparator(CountMap counts) {
        this(counts.getCounts());
    }

    /**
     * Constructor that accepts a {@link Map} of counts
     *
     * @param counts
     *            the count map
     */
    protected AbstractNodeCostComparator(Map<String,Long> counts) {
        this.counts = counts;
    }

    @Override
    public int compare(JexlNode left, JexlNode right) {
        left = JexlASTHelper.dereference(left);
        right = JexlASTHelper.dereference(right);

        int leftCost = getCostIndex(left);
        int rightCost = getCostIndex(right);

        int result = Integer.compare(leftCost, rightCost);

        if (result == 0) {
            result = comparator.compare(left, right);
        }

        return result;
    }

    @Override
    public int getCostIndex(JexlNode node) {
        if ((node instanceof ASTReference || node instanceof ASTReferenceExpression) && node.jjtGetNumChildren() == 1) {
            return getCostIndex(node.jjtGetChild(0));
        } else if (node instanceof ASTOrNode) {
            return getCostForUnion(node);
        } else if (QueryPropertyMarker.findInstance(node).isAnyType()) {
            return Integer.MAX_VALUE;
        } else if (node instanceof ASTAndNode) {
            return getCostForIntersection(node);
        } else {
            String key = getNodeKey(node);
            long score = counts.getOrDefault(key, getDefaultScore(node));
            if (score > Integer.MAX_VALUE) {
                score = Integer.MAX_VALUE;
            }
            return (int) score;
        }
    }

    /**
     * This method is the only difference between calculating cost based on field or term
     *
     * @param node
     *            a JexlNode
     * @return the node key
     */
    abstract String getNodeKey(JexlNode node);

    private long getDefaultScore(JexlNode node) {
        int id = JexlNodes.id(node);
        switch (id) {
            case ParserTreeConstants.JJTFUNCTIONNODE:
                return SEGMENT - 4L;
            case ParserTreeConstants.JJTNENODE:
                return SEGMENT - 3L;
            case ParserTreeConstants.JJTNRNODE:
                return SEGMENT - 2L;
            case ParserTreeConstants.JJTNOTNODE:
                return SEGMENT - 1L;
            default:
                return id * NODE_ID_MULTIPLIER;
        }
    }
}
