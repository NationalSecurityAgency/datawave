package datawave.query.jexl.nodes;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.ParserTreeConstants;

import datawave.query.jexl.JexlASTHelper;

/**
 * Comparator that enforces default ordering according to implied cost
 * <p>
 * Nodes are sorted by node type, then junction, then lexicographically
 */
public class DefaultJexlNodeComparator extends JexlNodeComparator {

    private static final int SEGMENT = Integer.MAX_VALUE / 48;

    private final JunctionComparator junctionComparator = new JunctionComparator();
    private final LexicographicalNodeComparator lexiComparator = new LexicographicalNodeComparator();

    @Override
    public int compare(JexlNode left, JexlNode right) {
        left = JexlASTHelper.dereference(left);
        right = JexlASTHelper.dereference(right);

        int result = Integer.compare(getCostIndex(left), getCostIndex(right));

        // EQ vs. (EQ AND EQ) will match
        if (result == 0) {
            result = junctionComparator.compare(left, right);
        }

        if (result == 0) {
            result = lexiComparator.compare(left, right);
        }

        return result;
    }

    /**
     *
     * @param node
     *            an arbitrary JexlNode
     * @return the node cost
     */
    @Override
    protected int getCostIndex(JexlNode node) {
        if ((node instanceof ASTReference || node instanceof ASTReferenceExpression) && node.jjtGetNumChildren() == 1) {
            return getCostIndex(node.jjtGetChild(0));
        } else if (node instanceof ASTOrNode) {
            return getCostForUnion(node);
        } else if (QueryPropertyMarker.findInstance(node).isAnyType()) {
            return Integer.MAX_VALUE;
        } else if (node instanceof ASTAndNode) {
            return getCostForIntersection(node);
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
            case ParserTreeConstants.JJTFUNCTIONNODE:
                return SEGMENT - 4;
            case ParserTreeConstants.JJTNENODE:
                return SEGMENT - 3;
            case ParserTreeConstants.JJTNRNODE:
                return SEGMENT - 2;
            case ParserTreeConstants.JJTNOTNODE:
                return SEGMENT - 1;
            default:
                return id;
        }
    }
}
