package datawave.query.jexl.visitors;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;

import datawave.query.index.lookup.RangeStream;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.nodes.QueryPropertyMarker.Instance;
import datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType;
import datawave.query.util.count.CountMap;

/**
 * Finds the maximum cardinality of a query in the context of the field index.
 * <p>
 * Cardinality in this context is defined as the maximum possible number of documents that can be returned for a given shard.
 * <p>
 * The {@link RangeStream} can persist the counts per term from the global index in the form of a {@link CountMap}. This map is used to determine the overall
 * cardinality of a particular query in the context of a particular shard.
 */
public class CardinalityVisitor extends ShortCircuitBaseVisitor {

    private final CountMap counts;

    public CardinalityVisitor(CountMap counts) {
        this.counts = counts;
    }

    public static long cardinality(JexlNode node, CountMap counts) {
        if (counts == null || counts.isEmpty()) {
            return 0L;
        }

        CardinalityVisitor visitor = new CardinalityVisitor(counts);
        return (long) node.jjtAccept(visitor, null);
    }

    @Override
    public Object visit(ASTJexlScript node, Object data) {
        return node.jjtGetChild(0).jjtAccept(this, data);
    }

    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        return node.jjtGetChild(0).jjtAccept(this, data);
    }

    @Override
    public Object visit(ASTOrNode node, Object data) {
        long count = 0L;
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode child = node.jjtGetChild(i);
            long childCount = (long) child.jjtAccept(this, data);

            if (childCount == Long.MAX_VALUE) {
                count = Long.MAX_VALUE;
                break;
            }

            count += childCount;

            if (count < 0) {
                // check for long overflow
                count = Long.MAX_VALUE;
                break;
            }
        }
        return count;
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {

        Instance instance = QueryPropertyMarker.findInstance(node);
        if (instance.isAnyTypeOf(MarkerType.EXCEEDED_TERM, MarkerType.EXCEEDED_VALUE, MarkerType.EXCEEDED_OR, MarkerType.BOUNDED_RANGE)) {
            return Long.MAX_VALUE;
        } else if (instance.isAnyType()) {
            return 0L;
        }

        long count = 0L;
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode child = node.jjtGetChild(i);
            long childCount = (long) child.jjtAccept(this, data);

            if (count == 0L) {
                count = childCount;
            } else {
                if (childCount > 0L) {
                    count = Math.min(count, childCount);
                }
            }
        }
        return count;
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        String key = JexlStringBuildingVisitor.buildQuery(node);
        if (counts.containsKey(key)) {
            return counts.get(key);
        }
        return 0L;
    }

    // rest of the operators

    @Override
    public Object visit(ASTNENode node, Object data) {
        return Long.MAX_VALUE;
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        return Long.MAX_VALUE;
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        return Long.MAX_VALUE;
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        return Long.MAX_VALUE;
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        return Long.MAX_VALUE;
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        return Long.MAX_VALUE;
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        return Long.MAX_VALUE;
    }

    @Override
    public Object visit(ASTNotNode node, Object data) {
        return Long.MAX_VALUE;
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        // Functions do not contribute to cardinality
        return 0L;
    }

}
