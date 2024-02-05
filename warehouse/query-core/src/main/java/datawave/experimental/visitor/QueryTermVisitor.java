package datawave.experimental.visitor;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTArrayAccess;
import org.apache.commons.jexl3.parser.ASTArrayLiteral;
import org.apache.commons.jexl3.parser.ASTAssignment;
import org.apache.commons.jexl3.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl3.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl3.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl3.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl3.parser.ASTBlock;
import org.apache.commons.jexl3.parser.ASTConstructorNode;
import org.apache.commons.jexl3.parser.ASTDivNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTEmptyFunction;
import org.apache.commons.jexl3.parser.ASTFalseNode;
import org.apache.commons.jexl3.parser.ASTForeachStatement;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTIfStatement;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTMapEntry;
import org.apache.commons.jexl3.parser.ASTMapLiteral;
import org.apache.commons.jexl3.parser.ASTMethodNode;
import org.apache.commons.jexl3.parser.ASTModNode;
import org.apache.commons.jexl3.parser.ASTMulNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTNullLiteral;
import org.apache.commons.jexl3.parser.ASTNumberLiteral;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.ASTReturnStatement;
import org.apache.commons.jexl3.parser.ASTSizeFunction;
import org.apache.commons.jexl3.parser.ASTStringLiteral;
import org.apache.commons.jexl3.parser.ASTTernaryNode;
import org.apache.commons.jexl3.parser.ASTTrueNode;
import org.apache.commons.jexl3.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl3.parser.ASTVar;
import org.apache.commons.jexl3.parser.ASTWhileStatement;
import org.apache.commons.jexl3.parser.JexlNode;

import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType;
import datawave.query.jexl.visitors.BaseVisitor;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;

/**
 * Parse out all terms from a query tree
 */
public class QueryTermVisitor extends BaseVisitor {

    private final Set<JexlNode> leaves = new HashSet<>();

    public static Set<JexlNode> parse(ASTJexlScript script) {
        QueryTermVisitor visitor = new QueryTermVisitor();
        script.jjtAccept(visitor, null);
        return visitor.leaves;
    }

    /*
     * Special nodes (check for query property markers here)
     */

    @Override
    public Object visit(ASTOrNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(node);
        if (instance.isAnyType()) {
            // room to improve
            JexlNode source = instance.getSource();
            if (source == null) {
                throw new IllegalStateException("QueryPropertyMarker has a null source for node: " + JexlStringBuildingVisitor.buildQueryWithoutParse(node));
            }

            if (instance.isType(MarkerType.BOUNDED_RANGE)) {
                // add full node so QueryPropertyMarker can detect a bounded range
                leaves.add(node);
                return data;
            } else if (instance.isAnyTypeOf(MarkerType.EVALUATION_ONLY, MarkerType.EXCEEDED_VALUE, MarkerType.EXCEEDED_TERM)) {
                // skip these nodes for remote scheduling. there may be room in the future for an optimized regex scan
                return data;
            } else if (source instanceof ASTAndNode) {
                return visit((ASTAndNode) source, null);
            } else if (source instanceof ASTOrNode) {
                return visit((ASTOrNode) source, null);
            } else if (source instanceof ASTERNode) {
                return visit((ASTERNode) source, null);
            } else if (source instanceof ASTNRNode) {
                return visit((ASTNRNode) source, null);
            } else if (source instanceof ASTLTNode) {
                return visit((ASTLTNode) source, null);
            } else if (source instanceof ASTGTNode) {
                return visit((ASTGTNode) source, null);
            } else if (source instanceof ASTLENode) {
                return visit((ASTLENode) source, null);
            } else if (source instanceof ASTGENode) {
                return visit((ASTGENode) source, null);
            } else {
                throw new IllegalStateException("QueryTermVisitor does not support source nodes of type " + source.getClass().getSimpleName());
            }
        } else {
            node.childrenAccept(this, data);
        }
        return data;
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTReference node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    // we might try looking up negations at first, as a first cut
    @Override
    public Object visit(ASTNotNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    /*
     * Expected leaf nodes
     */

    @Override
    public Object visit(ASTEQNode node, Object data) {
        leaves.add(node);
        return data;
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        leaves.add(node);
        return data;
    }

    // lt,gt, le, ge nodes that are not part of a range

    @Override
    public Object visit(ASTLTNode node, Object data) {
        leaves.add(node);
        return data;
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        leaves.add(node);
        return data;
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        leaves.add(node);
        return data;
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        leaves.add(node);
        return data;
    }

    // regex nodes

    @Override
    public Object visit(ASTERNode node, Object data) {
        leaves.add(node);
        return data;
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        leaves.add(node);
        return data;
    }

    /*
     * Do not expect these to be leaf nodes
     */

    @Override
    public Object visit(ASTMulNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTDivNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTModNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTUnaryMinusNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTBitwiseComplNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTIdentifier node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTNullLiteral node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTTrueNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTFalseNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTStringLiteral node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTArrayLiteral node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTMapLiteral node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTMapEntry node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTEmptyFunction node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTSizeFunction node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTBlock node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTIfStatement node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTWhileStatement node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTForeachStatement node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTAssignment node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTTernaryNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTBitwiseOrNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTBitwiseXorNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTBitwiseAndNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTMethodNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTConstructorNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTArrayAccess node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTReturnStatement node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTVar node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTNumberLiteral node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

}
