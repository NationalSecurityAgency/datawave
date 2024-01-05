package datawave.experimental.intersect;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.jexl2.parser.ASTAdditiveNode;
import org.apache.commons.jexl2.parser.ASTAdditiveOperator;
import org.apache.commons.jexl2.parser.ASTAmbiguous;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTArrayAccess;
import org.apache.commons.jexl2.parser.ASTArrayLiteral;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl2.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl2.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl2.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl2.parser.ASTBlock;
import org.apache.commons.jexl2.parser.ASTConstructorNode;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTDivNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTEmptyFunction;
import org.apache.commons.jexl2.parser.ASTEvaluationOnly;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTFloatLiteral;
import org.apache.commons.jexl2.parser.ASTForeachStatement;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTIfStatement;
import org.apache.commons.jexl2.parser.ASTIntegerLiteral;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTMapEntry;
import org.apache.commons.jexl2.parser.ASTMapLiteral;
import org.apache.commons.jexl2.parser.ASTMethodNode;
import org.apache.commons.jexl2.parser.ASTModNode;
import org.apache.commons.jexl2.parser.ASTMulNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTNullLiteral;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.ASTReturnStatement;
import org.apache.commons.jexl2.parser.ASTSizeFunction;
import org.apache.commons.jexl2.parser.ASTSizeMethod;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.ASTTernaryNode;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl2.parser.ASTVar;
import org.apache.commons.jexl2.parser.ASTWhileStatement;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.commons.jexl2.parser.SimpleNode;

import com.google.common.collect.Sets;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.BoundedRange;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.BaseVisitor;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;

/**
 * Simple post-order traversal of a Jexl query tree
 * <p>
 * A node that is not found in the nodes to uids map was never looked up in the field index.
 * <p>
 * A visit method may return a set of uids, an empty set, or null. A null return value indicates that a node should be skipped for the purpose of intersecting
 */
public class UidIntersection extends BaseVisitor implements UidIntersectionStrategy {

    private Map<String,Set<String>> nodesToUids;

    @Override
    public SortedSet<String> intersect(ASTJexlScript script, Map<String,Set<String>> nodesToUids) {
        this.nodesToUids = nodesToUids;

        Set<String> uids = (Set<String>) script.jjtAccept(this, null);
        return uids == null ? Collections.emptySortedSet() : new TreeSet<>(uids);
    }

    /*
     * some worker methods
     */

    /**
     * Return uids associated with this node
     *
     * @param node
     *            a Jexl node
     * @return the set of uids associated with this node, or null if this node was never evaluated against the field index (reword)
     */
    private Set<String> getUids(JexlNode node) {
        String key = JexlStringBuildingVisitor.buildQueryWithoutParse(node);
        return nodesToUids.get(key);
    }

    /*
     * Special case
     */

    @Override
    public Object visit(ASTJexlScript node, Object data) {
        if (node.jjtGetNumChildren() != 1) {
            throw new IllegalStateException("ASTJexlScript should only have one child");
        }
        return node.jjtGetChild(0).jjtAccept(this, data);
    }

    @Override
    public Object visit(ASTOrNode node, Object data) {
        Set<String> includes = null;
        Set<String> excludes = null;
        for (JexlNode child : JexlNodes.children(node)) {
            Object o = child.jjtAccept(this, null);
            if (o != null) {
                if (includes == null && excludes == null) {
                    includes = new HashSet<>();
                    excludes = new HashSet<>();
                }

                Set<String> childUids = (Set<String>) o;
                if (isNodeNegated(child)) {
                    excludes.addAll(childUids);
                } else {
                    includes.addAll(childUids);
                }
            }
        }

        if (includes == null && excludes == null) {

            return null;

        } else if (includes == null && excludes != null) {

            return excludes;

        } else if (includes != null && excludes == null) {

            // there were no excludes
            return includes;

        } else if (includes != null && excludes != null) {
            if (excludes.size() > 0) {
                includes.removeAll(excludes);
            }
            return includes;
        } else {
            throw new IllegalStateException("Error visiting ASTOrNode");
        }
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {

        // special handling of bounded ranges
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(node);
        if (instance.isType(BoundedRange.class)) {
            return getUids(node);
        } else if (instance.isAnyTypeOf(ASTEvaluationOnly.class, ASTDelayedPredicate.class)) {
            return null;
        }

        Set<String> includes = null;
        Set<String> excludes = null;
        for (JexlNode child : JexlNodes.children(node)) {
            Object o = child.jjtAccept(this, null);
            if (o != null) {
                if (excludes == null) {
                    excludes = new HashSet<>();
                }

                Set<String> childUids = (Set<String>) o;

                if (isNodeNegated(child)) {
                    excludes.addAll(childUids);
                } else {
                    if (includes == null) {
                        includes = new HashSet<>(childUids);
                    } else {
                        includes = new HashSet<>(Sets.intersection(includes, childUids));
                    }
                }
            }
        }

        if (includes == null || excludes == null) {
            return null;
        } else {
            try {
                includes.removeAll(excludes);
            } catch (UnsupportedOperationException e) {
                int i = 0;
            }
            return includes;
        }
    }

    /**
     * TODO -- need to handle case when child is wrapped ((!(foo == 'bar')))
     *
     * @param node
     * @return
     */
    private boolean isNodeNegated(JexlNode node) {
        JexlNode deref = JexlASTHelper.dereference(node);
        return (deref instanceof ASTNotNode || deref instanceof ASTNENode || deref instanceof ASTNRNode);
    }

    /*
     * might need to handle special nodes here
     */

    @Override
    public Object visit(ASTReference node, Object data) {
        if (node.jjtGetNumChildren() != 1) {
            throw new IllegalStateException("ASTReference node should have only one child");
        }

        return node.jjtGetChild(0).jjtAccept(this, data);
    }

    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        if (node.jjtGetNumChildren() != 1) {
            throw new IllegalStateException("ASTReferenceExpression node should have only one child");
        }
        return node.jjtGetChild(0).jjtAccept(this, data);
    }

    /**
     * Function nodes are not evaluated against the field index and will therefore always be skipped
     *
     * @param node
     *            a function node
     * @param data
     *            the data
     * @return null in every case
     */
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTNotNode node, Object data) {
        if (node.jjtGetNumChildren() != 1) {
            throw new IllegalStateException("ASTNotNode node should have only one child");
        }
        return node.jjtGetChild(0).jjtAccept(this, data);
    }

    /*
     * handle simple leaf nodes
     */

    @Override
    public Object visit(ASTEQNode node, Object data) {
        return getUids(node);
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        return getUids(node);
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        return getUids(node);
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        return getUids(node);
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        return getUids(node);
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        return getUids(node);
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        return getUids(node);
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        return getUids(node);
    }

    /*
     * should never hit these nodes
     */

    @Override
    public Object visit(SimpleNode node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTBlock node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTAmbiguous node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTIfStatement node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTWhileStatement node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTForeachStatement node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTAssignment node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTTernaryNode node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTBitwiseOrNode node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTBitwiseXorNode node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTBitwiseAndNode node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTAdditiveNode node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTAdditiveOperator node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTMulNode node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTDivNode node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTModNode node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTUnaryMinusNode node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTBitwiseComplNode node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTIdentifier node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTNullLiteral node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTTrueNode node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTFalseNode node, Object data) {
        return null;
    }

    public Object visit(ASTIntegerLiteral node, Object data) {
        return null;
    }

    public Object visit(ASTFloatLiteral node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTStringLiteral node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTArrayLiteral node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTMapLiteral node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTMapEntry node, Object data) {
        return null;
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
    public Object visit(ASTMethodNode node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTSizeMethod node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTConstructorNode node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTArrayAccess node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTReturnStatement node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTVar node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTNumberLiteral node, Object data) {
        return null;
    }
}
