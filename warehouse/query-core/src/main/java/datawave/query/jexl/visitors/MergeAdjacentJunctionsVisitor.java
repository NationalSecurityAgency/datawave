package datawave.query.jexl.visitors;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.ParserTreeConstants;

import datawave.query.jexl.nodes.QueryPropertyMarker;

/**
 * A visitor that merges adjacent {@link ASTAndNode} and {@link ASTOrNode}
 * <p>
 * It is easier to run multiple visitors in sequence than it is to merge all functionality into a single visitor.
 */
public class MergeAdjacentJunctionsVisitor extends BaseVisitor {

    /**
     * Static entrypoint
     *
     * @param node
     *            the JexlNode
     * @return the original node, potentially modified
     */
    public static JexlNode merge(JexlNode node) {
        MergeAdjacentJunctionsVisitor visitor = new MergeAdjacentJunctionsVisitor();
        node.jjtAccept(visitor, null);
        return node;
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        node.childrenAccept(this, data);
        mergeSimilarChildren(node);
        return data;
    }

    @Override
    public Object visit(ASTOrNode node, Object data) {
        node.childrenAccept(this, data);
        mergeSimilarChildren(node);
        return data;
    }

    /**
     * Merges any children of the provided JexlNode that match the node id
     *
     * @param node
     *            the JexlNode
     */
    protected void mergeSimilarChildren(JexlNode node) {

        if (QueryPropertyMarker.findInstance(node).isAnyType()) {
            // do not operate on any marker type
            return;
        }

        boolean adjacent = false;
        int target = JexlNodes.id(node);

        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode child = node.jjtGetChild(i);
            if (JexlNodes.id(child) == target && !QueryPropertyMarker.findInstance(child).isAnyType()) {
                adjacent = true;
                break;
            }
        }

        if (adjacent) {
            List<JexlNode> children = new ArrayList<>();
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                JexlNode child = node.jjtGetChild(i);
                if (JexlNodes.id(child) == target && !QueryPropertyMarker.findInstance(child).isAnyType()) {
                    children.addAll(List.of(JexlNodes.getChildren(child)));
                } else {
                    children.add(child);
                }
            }
            JexlNodes.setChildren(node, children.toArray(new JexlNode[0]));
        } else if (node.jjtGetNumChildren() == 1 && node.jjtGetParent() != null) {
            // junction with single child should be eliminated
            JexlNodes.swap(node.jjtGetParent(), node, node.jjtGetChild(0));
        }
    }

    /**
     * This method allows the visitor to merge the children for similar adjacent junctions where the nested junction is wrapped in a reference expression, as in
     * the case of:
     * <p>
     * <code>A and (B and C)</code>
     * <p>
     * In this case the children of the nested intersection should be pulled up into the top level intersection for a final result of:
     * <p>
     * <code>A and B and C</code>
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the data
     */
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        super.visit(node, data);

        if (node.jjtGetParent() != null && node.jjtGetNumChildren() == 1) {

            JexlNode parent = node.jjtGetParent();
            JexlNode child = node.jjtGetChild(0);

            if (areParentAndChildSameJunctions(parent, child)) {
                List<JexlNode> children = new ArrayList<>();
                for (int i = 0; i < parent.jjtGetNumChildren(); i++) {
                    JexlNode currChild = parent.jjtGetChild(i);
                    // this logic ensures the nodes are inserted in-line so that the order of children is preserved
                    if (currChild == node) {
                        children.addAll(List.of(JexlNodes.getChildren(currChild)));
                    } else {
                        children.add(currChild);
                    }
                }
                JexlNodes.setChildren(parent, children.toArray(new JexlNode[0]));
            }
        }
        return data;
    }

    /**
     * Two nodes are considered the same junction if their node id is equivalent and the node id maps to {@link ParserTreeConstants#JJTANDNODE} or
     * {@link ParserTreeConstants#JJTORNODE}. Neither node may be a marker node.
     *
     * @param a
     *            the first node
     * @param b
     *            the second node
     * @return true if the nodes are equivalent junctions
     */
    protected boolean areParentAndChildSameJunctions(JexlNode a, JexlNode b) {
        int idA = JexlNodes.id(a);
        int idB = JexlNodes.id(b);

        // node id must match between A and B
        if (idA != idB) {
            return false;
        }

        // A and B must be an intersection or a union
        if (!(idA == ParserTreeConstants.JJTANDNODE || idA == ParserTreeConstants.JJTORNODE)) {
            return false;
        }

        // neither A nor B can be marker nodes
        return !(QueryPropertyMarker.findInstance(a).isAnyType() || QueryPropertyMarker.findInstance(b).isAnyType());
    }
}
