package datawave.data.normalizer.regex.visitor;

import datawave.data.normalizer.regex.AlternationNode;
import datawave.data.normalizer.regex.EmptyNode;
import datawave.data.normalizer.regex.ExpressionNode;
import datawave.data.normalizer.regex.GroupNode;
import datawave.data.normalizer.regex.Node;

/**
 * Implementation of {@link CopyVisitor} that will return a copy of the tree trimmed such that the following modifications are made to it:
 * <ul>
 * <li>Remove all {@link EmptyNode} instances.</li>
 * <li>Remove all {@link GroupNode} instances that subsequently have no children.</li>
 * <li>Remove all {@link AlternationNode} instances that subsequently have one or no children. In the case of one child, the child will replace the
 * {@link AlternationNode}.</li>
 * <li>Remove all {@link ExpressionNode} instances that subsequently have an {@link ExpressionNode} child.</li>
 * </ul>
 * See the following examples:
 * <ul>
 * <li>Input {@code "3||4||5"} will return {@code "3|4|5"}</li>
 * <li>Input {@code "3|()"} will return {@code "3"}</li>
 * <li>Input {@code "()|()"} will return {@code null}</li>
 * </ul>
 */
public class EmptyLeafTrimmer extends CopyVisitor {
    
    /**
     * Return a copy of the given tree trimmed of empty nodes. If the entire tree is trimmed, null will be returned, otherwise a {@link ExpressionNode} with the
     * trimmed tree will be returned.
     * 
     * @param node
     *            the node to trim
     * @return the trimmed node
     */
    public static Node trim(Node node) {
        if (node == null) {
            return null;
        }
        EmptyLeafTrimmer visitor = new EmptyLeafTrimmer();
        return (Node) node.accept(visitor, null);
    }
    
    @Override
    public Object visitExpression(ExpressionNode node, Object data) {
        Node copy = (Node) super.visitExpression(node, data);
        if (copy.isLeaf()) {
            return null;
        } else if (copy.getChildCount() == 1) {
            Node child = copy.getFirstChild();
            if (child instanceof ExpressionNode) {
                return child;
            }
        }
        return copy;
    }
    
    @Override
    public Object visitAlternation(AlternationNode node, Object data) {
        Node copy = (Node) super.visitAlternation(node, data);
        if (copy.isLeaf()) {
            return null;
        } else if (copy.getChildCount() == 1) {
            return copy.getFirstChild();
        } else {
            return copy;
        }
    }
    
    @Override
    public Object visitGroup(GroupNode node, Object data) {
        Node copy = (Node) super.visitGroup(node, data);
        return copy.isLeaf() ? null : copy;
    }
    
    @Override
    public Object visitEmpty(EmptyNode node, Object data) {
        return null;
    }
}
