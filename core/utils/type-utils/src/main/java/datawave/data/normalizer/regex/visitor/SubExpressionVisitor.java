package datawave.data.normalizer.regex.visitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import datawave.data.normalizer.regex.AlternationNode;
import datawave.data.normalizer.regex.EncodedNumberNode;
import datawave.data.normalizer.regex.EncodedPatternNode;
import datawave.data.normalizer.regex.ExpressionNode;
import datawave.data.normalizer.regex.GroupNode;
import datawave.data.normalizer.regex.Node;

/**
 * An implementation of {@link CopyVisitor} that allows delegation of operations to be performed on sub-expressions of a regex, specifically, each alternated
 * expression of a regex with alternations, or the entire expression if no alternations are present.
 */
public class SubExpressionVisitor extends CopyVisitor {
    
    private static final Set<Class<? extends Node>> VALID_TOP_LEVEL_TYPES = ImmutableSet.of(GroupNode.class, EncodedNumberNode.class, EncodedPatternNode.class);
    
    @Override
    public Object visitExpression(ExpressionNode node, Object data) {
        if (node.getFirstChild() instanceof AlternationNode) {
            return super.visitExpression(node, data);
        } else {
            return visitSubExpression(node);
        }
    }
    
    /**
     * By default, return a copy of the sub-expression. This method should be overridden by any subclasses that need to manipulate sub-expressions.
     * 
     * @param node
     *            the sub-expression
     * @return the visited sub-expression
     */
    protected Object visitSubExpression(Node node) {
        return copy(node);
    }
    
    /**
     * Visit each sub-expression of the alternation with this visitor.
     * 
     * @param node
     *            the alternation node
     * @param data
     *            the data
     * @return null if all visited children returned null, an {@link ExpressionNode} if a single visited child returned a non-null result, or an
     *         {@link AlternationNode} with all non-null results from visited children
     */
    @Override
    public Object visitAlternation(AlternationNode node, Object data) {
        List<Node> children = new ArrayList<>();
        // Visit each alternated child.
        for (Node child : node.getChildren()) {
            Node visited = (Node) child.accept(this, data);
            // Do not retain null children.
            if (visited != null) {
                // If the returned node is an alternation node, retain each child of the returned alternation node.
                if (visited instanceof AlternationNode) {
                    children.addAll(visited.getChildren());
                } else if (visited instanceof ExpressionNode) {
                    if (visited.getChildCount() == 1 && visited.getFirstChild() instanceof AlternationNode) {
                        // If the returned node is an expression with an alternation child, retain each child of the alternation node.
                        children.addAll(visited.getFirstChild().getChildren());
                    } else if (visited.getChildCount() == 1 && VALID_TOP_LEVEL_TYPES.contains(visited.getFirstChild().getClass())) {
                        // If the returned node is an expression with a single child that is a top-level node type, retain the first child.
                        children.add(visited.getFirstChild());
                    } else {
                        // Otherwise retain the entire expression.
                        children.add(visited);
                    }
                } else if (VALID_TOP_LEVEL_TYPES.contains(visited.getClass())) {
                    // If the returned node is a valid top-level class, retain it.
                    children.add(visited);
                } else {
                    throw new IllegalArgumentException("Visited alternation child must be alternation or expression, but was " + visited);
                }
            }
        }
        
        // If there are no children, return null.
        if (children.isEmpty()) {
            return null;
        } else if (children.size() == 1) {
            // If there is only one child, return the child.
            return children.get(0);
        } else {
            // Otherwise return a new alternation node.
            AlternationNode copy = new AlternationNode();
            copy.addChildren(children);
            return copy;
        }
    }
}
