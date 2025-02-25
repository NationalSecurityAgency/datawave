package datawave.data.normalizer.regex.visitor;

import datawave.data.normalizer.regex.AlternationNode;
import datawave.data.normalizer.regex.EncodedNumberNode;
import datawave.data.normalizer.regex.ExpressionNode;
import datawave.data.normalizer.regex.Node;

/**
 * An implementation of {@link BaseVisitor} that will examine a node tree and return whether any non-encoded patterns remain in the tree. This is intended to be
 * used in conjunction with {@link SimpleNumberEncoder} to see if any further work remains to be done after encoding any and all simple numbers in the tree via
 * {@link SimpleNumberEncoder#encode(Node)}.
 * 
 * @see SimpleNumberEncoder
 */
public class NonEncodedNumbersChecker extends BaseVisitor {
    
    /**
     * Check if there are any non-encoded number patterns still present in the tree.
     * 
     * @param node
     *            the node to check
     * @return true if there are any non-encoded patterns, or false otherwise.
     */
    public static boolean check(Node node) {
        NonEncodedNumbersChecker visitor = new NonEncodedNumbersChecker();
        node.accept(visitor, null);
        return visitor.hasUnencodedPatterns;
    }
    
    private boolean hasUnencodedPatterns = false;
    
    @Override
    public Object visitExpression(ExpressionNode node, Object data) {
        // If we have not yet found any unencoded patterns, check the node.
        if (!this.hasUnencodedPatterns) {
            // If we have an alternation, examine the alternation.
            if (node.getFirstChild() instanceof AlternationNode) {
                return super.visitExpression(node, data);
            } else {
                // Otherwise, check if the node's first child is an encoded number.
                this.hasUnencodedPatterns = !(node.getFirstChild() instanceof EncodedNumberNode);
            }
        }
        return null;
    }
    
    @Override
    public Object visitAlternation(AlternationNode node, Object data) {
        // If we have not yet found any unencoded patterns, check each child.
        if (!this.hasUnencodedPatterns) {
            for (Node child : node.getChildren()) {
                child.accept(this, data);
                // If we found a child with an unencoded pattern, return early.
                if (this.hasUnencodedPatterns) {
                    break;
                }
            }
            
        }
        return null;
    }
    
    @Override
    public Object visitEncodedNumber(EncodedNumberNode node, Object data) {
        // No need to traverse down into the children.
        return null;
    }
}
