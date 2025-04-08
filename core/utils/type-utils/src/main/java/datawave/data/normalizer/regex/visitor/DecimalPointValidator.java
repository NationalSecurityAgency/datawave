package datawave.data.normalizer.regex.visitor;

import datawave.data.normalizer.regex.AlternationNode;
import datawave.data.normalizer.regex.ExpressionNode;
import datawave.data.normalizer.regex.Node;
import datawave.data.normalizer.regex.NodeListIterator;
import datawave.data.normalizer.regex.RegexUtils;

/**
 * Implementation of {@link BaseVisitor} that accepts a {@link Node} tree and verifies that each alternated expression does not contain more than one decimal
 * point.
 */
public class DecimalPointValidator extends BaseVisitor {
    
    public static void validate(Node node) {
        if (node != null) {
            DecimalPointValidator visitor = new DecimalPointValidator();
            node.accept(visitor, null);
        }
    }
    
    @Override
    public Object visitExpression(ExpressionNode node, Object data) {
        if (node.getFirstChild() instanceof AlternationNode) {
            return super.visitExpression(node, data);
        } else {
            checkForInvalidDecimalPoints(node);
        }
        return null;
    }
    
    /**
     * Check the given expressions for valid decimal point specifications.
     * 
     * @param node
     *            the node to validate
     */
    private void checkForInvalidDecimalPoints(Node node) {
        boolean decimalPointSeen = false;
        NodeListIterator iter = node.getChildrenIterator();
        // Iterate through each element.
        while (iter.hasNext()) {
            // Get the next element.
            Node next = iter.next();
            // If the current element is a decimal point, validate it.
            if (RegexUtils.isDecimalPoint(next)) {
                if (decimalPointSeen) {
                    throw new IllegalArgumentException("Regex may not contain expressions with than one decimal point.");
                } else {
                    decimalPointSeen = true;
                }
            }
            // Skip past any quantifiers or optionals if specified.
            iter.seekPastQuantifiers();
            iter.seekPastQuantifiers();
        }
    }
}
