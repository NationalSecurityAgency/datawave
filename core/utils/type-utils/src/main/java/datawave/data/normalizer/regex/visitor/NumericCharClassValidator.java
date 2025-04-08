package datawave.data.normalizer.regex.visitor;

import datawave.data.normalizer.regex.CharClassNode;
import datawave.data.normalizer.regex.CharRangeNode;
import datawave.data.normalizer.regex.EscapedSingleCharNode;
import datawave.data.normalizer.regex.Node;
import datawave.data.normalizer.regex.RegexConstants;
import datawave.data.normalizer.regex.SingleCharNode;

/**
 * Implementation of {@link BaseVisitor} that accepts a {@link Node} tree and verifies that any {@link CharClassNode} instances in it only have the following
 * children:
 * <ul>
 * <li>A {@link SingleCharNode} that has a digit.</li>
 * <li>A {@link CharRangeNode} that have a digit start and a digit end.</li>
 * </ul>
 */
public class NumericCharClassValidator extends BaseVisitor {
    
    private static final String ERROR_MESSAGE = "Character classes may only contain numeric characters and numeric ranges.";
    
    public static void validate(Node node) {
        if (node != null) {
            NumericCharClassValidator visitor = new NumericCharClassValidator();
            node.accept(visitor, null);
        }
    }
    
    @Override
    public Object visitCharClass(CharClassNode node, Object data) {
        for (Node child : node.getChildren()) {
            if (child instanceof EscapedSingleCharNode) {
                // Do not allow any escaped characters.
                throw new IllegalArgumentException(ERROR_MESSAGE);
            } else if (child instanceof SingleCharNode) {
                // Verify the character is a period or digit.
                validate((SingleCharNode) child);
            } else if (child instanceof CharRangeNode) {
                // Verify the range is numeric.
                validate((CharRangeNode) child);
            }
        }
        return null;
    }
    
    private void validate(SingleCharNode node) {
        if (!RegexConstants.ALL_DIGITS.contains(node.getCharacter())) {
            throw new IllegalArgumentException(ERROR_MESSAGE);
        }
    }
    
    private void validate(CharRangeNode node) {
        if (!RegexConstants.ALL_DIGITS.contains(node.getStart())) {
            throw new IllegalArgumentException(ERROR_MESSAGE);
        }
        if (!RegexConstants.ALL_DIGITS.contains(node.getEnd())) {
            throw new IllegalArgumentException(ERROR_MESSAGE);
        }
    }
    
}
