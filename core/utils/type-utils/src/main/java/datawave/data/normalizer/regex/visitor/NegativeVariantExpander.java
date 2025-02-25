package datawave.data.normalizer.regex.visitor;

import datawave.data.normalizer.regex.AlternationNode;
import datawave.data.normalizer.regex.AnyCharNode;
import datawave.data.normalizer.regex.ExpressionNode;
import datawave.data.normalizer.regex.Node;
import datawave.data.normalizer.regex.RegexConstants;
import datawave.data.normalizer.regex.SingleCharNode;

/**
 * Implementation of {@link CopyVisitor} that expands all regex expressions with negative variants of sub-expressions where applicable, particularly in the case
 * of a complete expression with a leading wildcard. See the following examples:
 * <ul>
 * <li>Input {@code ".453.*" will return ".4.*|-.4.*"}</li>
 * <li>Input {@code ".453.*" will return ".*4|-.*4"}</li>
 * <li>Input {@code ".453.*" will return ".*?4|-.*?4"}</li>
 * <li>Input {@code ".453.*" will return ".+4|-.+4"}</li>
 * <li>Input {@code ".453.*" will return ".+?4|-.+?4"}</li>
 * </ul>
 * Regexes with leading wildcards that have a negative sign in front of them will not require any expansion.
 */
public class NegativeVariantExpander extends SubExpressionVisitor {
    
    public static Node expand(Node node) {
        if (node == null) {
            return null;
        }
        NegativeVariantExpander visitor = new NegativeVariantExpander();
        return (Node) node.accept(visitor, null);
    }
    
    @Override
    protected Object visitSubExpression(Node node) {
        if (node.getFirstChild() instanceof AnyCharNode) {
            return expandLeadingWildcard(node);
        } else {
            return copy(node);
        }
    }
    
    /**
     * Return an expression that contains the original expression, as well as a negative variant of it.
     * 
     * @param node
     *            the expression to expand
     * @return the expanded expression
     */
    private Node expandLeadingWildcard(Node node) {
        // Create a copy of the original expression.
        Node negativeCopy = copy(node);
        
        // Insert a negative sign directly before the wildcard character.
        SingleCharNode negativeSign = new SingleCharNode(RegexConstants.HYPHEN);
        negativeCopy.addChild(negativeSign, 0);
        
        // Create an alternation node with a copy of the original expression and the negative copy as its children.
        AlternationNode alternation = new AlternationNode();
        alternation.addChild(copy(node));
        alternation.addChild(negativeCopy);
        
        // Return the alternation as the child of a new expression node.
        return new ExpressionNode(alternation);
    }
}
