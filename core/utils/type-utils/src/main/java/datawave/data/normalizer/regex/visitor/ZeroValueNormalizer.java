package datawave.data.normalizer.regex.visitor;

import java.util.function.Consumer;

import datawave.data.normalizer.regex.AlternationNode;
import datawave.data.normalizer.regex.ExpressionNode;
import datawave.data.normalizer.regex.Node;
import datawave.data.normalizer.regex.NodeListIterator;
import datawave.data.normalizer.regex.RegexConstants;
import datawave.data.normalizer.regex.RegexUtils;
import datawave.data.normalizer.regex.SingleCharNode;

/**
 * Implementation of {@link CopyVisitor} that:
 * <ol>
 * <li>Simplifies any positive non-simple number patterns that can only match zero to {@code "0"}.</li>
 * <li>Simplifies any negative non-simple number patterns that can only match zero to {@code "0"}.</li>
 * <li>Identifies any negative non-simple number patterns that can match zero, and adds a {@code "0"} alternation.</li>
 * </ol>
 */
public class ZeroValueNormalizer extends SubExpressionVisitor {
    
    public static Node expand(Node node) {
        if (node == null) {
            return null;
        }
        ZeroValueNormalizer normalizer = new ZeroValueNormalizer();
        return (Node) node.accept(normalizer, null);
    }
    
    @Override
    protected Object visitSubExpression(Node node) {
        // If the node represents a simple number, return a copy of it.
        if (RegexUtils.isSimpleNumber(node)) {
            return copy(node);
        }
        
        return normalizePattern(node, RegexUtils.isNegativeRegex(node));
    }
    
    private Node normalizePattern(Node node, boolean negative) {
        // If the pattern can only match zero, simplify it to just '0'.
        if (matchesZeroOnly(node, negative)) {
            return createZeroCharExpression();
        }
        // If the pattern can match zero, add an alternation for '0'.
        if (patternMatchesZero(node, negative)) {
            AlternationNode alternation = new AlternationNode();
            alternation.addChild(node);
            alternation.addChild(createZeroCharExpression());
            return new ExpressionNode(alternation);
        }
        // Otherwise the pattern can match numbers other than zero. Return a copy of it.
        return copy(node);
    }
    
    /**
     * Return whether the given pattern will only match 0.
     * 
     * @param node
     *            the node
     * @param negative
     *            whether the pattern is negative
     * @return true if the pattern will only match 0, or false otherwise
     */
    private boolean matchesZeroOnly(Node node, boolean negative) {
        // The minimum child count and index of the first non-minus sign node depends on whether the pattern is negative.
        int minChildCount = negative ? 2 : 1;
        int firstChild = negative ? 1 : 0;
        if (node.getChildCount() == minChildCount) {
            // If the minimum number of children is present, return whether it matches zero only.
            return RegexUtils.matchesZeroOnly(node.getChildAt(firstChild));
        } else {
            // If there are multiple children, return whether all children match zero only.
            NodeListIterator iter = node.getChildrenIterator();
            // Skip past the minus sign if present.
            if (negative) {
                iter.next();
            }
            // Seek past all elements that only match zero.
            seekPastAllZeroOnlyElements(iter);
            return !iter.hasNext();
        }
    }
    
    /**
     * Return true if the given negative pattern can match zero.
     * 
     * @param node
     *            the negative pattern
     * @return true if the pattern can match 0, or false otherwise
     */
    private boolean patternMatchesZero(Node node, boolean negative) {
        // If the child count is 2, there is only one node after the minus sign. Evaluate that by itself.
        int minChildCount = negative ? 2 : 1;
        int firstChild = negative ? 1 : 0;
        if (node.getChildCount() == minChildCount) {
            // If there is only one child, return whether it matches zero only.
            return RegexUtils.matchesZero(node.getChildAt(firstChild));
        } else {
            // If there are multiple children, return whether all children match zero only.
            NodeListIterator iter = node.getChildrenIterator();
            if (negative) {
                // Skip past the minus sign.
                iter.next();
            }
            // Seek past all elements that only match zero.
            seekPastAllZeroMatchingElements(iter);
            return !iter.hasNext();
        }
    }
    
    /**
     * Return a new {@link ExpressionNode} that contains the expression {@code "0"}.
     * 
     * @return the new node
     */
    private Node createZeroCharExpression() {
        return new ExpressionNode(new SingleCharNode(RegexConstants.ZERO));
    }
    
    /**
     * Seek past all consecutive elements that only match zero in the given iterator, including any after a decimal point.
     * 
     * @param iterator
     *            the iterator
     */
    private void seekPastAllZeroOnlyElements(NodeListIterator iterator) {
        seekPast(iterator, NodeListIterator::seekPastZeroOnlyElements);
    }
    
    /**
     * Seek past all consecutive elements that can match zero in the given iterator, including any after a decimal point.
     * 
     * @param iterator
     *            the iterator
     */
    private void seekPastAllZeroMatchingElements(NodeListIterator iterator) {
        seekPast(iterator, NodeListIterator::seekPastZeroMatchingElements);
    }
    
    /**
     * Seek past elements using the given delegate function. If a decimal point is present, seek past that as well.
     * 
     * @param iter
     *            the iterator
     * @param delegate
     *            the delegate function
     */
    private void seekPast(NodeListIterator iter, Consumer<NodeListIterator> delegate) {
        delegate.accept(iter);
        if (iter.hasNext() && RegexUtils.isDecimalPoint(iter.peekNext())) {
            iter.next();
            delegate.accept(iter);
        }
    }
}
