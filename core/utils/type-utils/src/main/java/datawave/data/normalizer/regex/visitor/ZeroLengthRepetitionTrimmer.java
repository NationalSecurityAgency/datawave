package datawave.data.normalizer.regex.visitor;

import datawave.data.normalizer.regex.ExpressionNode;
import datawave.data.normalizer.regex.IntegerNode;
import datawave.data.normalizer.regex.IntegerRangeNode;
import datawave.data.normalizer.regex.Node;
import datawave.data.normalizer.regex.NodeListIterator;
import datawave.data.normalizer.regex.QuestionMarkNode;
import datawave.data.normalizer.regex.RepetitionNode;

/**
 * Implementation of {@link CopyVisitor} that will return a copy of the tree trimmed of any characters that were immediately followed by a zero-length
 * repetition quantifier, i.e. {@code {0}} or {@code {0,0}}. See the following examples:
 * <ul>
 * <li>Input {@code "123.*"} will return {@code "123.*"}.</li>
 * <li>Input {@code "123{3}"} will return {@code "123{3}"}.</li>
 * <li>Input {@code "12[3-6]{0}"} will return {@code "12"}.</li>
 * <li>Input {@code "12[3-6]{0,0}"} will return {@code "12"}.</li>
 * <li>Input {@code "2{0,0}|3{0}"} will return null.</li>
 * </ul>
 */
public class ZeroLengthRepetitionTrimmer extends SubExpressionVisitor {
    
    /**
     * Return a copy of the given tree trimmed of all characters followed by a zero-length repetition quantifier. If the entire tree is trimmed, null will be
     * returned, otherwise an {@link ExpressionNode} with the trimmed tree will be returned.
     * 
     * @param node
     *            the node to trim
     * @return the trimmed node
     */
    public static Node trim(Node node) {
        if (node == null) {
            return null;
        }
        ZeroLengthRepetitionTrimmer visitor = new ZeroLengthRepetitionTrimmer();
        return (Node) node.accept(visitor, null);
    }
    
    @Override
    public Object visitExpression(ExpressionNode node, Object data) {
        Node visited = (Node) super.visitExpression(node, data);
        return visited != null && visited.isLeaf() ? null : visited;
    }
    
    @Override
    protected Object visitSubExpression(Node node) {
        Node copy = new ExpressionNode();
        NodeListIterator iter = node.getChildrenIterator();
        
        // Check each child for any zero-length repetitions.
        while (iter.hasNext()) {
            Node next = iter.next();
            if (iter.hasNext() && iter.isNextInstanceOf(RepetitionNode.class)) {
                Node repetition = iter.next();
                // If we have a zero-length repetition, do not copy it.
                if (isZeroLengthRepetition(repetition)) {
                    // If there is a ? after the repetition, move past it.
                    if (iter.hasNext() && iter.isNextInstanceOf(QuestionMarkNode.class)) {
                        iter.next();
                    }
                } else {
                    // Otherwise this is a non-zero length repetition. Copy it.
                    copy.addChild(copy(next));
                    copy.addChild(copy(repetition));
                }
            } else {
                // The child is not followed by a repetition. Copy it.
                copy.addChild(copy(next));
            }
        }
        
        // If we have any children after removing zero-length repetitions, return the copy. Otherwise, return null.
        if (copy.hasChildren()) {
            return copy;
        } else {
            return null;
        }
    }
    
    /**
     * Return whether the given repetition is {@code {0}} or {@code {0,0}}.
     * 
     * @param node
     *            the node
     * @return true if the given repetition is a zero-length repetition, or false otherwise
     */
    private boolean isZeroLengthRepetition(Node node) {
        Node child = node.getFirstChild();
        if (child instanceof IntegerNode) {
            return ((IntegerNode) child).getValue() == 0;
        } else {
            IntegerRangeNode rangeNode = (IntegerRangeNode) child;
            return rangeNode.getStart() == 0 && rangeNode.isEndBounded() && rangeNode.getEnd() == 0;
        }
    }
}
