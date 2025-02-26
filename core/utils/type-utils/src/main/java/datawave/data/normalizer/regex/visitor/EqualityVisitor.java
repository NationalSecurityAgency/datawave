package datawave.data.normalizer.regex.visitor;

import datawave.data.normalizer.regex.AlternationNode;
import datawave.data.normalizer.regex.AnyCharNode;
import datawave.data.normalizer.regex.CharClassNode;
import datawave.data.normalizer.regex.CharRangeNode;
import datawave.data.normalizer.regex.DigitCharClassNode;
import datawave.data.normalizer.regex.EmptyNode;
import datawave.data.normalizer.regex.EncodedNumberNode;
import datawave.data.normalizer.regex.EncodedPatternNode;
import datawave.data.normalizer.regex.EndAnchorNode;
import datawave.data.normalizer.regex.EscapedSingleCharNode;
import datawave.data.normalizer.regex.ExpressionNode;
import datawave.data.normalizer.regex.GroupNode;
import datawave.data.normalizer.regex.IntegerNode;
import datawave.data.normalizer.regex.IntegerRangeNode;
import datawave.data.normalizer.regex.Node;
import datawave.data.normalizer.regex.OneOrMoreNode;
import datawave.data.normalizer.regex.QuestionMarkNode;
import datawave.data.normalizer.regex.RepetitionNode;
import datawave.data.normalizer.regex.SingleCharNode;
import datawave.data.normalizer.regex.StartAnchorNode;
import datawave.data.normalizer.regex.ZeroOrMoreNode;

/**
 * A {@link Visitor} implementation that will compare two {@link Node} tree and determine if they are equal.
 */
public class EqualityVisitor implements Visitor {
    
    /**
     * Return whether the given {@link Node} trees are equal.
     * 
     * @param left
     *            the left tree to compare
     * @param right
     *            the right tree to compare
     * @return true if the trees are equal, or false otherwise.
     */
    public static boolean isEqual(Node left, Node right) {
        if (left != null && right != null) {
            EqualityVisitor visitor = new EqualityVisitor();
            return (boolean) left.accept(visitor, right);
        } else {
            return left == null && right == null;
        }
    }
    
    private boolean isEqual(Node left, Object data) {
        Node right = (Node) data;
        // Compare the nodes.
        if (!left.equals(right)) {
            return false;
        }
        // Compare the child counts.
        if (left.getChildCount() != right.getChildCount()) {
            return false;
        }
        // Compare the children.
        for (int index = 0; index < left.getChildCount(); index++) {
            Node leftChild = left.getChildAt(index);
            Node rightChild = right.getChildAt(index);
            boolean isEqual = (boolean) leftChild.accept(this, rightChild);
            if (!isEqual) {
                return false;
            }
        }
        return true;
    }
    
    @Override
    public Object visitExpression(ExpressionNode node, Object data) {
        return isEqual(node, data);
    }
    
    @Override
    public Object visitAlternation(AlternationNode node, Object data) {
        return isEqual(node, data);
    }
    
    @Override
    public Object visitGroup(GroupNode node, Object data) {
        return isEqual(node, data);
    }
    
    @Override
    public Object visitDigitChar(DigitCharClassNode node, Object data) {
        return isEqual(node, data);
    }
    
    @Override
    public Object visitCharClass(CharClassNode node, Object data) {
        return isEqual(node, data);
    }
    
    @Override
    public Object visitCharRange(CharRangeNode node, Object data) {
        return isEqual(node, data);
    }
    
    @Override
    public Object visitSingleChar(SingleCharNode node, Object data) {
        return isEqual(node, data);
    }
    
    @Override
    public Object visitEscapedSingleChar(EscapedSingleCharNode node, Object data) {
        return isEqual(node, data);
    }
    
    @Override
    public Object visitRepetition(RepetitionNode node, Object data) {
        return isEqual(node, data);
    }
    
    @Override
    public Object visitQuestionMark(QuestionMarkNode node, Object data) {
        return isEqual(node, data);
    }
    
    @Override
    public Object visitAnyChar(AnyCharNode node, Object data) {
        return isEqual(node, data);
    }
    
    @Override
    public Object visitZeroToMany(ZeroOrMoreNode node, Object data) {
        return isEqual(node, data);
    }
    
    @Override
    public Object visitOneToMany(OneOrMoreNode node, Object data) {
        return isEqual(node, data);
    }
    
    @Override
    public Object visitInteger(IntegerNode node, Object data) {
        return isEqual(node, data);
    }
    
    @Override
    public Object visitIntegerRange(IntegerRangeNode node, Object data) {
        return isEqual(node, data);
    }
    
    @Override
    public Object visitEmpty(EmptyNode node, Object data) {
        return isEqual(node, data);
    }
    
    @Override
    public Object visitStartAnchor(StartAnchorNode node, Object data) {
        return isEqual(node, data);
    }
    
    @Override
    public Object visitEndAnchor(EndAnchorNode node, Object data) {
        return isEqual(node, data);
    }
    
    @Override
    public Object visitEncodedNumber(EncodedNumberNode node, Object data) {
        return isEqual(node, data);
    }
    
    @Override
    public Object visitEncodedPattern(EncodedPatternNode node, Object data) {
        return isEqual(node, data);
    }
}
