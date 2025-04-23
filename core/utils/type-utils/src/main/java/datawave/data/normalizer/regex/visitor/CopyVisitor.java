package datawave.data.normalizer.regex.visitor;

import java.util.Objects;

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
 * A {@link Visitor} implementation that returns a copy of a given {@link Node} tree.
 */
public class CopyVisitor implements Visitor {
    
    /**
     * Return a copy of the given node tree, or null if the node is null. Any null children will be filtered out.
     * 
     * @param node
     *            the tree to copy
     * @return the copy
     */
    public static Node copy(Node node) {
        if (node == null) {
            return null;
        }
        CopyVisitor visitor = new CopyVisitor();
        return (Node) node.accept(visitor, null);
    }
    
    /**
     * Return a copy of the given node.
     * 
     * @param node
     *            the node to copy
     * @param data
     *            the data
     * @return the copy
     */
    protected Node copy(Node node, Object data) {
        Node copy = node.shallowCopy();
        node.getChildren().stream().map((child) -> (Node) child.accept(this, data)).filter(Objects::nonNull).forEach(copy::addChild);
        return copy;
    }
    
    @Override
    public Object visitExpression(ExpressionNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visitAlternation(AlternationNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visitGroup(GroupNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visitDigitChar(DigitCharClassNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visitCharClass(CharClassNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visitCharRange(CharRangeNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visitSingleChar(SingleCharNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visitEscapedSingleChar(EscapedSingleCharNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visitRepetition(RepetitionNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visitQuestionMark(QuestionMarkNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visitAnyChar(AnyCharNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visitZeroToMany(ZeroOrMoreNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visitOneToMany(OneOrMoreNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visitInteger(IntegerNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visitIntegerRange(IntegerRangeNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visitEmpty(EmptyNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visitStartAnchor(StartAnchorNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visitEndAnchor(EndAnchorNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visitEncodedNumber(EncodedNumberNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visitEncodedPattern(EncodedPatternNode node, Object data) {
        return copy(node, data);
    }
}
