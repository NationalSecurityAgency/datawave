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
import datawave.data.normalizer.regex.OneOrMoreNode;
import datawave.data.normalizer.regex.QuestionMarkNode;
import datawave.data.normalizer.regex.RepetitionNode;
import datawave.data.normalizer.regex.SingleCharNode;
import datawave.data.normalizer.regex.StartAnchorNode;
import datawave.data.normalizer.regex.ZeroOrMoreNode;

/**
 * A basic {@link Visitor} implementation that will pass itself to the children of any node that accepts it.
 */
public class BaseVisitor implements Visitor {
    
    @Override
    public Object visitExpression(ExpressionNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visitAlternation(AlternationNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visitGroup(GroupNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visitDigitChar(DigitCharClassNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visitCharClass(CharClassNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visitCharRange(CharRangeNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visitSingleChar(SingleCharNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visitEscapedSingleChar(EscapedSingleCharNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visitRepetition(RepetitionNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visitQuestionMark(QuestionMarkNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visitAnyChar(AnyCharNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visitZeroToMany(ZeroOrMoreNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visitOneToMany(OneOrMoreNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visitInteger(IntegerNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visitIntegerRange(IntegerRangeNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visitEmpty(EmptyNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visitStartAnchor(StartAnchorNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visitEndAnchor(EndAnchorNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visitEncodedNumber(EncodedNumberNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visitEncodedPattern(EncodedPatternNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
}
